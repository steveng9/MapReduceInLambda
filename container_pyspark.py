import time
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
import os
from Inspector import Inspector
import json
from collections import Counter


SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:585008066606:MapReduceResult"
DATA_BUCKET = "project.bucket.text.raw"
OUTPUT_BUCKET = "tcss562.project.output"
OUTPUT_FILENAME = "fargate_results/container_result_"
S3_FILEPATH = f"s3a://{DATA_BUCKET}/*.txt"
# S3_FILEPATH = f"s3a://{DATA_BUCKET}/100.33439.txt"



def publish_to_sns(message):
    sns_client = boto3.client('sns', region_name="us-east-1")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="Container MapReduce result (from s3)"
    )
    return response

def send_results_to_S3(inspection):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name='us-east-1'
    )
    s3_client.put_object(Body=json.dumps(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME+str(int(time.time())))



def process_data_from_s3_via_pyspark():

    inspector = Inspector() 
    inspector.inspectAll()

    t0 = time.process_time()
    conf = SparkConf() \
        .setAppName("S3Access") \
        .setMaster("local[*]") \
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
        .set("spark.hadoop.fs.s3a.access.key", os.getenv("ACCESS_KEY")) \
        .set("spark.hadoop.fs.s3a.secret.key", os.getenv("SECRET_KEY")) \
        .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .set("spark.executor.cores", "1") \
        .set("spark.executor.memory", "2g")

    # .set("spark.executor.instances", str(num_executors)) \

    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    t1 = time.process_time()
    rdd = spark.sparkContext.textFile(S3_FILEPATH)

    t2 = time.process_time()
    words = rdd.flatMap(lambda line: line.split(" "))
    key_values = words.map(lambda word: (word, 1))
    counts = key_values.reduceByKey(lambda a, b: a + b)

    t3 = time.process_time()
    sorted_counts = counts.sortBy(lambda x: -x[1])
    most_common = sorted_counts.take(10)

    t4 = time.process_time()
    timings = [round(t * 1000, 2) for t in [t1-t0, t2-t1, t3-t2, t4-t3]]

    inspector.inspectAllDeltas()
    inspection = inspector.finish()
    inspection["most_common"] = most_common
    inspection["timings"] = timings

    send_results_to_S3(inspection)

    print("Most Common: ", most_common)
    for timing in timings:
        print(":: %6.2f"%(timing), end="   ")



process_data_from_s3_via_pyspark()
# response = publish_to_sns(result)
# print("Message published to SNS:", response)

