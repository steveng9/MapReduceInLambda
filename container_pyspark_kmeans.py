import time
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
import os
from Inspector import Inspector
import json
import numpy as np
import re
from utils import *


SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:585008066606:MapReduceResult"

DATA_BUCKET = "project.bucket.final"
# DATA_BUCKET = "tcss562.project.output/test_data"
DATA_FILEPATH = f"s3a://{DATA_BUCKET}/points_*.csv"
CENTROIDS_FILEPATH = f"s3a://{DATA_BUCKET}/initial_15_kmeans_centroids.csv"

OUTPUT_BUCKET = "tcss562.project.output"
# OUTPUT_FILENAME = "fargate_results/kmeans/container_result_FG_km_"
OUTPUT_FILENAME = "ec2A_results/kmeans/container_result_km_192_"
# S3_FILEPATH = f"s3a://{DATA_BUCKET}/100.33439.txt"



def publish_to_sns(message):
    sns_client = boto3.client('sns', region_name="us-east-1")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="Container KMeans MapReduce result (from s3)"
    )
    return response

def send_results_to_S3(inspection, filename):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name='us-east-1'
    )
    s3_client.put_object(Body=json.dumps(inspection), Bucket=OUTPUT_BUCKET, Key=filename+str(int(time.time())))

def euclidean(point1, point2):
    return np.sqrt(np.sum((np.array(point1) - np.array(point2))**2))

def process_data_from_s3_via_pyspark():

    inspector = Inspector()
    inspector.inspectAll()

    try:
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

        # Get points file_names
        t1 = time.process_time()
        prefix = 'points_'
        s3_client = boto3.client(
            's3',
            aws_access_key_id=os.getenv("ACCESS_KEY"),
            aws_secret_access_key=os.getenv("SECRET_KEY"),
            region_name='us-east-1'
        )
        response = s3_client.list_objects_v2(Bucket=DATA_BUCKET, Prefix=prefix)
        files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]

        # Create RDD from filtered file list
        file_paths = ["s3a://{}/{}".format(DATA_BUCKET, file) for file in files]
        num_files = len(file_paths)

        data = spark.sparkContext.textFile(','.join(file_paths))

        # data = spark.sparkContext.textFile(DATA_FILEPATH)
        centroids = spark.sparkContext.textFile(CENTROIDS_FILEPATH)

        t2 = time.process_time()
        centroids = centroids.map(lambda line: np.array(list(map(float, re.split(r',', line.strip()))))).collect()
        data = data.map(lambda line: np.array(list(map(float, re.split(r',', line.strip())))))

        t3 = time.process_time()
        max_iter = 25
        metric = euclidean

        for _ in range(max_iter):
            cluster = data.map(lambda point: (np.argmin([metric(point, centroid) for centroid in centroids]), point))

            centroids = (cluster
                         .map(lambda x: (x[0], (x[1], 1)))
                         .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
                         .mapValues(lambda x: x[0] / x[1])
                         .map(lambda x: x[1])
                         .collect())

        t4 = time.process_time()
        timings = [round(t * 1000, 2) for t in [t1-t0, t2-t1, t3-t2, t4-t3]]

        inspector.inspectAllDeltas()
        # inspector.addAttribute("centroids", centroids)
        inspector.addAttribute("timings", timings)
        inspector.addAttribute("num_files", num_files)
        inspection = get_master_inspection(inspector.finish())

        send_results_to_S3(inspection, OUTPUT_FILENAME)

        print("Centroids: ", centroids)
        for timing in timings:
            print(":: %6.2f"%(timing), end="   ")
    except Exception as e:
        inspector.addAttribute("error", str(e))
        filename = OUTPUT_FILENAME + "err"
        send_results_to_S3(get_master_inspection(inspector.finish()), filename)





process_data_from_s3_via_pyspark()

