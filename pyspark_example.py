import time
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
import boto3
import os

text = "Moby-Dick.txt"
# text = "a.txt"
SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:585008066606:MapReduceResult"
# BUCKET = "test.bucket.462562f24.skg"
BUCKET = "project.bucket.text.raw"
DATA = "Moby-Dick.txt"
S3_FILEPATH = f"s3a://{BUCKET}/*.txt"


num_executors = 4


def publish_to_sns(message):
    sns_client = boto3.client('sns', region_name="us-east-1")
    response = sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Message=message,
        Subject="Container MapReduce result (from s3)"
    )
    return response


def process_data_from_s3_via_pyspark():

    access_key = os.getenv("ACCESS_KEY")
    secret_key = os.getenv("SECRET_KEY")

    t0 = time.process_time() * 1000

    conf = SparkConf() \
        .setAppName("S3Access") \
        .setMaster("local[*]") \
        .set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0,com.amazonaws:aws-java-sdk-bundle:1.11.375') \
        .set("spark.hadoop.fs.s3a.access.key", access_key) \
        .set("spark.hadoop.fs.s3a.secret.key", secret_key) \
        .set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .set('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .set("spark.executor.instances", str(num_executors)) \
        .set("spark.executor.cores", "1") \
        .set("spark.executor.memory", "2g")

    sc = SparkContext(conf=conf)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    t1 = time.process_time() * 1000
    rdd = spark.sparkContext.textFile(S3_FILEPATH)

    t2 = time.process_time() * 1000
    words = rdd.flatMap(lambda line: line.split(" "))
    key_values = words.map(lambda word: (word, 1)) 

    t3 = time.process_time() * 1000
    counts = key_values.reduceByKey(lambda a, b: a + b)

    t4 = time.process_time() * 1000
    # Count=counts.collect()  # Count = the number of different words
    Count=counts.count()  # Count = the number of different words

    t5 = time.process_time() * 1000
    Sum=counts.map(lambda x:x[1]).reduce(lambda x,y:x+y) # 

    t6 = time.process_time() * 1000

    return Count,Sum,float(Sum)/Count,t1-t0,t2-t1,t3-t2,t4-t3,t5-t4,t6-t5



# count,sum_,ave_,t0,t1,t2,t3,t4,t5 = process_data_from_s3()
count,sum_,ave_,t0,t1,t2,t3,t4,t5 = process_data_from_s3_via_pyspark()
result = 'FROM S3 : Different words=%5.0f, total words=%6.0f, mean no. occurances per word=%4.2f \n t0=%6.2f, t1=%6.2f, t2=%6.2f, t3=%6.2f, t4=%6.2f, t5=%6.2f'%(count,sum_,ave_,t0,t1,t2,t3,t4,t5)
print(result)
response = publish_to_sns(result)
print("Message published to SNS:", response)




# def process_data_from_s3():

#     t0 = time.process_time() * 1000
#     s3 = boto3.client('s3', region_name="us-east-1")
#     response = s3.get_object(Bucket=BUCKET, Key=DATA)
#     # lines = response['Body'].read().decode('utf-8').split('\n')

#     t1 = time.process_time() * 1000
#     # Create a SparkConf object and set the master URL to local[*]
#     conf = SparkConf() \
#         .setAppName("WordCount") \
#         .setMaster(f"local[{num_executors}]") \
#         .set("spark.executor.instances", str(num_executors)) \
#         .set("spark.executor.cores", "1") \
#         .set("spark.executor.memory", "2g")

#     # Initialize SparkContext with this configuration
#     sc = SparkContext(conf=conf)
#     spark = SparkSession.builder.config(conf=conf).getOrCreate()


#     t2 = time.process_time() * 1000
#     # text_file = sc.textFile(text)
#     # text_file = sc.textFile(response['Body'].read().decode('utf-8'))
#     text_file = sc.parallelize([response['Body'].read().decode('utf-8')])


#     t3 = time.process_time() * 1000
#     words = text_file.flatMap(lambda line: line.split(" "))
#     not_empty = words.filter(lambda x: x!='') 
#     key_values = not_empty.map(lambda word: (word, 1)) 
#     counts = key_values.reduceByKey(lambda a, b: a + b)


#     t4 = time.process_time() * 1000
#     Count=counts.count()  # Count = the number of different words

#     t5 = time.process_time() * 1000
#     Sum=counts.map(lambda x:x[1]).reduce(lambda x,y:x+y) # 


#     t6 = time.process_time() * 1000

#     return Count,Sum,float(Sum)/Count,t1-t0,t2-t1,t3-t2,t4-t3,t5-t4,t6-t5
