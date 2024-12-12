import time
import boto3
import os
from collections import Counter
from Inspector import Inspector
from utils import *






DATA_BUCKET = "project.bucket.text.raw"
DATA_FILEPATH = f"s3a://{DATA_BUCKET}/*.txt"

OUTPUT_BUCKET = "tcss562.project.output"
OUTPUT_FILENAME = "ec2A_results/wordcount/container_result_wc_1_"







def single_wordcount():
    print("sstarting program!")

    inspector = Inspector()
    inspector.inspectAll()

    t0 = time.process_time()
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name='us-east-1'
    )

    print("connected to s3")

    t1 = time.process_time()
    response = s3_client.list_objects_v2(Bucket=DATA_BUCKET)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.txt')]
    num_files = len(files)
    print("NUMber of FILES: ", num_files)

    t2 = time.process_time()
    counter = Counter()
    for i, file in enumerate(files):
        print("processing file number ", i)
        response = s3_client.get_object(Bucket=DATA_BUCKET, Key=file)

        # process data logic
        lines = response['Body'].read().decode('utf-8').split('\n')
        for j, line in enumerate(lines):
            counter += Counter(line.split())

    t3 = time.process_time()
    print("MOST COMMON:", counter.most_common(4))

    timings = [round(t * 1000, 2) for t in [t1 - t0, t2 - t1, t3 - t2]]
    inspector.inspectAllDeltas()
    inspection = inspector.finish()
    inspection["timings"] = timings
    inspection["num_files"] = num_files
    inspection["most_common"] = counter.most_common(4)
    s3_client.put_object(Body=get_master_inspection(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME + str(int(time.time())))
    print("DONE!!!")




single_wordcount()



