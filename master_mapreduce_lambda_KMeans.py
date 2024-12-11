import json
import boto3
import botocore
from botocore.exceptions import NoCredentialsError
from concurrent.futures import ThreadPoolExecutor
import json
from Inspector import Inspector
import time
from collections import Counter
import pandas as pd
import numpy as np
from utils import *


SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:585008066606:MapReduceResult"
OUTPUT_BUCKET = "tcss562.project.output"
OUTPUT_FILENAME = "lambda_results/kmeans/lambda_km_"



k = 15
num_points_in_file = 10_000
max_time = 13.5 * 60




def lambda_handler(event, context):
    start_time = time.time()

    inspector = Inspector()
    inspector.inspectAll()

    data_bucket = event['bucket']
    num_workers = event['workers']
    max_iters = int(event["max_iters"])

    t0 = time.process_time()
    cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=14*60, connect_timeout=13.5*60, region_name="us-east-1")
    s3 = boto3.client('s3')

    try:

        t1 = time.process_time()

        # assign workload
        response = s3.list_objects_v2(Bucket=data_bucket)
        file_names = [obj['Key'] for obj in response['Contents']]
        file_names = [file_name for file_name in file_names if file_name.startswith("points_")]
        num_points = len(file_names) * num_points_in_file
        worker_load = {i: [] for i in range(num_workers)}
        num_files = len(file_names)
        for m, file_name in enumerate(file_names):
            worker_load[m % num_workers].append(file_name)

        centroid_data = s3.get_object(Bucket=data_bucket, Key="initial_15_kmeans_centroids.csv")
        centroids = pd.read_csv(centroid_data['Body'], header=None)

        num_iters = 0
        for _ in range(max_iters):
            if time.time() - start_time > max_time:
                break
            num_iters += 1
            t2 = time.process_time()
            with ThreadPoolExecutor(max_workers=num_workers) as executor:
                futs = []
                for i in range(num_workers):
                    payload = {
                        'bucket': data_bucket,
                        'files_to_process': worker_load[i],
                        'worker_id': i,
                        'centroids': centroids.to_json(),
                        "k": k
                    }
                    lambda_client = boto3.client('lambda',config=cfg)
                    futs.append(
                        executor.submit(lambda_client.invoke,
                                        FunctionName="worker_mapreduce_kmeans_function",
                                        InvocationType="RequestResponse",
                                        Payload=json.dumps(payload)
                                        )
                    )

                results = [fut.result() for fut in futs]
                payloads = [json.loads(r['Payload'].read()) for r in results]

            # aggregate workers' results
            t3 = time.process_time()
            full_cluster_sums = {c: np.zeros(centroids.shape[1]) for c in range(k)}
            for payload in payloads:
                worker_cluster_sums = pd.read_json(payload["cluster_sums"], orient='records')
                for c in range(k):
                    full_cluster_sums[c] = full_cluster_sums[c] + worker_cluster_sums.iloc[c].values
            for c in range(k):
                centroids.iloc[c] = (full_cluster_sums[c] / num_points).tolist()


        t4 = time.process_time()
        timings = [round(t * 1000, 2) for t in [t1 - t0, t2 - t1, t3 - t2, t4 - t3]]
        worker_inspectors = [payload["inspector"] for payload in payloads if "inspector" in payload]
        worker_errors = [payload["error"] for payload in payloads if "error" in payload]
        # worker_num_files = {worker_id: len(files) for worker_id, files in worker_load.items()}


        inspector.inspectAllDeltas()

        inspection = inspector.finish()
        inspection["worker_inspectors"] = worker_inspectors
        inspection["worker_errors"] = worker_errors
        # inspection["workers_num_files"] = worker_num_files
        # inspection["centroids"] = centroids.to_json(orient='records')
        inspection["timings"] = timings
        inspection["num_worker_lambdas"] = num_workers
        inspection["num_files"] = num_files
        inspection["num_iters"] = num_iters
        s3.put_object(Body=get_master_inspection(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME+f"{num_workers}_"+str(int(time.time())))
        return inspection

    except Exception as e:
        inspector.addAttribute("Error!", str(e))
        inspection = inspector.finish()
        s3.put_object(Body=json.dumps(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME+str(int(time.time())))
        return inspection
