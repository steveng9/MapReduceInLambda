import json
import time

import boto3
from botocore.exceptions import NoCredentialsError
from collections import Counter
from Inspector import Inspector
import pandas as pd
import numpy as np
from utils import *


def euclidean(point1, point2):
    return np.sqrt(np.sum((np.array(point1) - np.array(point2))**2))

def lambda_handler(event, context):
    inspector = Inspector()
    inspector.inspectAll()

    num_files_process = 0
    bucket = event['bucket']
    k = int(event['k'])
    files_to_process = event['files_to_process']
    worker_id = event['worker_id']
    my_centroids = pd.read_json(event['centroids'])

    s3 = boto3.client('s3')
    my_points = pd.DataFrame()

    try:
        for file_name in files_to_process:
            response = s3.get_object(Bucket=bucket, Key=file_name)
            my_points = pd.concat([my_points, pd.read_csv(response['Body'], header=None)])

        clusters = {i: [] for i in range(k)}
        cluster_sums = pd.DataFrame(np.zeros((k, my_points.shape[1])))
        for _, d_row in my_points.iterrows():
            point = d_row.tolist()
            costs = [euclidean(point, c) for c in my_centroids.values]
            closest_centroid_id = np.argmin(costs)
            clusters[closest_centroid_id].append(point)
        for key in range(k):
            cluster_sums.iloc[key] = np.sum(np.array(clusters[key]), axis=0).tolist()

        inspector.inspectAllDeltas()
        return {
            'statusCode': 200,
            'body': f"worker {worker_id} completed successfully!",
            "worker_id": worker_id,
            'cluster_sums': cluster_sums.to_json(orient='records'),
            "inspector_info": get_worker_inspection(inspector.finish())
        }



    except NoCredentialsError as e1:
        print("Credentials not available")
        return {
            'statusCode': 400,
            'body': f"worker {worker_id} failed! NoCredentialsError",
            "worker_id": worker_id,
            "error": f"WORKER: {worker_id} ERROR... {str(e1)}"
        }
    except TimeoutError:
        return {
            "statusCode": 500,
            "body": f"worker {worker_id} TIMED OUT! It finished {num_files_process} files.",
            "error": f"The worker {worker_id} was terminated because it exceeded the timeout limit."
        }
    except Exception as e2:
        return {
            'statusCode': 400,
            'body': f"worker {worker_id} failed!",
            "worker_id": worker_id,
            "error": f"WORKER: {worker_id} ERROR... {str(e2)}"
        }

