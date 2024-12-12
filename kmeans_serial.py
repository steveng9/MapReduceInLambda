import pandas as pd
import time
import boto3
import os
from Inspector import Inspector
import numpy as np
from utils import *






DATA_BUCKET = "project.bucket.final"
DATA_FILEPATH = f"s3a://{DATA_BUCKET}/points_*.csv"
CENTROIDS_FILEPATH = f"s3a://{DATA_BUCKET}/initial_15_kmeans_centroids.csv"

OUTPUT_BUCKET = "tcss562.project.output"
OUTPUT_FILENAME = "ec2A_results/kmeans/container_result_km_1_"




max_iters = 25
k = 15

def euclidean(point1, point2):
    return np.sqrt(np.sum((np.array(point1) - np.array(point2))**2))


def single_kmeans():
    print("sstarting program!")

    inspector = Inspector()
    inspector.inspectAll()

    t0 = time.process_time()

    prefix = "points_"
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("ACCESS_KEY"),
        aws_secret_access_key=os.getenv("SECRET_KEY"),
        region_name='us-east-1'
    )
    my_points = pd.DataFrame()
    print("retrieving file names")

    t1 = time.process_time()
    response = s3_client.list_objects_v2(Bucket=DATA_BUCKET, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    num_files = len(files)

    print("retrieving files", num_files)
    t2 = time.process_time()
    for x, file_name in enumerate(files):
        print("appending file", x)
        response = s3_client.get_object(Bucket=DATA_BUCKET, Key=file_name)
        my_points = pd.concat([my_points, pd.read_csv(response['Body'], header=None)])
    centroid_data = s3_client.get_object(Bucket=DATA_BUCKET, Key="initial_15_kmeans_centroids.csv")
    centroids = pd.read_csv(centroid_data['Body'], header=None)

    t3 = time.process_time()
    for x in range(max_iters):
        print("iter", x)

        clusters = {i: [] for i in range(k)}
        for _, d_row in my_points.iterrows():
            point = d_row.tolist()
            costs = [euclidean(point, c) for c in centroids.values]
            closest_centroid_id = np.argmin(costs)
            clusters[closest_centroid_id].append(point)
        for key in clusters.keys():
            if clusters[key]:
                centroids.iloc[key] = np.mean(np.array(clusters[key]), axis=0).tolist()
    print(centroids)

    t4 = time.process_time()
    timings = [round(t * 1000, 2) for t in [t1 - t0, t2 - t1, t3 - t2, t4 - t3]]
    inspector.inspectAllDeltas()

    inspection = inspector.finish()
    inspection["timings"] = timings
    inspection["num_files"] = num_files
    s3_client.put_object(Body=get_master_inspection(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME + str(int(time.time())))
    print("DONE!!!")

    return centroids



single_kmeans()



