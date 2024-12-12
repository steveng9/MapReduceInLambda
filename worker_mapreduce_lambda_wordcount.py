import json
import time

import boto3
from botocore.exceptions import NoCredentialsError
from collections import Counter
from Inspector import Inspector
from utils import *
max_time = 13 * 60
# max_time = 2


def lambda_handler(event, context):
    start_time = time.time()

    worker_id = event['worker_id']
    inspector = Inspector()
    inspector.inspectAll()

    num_files_process = 0
    bucket = event['bucket']
    # data = event['data']
    # start_byte = event['start_byte']
    # end_byte = event['end_byte']
    files_to_process = event['files_to_process']
    worker_id = event['worker_id']

    s3 = boto3.client('s3')

    try:
        # get data logic
        counter = Counter()

        for file_name in files_to_process:
            if time.time() - start_time > max_time:
                raise TimeoutError("Operation timed out")

            response = s3.get_object(Bucket=bucket, Key=file_name)

            # process data logic
            lines = response['Body'].read().decode('utf-8').split('\n')
            for i, line in enumerate(lines):
                counter += Counter(line.split())
                # if i > 300: break

            num_files_process += 1

        inspector.inspectAllDeltas()
        return {
            'statusCode': 200,
            'body': f"worker {worker_id} completed successfully!",
            "worker_id": worker_id,
            'word_counts': counter,
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


# import json
# import boto3
# from botocore.exceptions import NoCredentialsError
# from collections import Counter
# from Inspector import Inspector
# import concurrent.futures
#
#
# def long_running_task(event):
#     bucket = event['bucket']
#     # data = event['data']
#     # start_byte = event['start_byte']
#     # end_byte = event['end_byte']
#     files_to_process = event['files_to_process']
#     worker_id = event['worker_id']
#
#     s3 = boto3.client('s3')
#
#     try:
#         # get data logic
#         counter = Counter()
#
#         for file_name in files_to_process:
#             response = s3.get_object(Bucket=bucket, Key=file_name)
#
#             # process data logic
#             lines = response['Body'].read().decode('utf-8').split('\n')
#             for i, line in enumerate(lines):
#                 counter += Counter(line.split())
#                 # if i > 300: break
#
#         return {
#             'statusCode': 200,
#             'body': f"worker {worker_id} completed successfully!",
#             "worker_id": worker_id,
#             'word_counts': counter,
#         }
#
#     except NoCredentialsError as e1:
#         print("Credentials not available")
#         return {
#             'statusCode': 400,
#             'body': f"worker {worker_id} failed! NoCredentialsError",
#             "worker_id": worker_id,
#             "error": f"WORKER: {worker_id} ERROR... {str(e1)}"
#         }
#     except Exception as e2:
#         return {
#             'statusCode': 400,
#             'body': f"worker {worker_id} failed!",
#             "worker_id": worker_id,
#             "error": f"WORKER: {worker_id} ERROR... {str(e2)}"
#         }
#
#
# def lambda_handler(event, context):
#     worker_id = event['worker_id']
#     inspector = Inspector()
#     inspector.inspectAll()
#
#     try:
#         with concurrent.futures.ThreadPoolExecutor() as executor:
#             future = executor.submit(long_running_task, event)
#             result = future.result(timeout=13 * 60)  # Timeout set to 13 minutes
#
#             inspector.inspectAllDeltas()
#             result["inspector"] = json.dumps(inspector.finish())
#             return result
#
#     except concurrent.futures.TimeoutError:
#
#         return {
#             "statusCode": 500,
#             "body": f"worker {worker_id} TIMED OUT!",
#             "error": f"The worker {worker_id} was terminated because it exceeded the timeout limit."
#         }
