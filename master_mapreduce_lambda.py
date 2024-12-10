import json
import boto3
import botocore
from botocore.exceptions import NoCredentialsError
from concurrent.futures import ThreadPoolExecutor
import json
from Inspector import Inspector
import time
from collections import Counter


SNS_TOPIC_ARN = "arn:aws:sns:us-east-1:585008066606:MapReduceResult"
OUTPUT_BUCKET = "tcss562.project.output"
OUTPUT_FILENAME = "lambda_results/master_result_"



def lambda_handler(event, context):

    inspector = Inspector()
    inspector.inspectAll()

    data_bucket = event['bucket']
    num_workers = event['workers']

    t0 = time.process_time()
    cfg = botocore.config.Config(retries={'max_attempts': 0}, read_timeout=14*60, connect_timeout=13.5*60, region_name="us-east-1")
    s3 = boto3.client('s3')
    sns_client = boto3.client('sns')


    # TODO verify that it launches many sandboxes concurrently.
    try:

        t1 = time.process_time()

        # assign workload
        response = s3.list_objects_v2(Bucket=data_bucket)
        file_names = [obj['Key'] for obj in response['Contents']]
        file_names = [file_name for file_name in file_names if file_name.split(".")[0].isdigit()]
        files_sorted_by_size = sorted(file_names, key=lambda x: int(x.split(".")[0]))
        worker_load = {i: [] for i in range(num_workers)}
        for k, file_name in enumerate(files_sorted_by_size):
            worker_load[k % num_workers].append(file_name)

        t2 = time.process_time()
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futs = []
            for i in range(num_workers):
                # TODO change to split at space inbetween words
                # start_byte = i * worker_load
                # end_byte = start_byte + worker_load - 1 if i < num_workers - 1 else file_size - 1
                payload = {
                    'bucket': data_bucket,
                    # 'data': data,
                    # 'start_byte': start_byte,
                    # 'end_byte': end_byte,
                    'files_to_process': worker_load[i],
                    # 'files_to_process': [worker_load[i][0]],
                    'worker_id': i
                }
                lambda_client = boto3.client('lambda',config=cfg)
                futs.append(
                    executor.submit(lambda_client.invoke,
                                    FunctionName="worker_mapreduce_function",
                                    InvocationType="RequestResponse",
                                    Payload=json.dumps(payload)
                                    )
                )

            results = [fut.result() for fut in futs]
            payloads = [json.loads(r['Payload'].read()) for r in results]

        # aggregate workers' results
        t3 = time.process_time()
        word_counts = Counter()
        for payload in payloads:
            if 'word_counts' in payload:
                word_counts.update(payload['word_counts'])

        # prepare result message
        t4 = time.process_time()
        # sns_response = sns_client.publish(
        #     TopicArn=SNS_TOPIC_ARN,
        #     Message=json.dumps({'num_workers': num_workers, 'word_counts': word_counts}),
        #     Subject="Lambda MapReduce result"
        # )
        timings = [round(t * 1000, 2) for t in [t1 - t0, t2 - t1, t3 - t2, t4 - t3]]
        worker_inspectors = [payload["inspector"] for payload in payloads if "inspector" in payload]
        worker_errors = [payload["error"] for payload in payloads if "error" in payload]
        worker_load_sizes = {worker_id: sum([int(filename.split(".")[0]) for filename in files]) for worker_id, files in worker_load.items()}
        worker_num_files = {worker_id: len(files) for worker_id, files in worker_load.items()}


        inspector.inspectAllDeltas()

        inspection = inspector.finish()
        inspection["worker_inspectors"] = worker_inspectors
        inspection["worker_errors"] = worker_errors
        inspection["workers_total_bytes"] = worker_load_sizes
        inspection["workers_num_files"] = worker_num_files
        # inspection["sns_response"] = sns_response
        inspection["most_common"] = word_counts.most_common(10)
        inspection["timings"] = timings
        inspection["num_worker_lambdas"] = num_workers
        s3.put_object(Body=json.dumps(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME+str(int(time.time())))
        return inspection

    except Exception as e:
        # sns_response = sns_client.publish(
        #     TopicArn=SNS_TOPIC_ARN,
        #     Message=json.dumps({'error in master lambda function!': str(e)})
        # )
        inspector.addAttribute("Error!", str(e))
        inspection = inspector.finish()
        s3.put_object(Body=json.dumps(inspection), Bucket=OUTPUT_BUCKET, Key=OUTPUT_FILENAME+str(int(time.time())))
        return inspection
