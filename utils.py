
import json


master_reporting_info = [
    "runtime",
    "cpuCores",
    "newcontainer",
    "functionMemory",
    "cpuIdle",
    "utilizedCPUs",
    "recommendedMemory",
    "totalMemory",
    "freeMemory",
    "worker_inspectors",
    "worker_errors",
    "workers_total_bytes",
    "most_common",
    "centroids",
    "timings",
    "num_files",
    "num_worker_lambdas",
    "availableCPUs",
    "utilizedCPUs",
    "Error!",
    "error",
    "num_iters",
]
def get_master_inspection(report):
    return json.dumps({key: report.get(key, "Na") for key in master_reporting_info})

worker_reporting_info = ["runtime", "newcontainer", "functionMemory", "cpuIdle", "utilizedCPUs", "recommendedMemory", "totalMemory", "freeMemory"]
def get_worker_inspection(report):
    return {key: report.get(key, "Na") for key in worker_reporting_info}
