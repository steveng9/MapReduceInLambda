import json
import boto3
import os

ecs_client = boto3.client('ecs')

def lambda_handler(event, context):
    # security_groups = os.environ['SECURITY_GROUPS'].split(',')
    # assign_public_ip = os.environ['ASSIGN_PUBLIC_IP']

    response = ecs_client.run_task(
        cluster="MapReduce_in_container47",
        taskDefinition="MapReduceInContainerTaskDef2:2",
        launchType='FARGATE',
        networkConfiguration={
            'awsvpcConfiguration': {
                'subnets': ["subnet-0d8fe6f4bf6dfaecd"],
                'securityGroups': ["sg-0e2c9bbcc48bac5c9"],
                'assignPublicIp': "ENABLED"
            }
        }
    )

    return {
        'statusCode': 200,
        'body': str(response),
        'message': "Fargate Task Launched!"
    }
