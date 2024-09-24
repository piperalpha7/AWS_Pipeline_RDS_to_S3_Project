from __future__ import print_function
import boto3
import urllib

print('lambda function to call glue job')

glue = boto3.client('glue')

def lambda_handler(event, context):
    gluejobname="quality checks dynamic"

    runId = glue.start_job_run(JobName=gluejobname)
    status = glue.get_job_run(JobName=gluejobname, RunId=runId['JobRunId'])
    print("Job Status : ", status['JobRun']['JobRunState'])
