import boto3
import csv
import os
from datetime import datetime


s3_client = boto3.client('s3')
tagging_client = boto3.client('resourcegroupstaggingapi')
S3_BUCKET_NAME = "deletion-candidate-reports"
S3_FOLDER_PATH = "reports"

def lambda_handler(event, context):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    csv_file_name = f"{S3_FOLDER_PATH}/deletion_candidate_report_{timestamp}.csv"
    
    resources = tagging_client.get_resources(
        TagFilters=[{"Key": "DeletionCandidate", "Values": ["yes"]}]
    )['ResourceTagMappingList']
    
    # Prepare CSV data
    csv_data = [["aws_service", "resource_name", "resource_id"]]
    
    for resource in resources:
        arn = resource['ResourceARN']
        service = arn.split(":")[2]
        resource_id = arn.split("/")[-1]
        resource_name = next((tag['Value'] for tag in resource.get('Tags', []) if tag['Key'] == 'Name'), resource_id)
        csv_data.append([service, resource_name, resource_id])

    csv_content = "\n".join([",".join(row) for row in csv_data])

    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=csv_file_name,
        Body=csv_content,
        ContentType="text/csv"
    )
    
    return {
        'statusCode': 200,
        'body': f"Report generated and uploaded to s3://{S3_BUCKET_NAME}/{csv_file_name}"
    }
