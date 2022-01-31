import json
import logging
import boto3
import os
import requests
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # Get object name from event payload
    print(event)
    object_name = event['object_name']

    # Get all environment variables value
    bucket_name = os.environ["BUCKET_NAME"]
    expiration_limit = os.environ["EXPIRATION"]
    print("Bucket name is :"+bucket_name)
    print("Object expiration is :"+expiration_limit)
    
    # Generate Pre-signed URL to generate for S3 object
    s3_client = boto3.client('s3')
    try:
        response = s3_client.generate_presigned_post(bucket_name,object_name,ExpiresIn=int(expiration_limit))
        
        if response is None:
            return {
                'statusCode': 200,
                'body': json.dumps('Unable to get pre-signed URL.')
            }
        else:
            #Upload a file on S3 once the pre-signed URL has been generated
            print("Pre-signed URL generated successfully.")
            print(json.dumps(response))
            files = { 'file': open(object_name, 'rb')}
            r = requests.post(response['url'], data=response['fields'], files=files)
            print(r.status_code)
            return {
                'statusCode': 200,
                'body': json.dumps('Data file uploaded to S3 successfully.')
            }
    except ClientError as e:
        logging.error(e)
        return {
            'statusCode': 200,
            'body': json.dumps('Problem occured while generating the pre-signed URL.')
        }
