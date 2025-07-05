import boto3
import pandas as pd
from io import StringIO

def lambda_handler(event, context):
    
    print("Received Event Message - ",event)
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket, Key=key)
    file_content = response["Body"].read().decode('utf-8')
    
    data = pd.read_csv(StringIO(file_content))
    print(data)