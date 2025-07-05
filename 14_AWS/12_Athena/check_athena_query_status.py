import boto3
import json

def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='us-east-1')

    query_execution_id = event['query_execution_id']

    response = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
    status = response['QueryExecution']['Status']['State']
    print(f"Query {query_execution_id} status: {status}")

    if status == 'SUCCEEDED':
        output_location = response['QueryExecution']['ResultConfiguration']['OutputLocation']
        return {
            "statusCode": 200,
            "body": json.dumps({"query_execution_id": query_execution_id, "status": "SUCCEEDED", "s3_output": output_location})
        }
    elif status in ['FAILED', 'CANCELLED']:
        raise Exception(f"Query {query_execution_id} failed with status: {status}")
    else:
        raise Exception(f"Query {query_execution_id} is still in progress.")