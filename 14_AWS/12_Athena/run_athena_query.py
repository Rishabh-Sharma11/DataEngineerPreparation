import boto3
import json

def lambda_handler(event, context):
    athena_client = boto3.client('athena', region_name='us-east-1')

    query = """
        SELECT 
            YEAR_ID,
            STATUS,
            SUM(SALES) AS total_sales,
            COUNT(DISTINCT ORDERNUMBER) AS total_orders,
            AVG(SALES) AS average_sales
        FROM sales_db.sales_data_raw
        GROUP BY YEAR_ID, STATUS
        ORDER BY YEAR_ID, total_sales DESC;
        """
    output_location = "s3://sales-data-analysis-gds-de/results/"

    # Start the Athena query
    response = athena_client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'sales_db'
        },
        ResultConfiguration={
            'OutputLocation': output_location
        }
    )

    query_execution_id = response['QueryExecutionId']
    print(f"Started Athena query with ID: {query_execution_id}")

    return {
        "statusCode": 200,
        "body": json.dumps({"query_execution_id": query_execution_id})
    }