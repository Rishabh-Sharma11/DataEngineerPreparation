import json

def lambda_handler(event, context):

    print("Starting SQS Batch Process")
    print("Event Received - ",event)
    print("Messages received in current batch = ",len(event['Records']))

    for record in event['Records']:
        sales_order = json.loads(record['body'])
        print(sales_order)

    print("Ending SQS Batch Process")
    return {
        'statusCode': 200,
        'body': json.dumps('Processed sales orders from SQS!')
    }
