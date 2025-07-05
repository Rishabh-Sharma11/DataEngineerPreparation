import boto3

def lambda_handler(event, context):

    print("From Where Trigger is coming - ",event)
    print("Starting SQS Batch Process")

    # Specify your SQS queue URL
    queue_url = 'https://sqs.us-east-1.amazonaws.com/851725469799/devSQS'

    sqs = boto3.client('sqs')

    # Receive messages from the SQS queue
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10,
        WaitTimeSeconds=2  
    )

    messages = response.get('Messages', [])
    print("List of messages - ",messages)

    print("Total messages received in the batch : ",len(messages))

    for message in messages:

        print("Processing message: ", message['Body'])
        # Delete message from the queue
        receipt_handle = message['ReceiptHandle']
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )
        print("Message deleted from the queue with receipt_handle - ",receipt_handle)
        
    print("Ending SQS Batch Process")

    return {
        'statusCode': 200,
        'body': f'{len(messages)} messages processed and deleted successfully'
    }