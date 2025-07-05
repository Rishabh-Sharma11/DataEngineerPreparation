import boto3
import pandas as pd


s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
sns_arn = 'arn:aws:sns:us-east-1:851725469799:test_sns_v2'

def lambda_handler(event, context):
    # TODO implement
    print("Received Event Trigger - ",event)
    try:
        bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
        s3_file_key = event["Records"][0]["s3"]["object"]["key"]
        print("Bucket Name - ",bucket_name)
        print("File Path - ",s3_file_key)

        resp = s3_client.get_object(Bucket=bucket_name, Key=s3_file_key)
        print("Get object response body - ",resp['Body'])

        df_s3_data = pd.read_csv(resp['Body'], sep=",")
        print(df_s3_data.head())

        message = "s3://"+bucket_name+"/"+s3_file_key+" File has been processed succesfuly !!"
        respone = sns_client.publish(Subject="SUCCESS - Daily Data Processing",TargetArn=sns_arn, Message=message, MessageStructure='text')
    except Exception as err:
        print("ERROR - ",err)
        message = "Input S3 File processing failed !! With Error - " + str(err) + " -- Received Event : " + str(event)
        respone = sns_client.publish(Subject="FAILED - Daily Data Processing", TargetArn=sns_arn, Message=message, MessageStructure='text')