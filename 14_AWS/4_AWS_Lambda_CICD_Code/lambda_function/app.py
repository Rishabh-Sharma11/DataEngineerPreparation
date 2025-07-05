import os
import pandas as pd
import requests

def lambda_handler(event, context):

    print("Deployment via CICD process !!")
    response = requests.get("https://jsonplaceholder.typicode.com/posts")
    data = response.json()
    

    df = pd.DataFrame(data)
    print(df.head())


    print("Environment Variables:")
    defined_variables = ["ENV", "API_KEY", "LOG_LEVEL"]

    for key in defined_variables:
        value = os.environ.get(key)
        if value is not None:
            print(f"{key}: {value}")

    return {"statusCode": 200, 
            "body": "Lambda function executed successfully"
        }