import json
import requests

def lambda_handler(event, context):
    
    print("Event Data -> ", event)
    response = requests.get("https://jsonplaceholder.typicode.com/posts/1")
    data = response.json()
    print("API Response : ",data)
    return data