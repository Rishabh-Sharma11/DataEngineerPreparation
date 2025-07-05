# pip install mysql-connector-python
# pip install boto3

import mysql.connector
import boto3
import json

def get_secret(secret_name, region_name="us-east-1"):
    """
    Retrieve database credentials from AWS Secrets Manager.
    """
    try:
        client = boto3.client("secretsmanager", region_name=region_name)

        response = client.get_secret_value(SecretId=secret_name)

        if "SecretString" in response:
            return json.loads(response["SecretString"])
        else:
            return json.loads(response["SecretBinary"].decode("utf-8"))
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def connect_and_create_db():
    connection = None
    try:

        secret_name = "rds_mysql_creds"
        region_name = "us-east-1"
        secrets = get_secret(secret_name, region_name)

        if not secrets:
            print("Failed to retrieve secrets.")
            return

        host = secrets["host"]
        port = secrets.get("port", 3306)
        user = secrets["dbuser"]
        password = secrets["dbpassword"]

        connection = mysql.connector.connect(
            host=host,
            port=port,
            user=user,
            password=password
        )

        if connection.is_connected():
            print("Successfully connected to the RDS instance.")
            
            cursor = connection.cursor()
            
            cursor.execute("CREATE DATABASE IF NOT EXISTS GDSdev;")
            print("Database created successfully.")
            
            cursor.execute("SHOW DATABASES;")
            print(cursor.fetchall())
            
        else:
            print("Failed to connect to the RDS instance.")
    except mysql.connector.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if connection is not None and connection.is_connected():
            cursor.close()
            connection.close()
            print("Connection closed.")

if __name__ == "__main__":
    connect_and_create_db()