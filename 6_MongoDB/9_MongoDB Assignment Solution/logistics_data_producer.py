#!/usr/bin/env python
# coding: utf-8

# In[70]:


#Import necessary libraries

import threading
from time import sleep
from uuid import uuid4

import mysql.connector
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

from kafka import KafkaProducer
from kafka.errors import KafkaError
from avro import schema, io
from datetime import datetime, timedelta
import time
import pickle
import json
import pandas as pd


# In[71]:


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


# In[72]:


def fetch_and_produce_data(producer, data):
    for index, row in data.iterrows():
        # Include all fields from the CSV file in the logistics_data dictionary
        logistics_data = {
            "GpsProvider": row["GpsProvider"],
            "BookingID": row["BookingID"],
            "Market/Regular ": row["Market/Regular "],
            "BookingID_Date": row["BookingID_Date"],
            "vehicle_no": row["vehicle_no"],
            "Origin_Location": row["Origin_Location"],
            "Destination_Location": row["Destination_Location"],
            "Org_lat_lon": row["Org_lat_lon"],
            "Des_lat_lon": row["Des_lat_lon"],
            "Data_Ping_time": row["Data_Ping_time"],
            "Planned_ETA": row["Planned_ETA"],
            "Current_Location": row["Current_Location"],
            "DestinationLocation": row["DestinationLocation"],
            "actual_eta": row["actual_eta"],
            "Curr_lat": row["Curr_lat"],
            "Curr_lon": row["Curr_lon"],
            "ontime": row["ontime"],
            "delay": row["delay"],
            "OriginLocation_Code": row["OriginLocation_Code"],
            "DestinationLocation_Code": row["DestinationLocation_Code"],
            "trip_start_date": row["trip_start_date"],
            "trip_end_date": row["trip_end_date"],
            "TRANSPORTATION_DISTANCE_IN_KM": row["TRANSPORTATION_DISTANCE_IN_KM"],
            "vehicleType": row["vehicleType"],
            "Minimum_kms_to_be_covered_in_a_day": row["Minimum_kms_to_be_covered_in_a_day"],
            "Driver_Name": row["Driver_Name"],
            "Driver_MobileNo": row["Driver_MobileNo"],
            "customerID": row["customerID"],
            "customerNameCode": row["customerNameCode"],
            "supplierID": row["supplierID"],
            "supplierNameCode": row["supplierNameCode"],
            "Material Shipped": row["Material Shipped"],
            # Add other fields as needed
        }

        # Produce to Kafka with GPSprovider as key
        producer.produce(
            topic='logistics_data',  # Replace with your Kafka topic
            key=str(row["GpsProvider"]),
            value=logistics_data,
            on_delivery=delivery_report
        )

        print("Produced message:", logistics_data)


# In[73]:


# Load CSV data into Pandas DataFrame
data = pd.read_csv('delivery_trip_truck_data.csv')  # Replace with your CSV file path


# In[74]:


data.head(10)


# In[75]:


object_columns = data.select_dtypes(include=['object']).columns
data[object_columns] = data[object_columns].fillna('unknown value')


# In[76]:


data.dtypes


# In[77]:





    # Define Kafka configuration
    kafka_config = {
        'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
        'sasl.mechanisms': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': '6XKBXWERKDEGFDUB',
        'sasl.password': 'Px0Bvj8IhlYWQNSChmL7e6o8BrG5IQrZvEQ0HWx9R0FSJDZi4wYotXoa0q6Na+aj'
    }

    # Create a Schema Registry client
    schema_registry_client = SchemaRegistryClient({
        'url': 'https://psrc-yorrp.us-east-2.aws.confluent.cloud',
        'basic.auth.user.info': '{}:{}'.format('JSFZ3A3FPHTHTJUO', 'NmDqPb+5CkKIc+KCiuaazHtDHwBOHZXENUH3V2wYuE12VlF58HfoJbkbiDc8GDXV')
    })

    # Fetch the latest Avro schema for the value
    subject_name = 'logistics_data-value'  # Adjust the subject name accordingly
    schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

    # Create Avro Serializer for the value
    key_serializer = StringSerializer('utf_8')
    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    # Define the SerializingProducer
    producer = SerializingProducer({
        'bootstrap.servers': kafka_config['bootstrap.servers'],
        'security.protocol': kafka_config['security.protocol'],
        'sasl.mechanisms': kafka_config['sasl.mechanisms'],
        'sasl.username': kafka_config['sasl.username'],
        'sasl.password': kafka_config['sasl.password'],
        'key.serializer': key_serializer,
        'value.serializer': avro_serializer
    })

    fetch_and_produce_data(producer, data)

    # Close the producer after processing all rows
    producer.flush()


# In[ ]:




