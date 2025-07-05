from confluent_kafka import Producer
import json
import time

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()}")

kafka_config = {
    'bootstrap.servers': 'pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092',
    'sasl.mechanisms': 'PLAIN',
    'security.protocol': 'SASL_SSL',
    'sasl.username': 'U4ZHZEMXRQQEL62M',
    'sasl.password': '9fBSku8MpYbWKI46BEqBxbR4h1IWgauihDZnHmma5HxdLCG0CbIXMbt0njnqcIoC'
}

with open('user_transactions.json') as f:
    data = json.load(f)

p = Producer(**kafka_config)

for record in data:
    record_str = json.dumps(record)
    p.produce('trx_topic_data', record_str, callback=delivery_report)
    print("Message Published -> ",record_str)
    time.sleep(4)

p.flush()