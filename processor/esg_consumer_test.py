import json
import time
from kafka import KafkaConsumer

print("=== ESG DATA CONSUMER (test) Started ===")

def start_consumer():
    time.sleep(15)
    
    print("[*] Attempting to connect to Kafka...")
    
    try:
        consumer = KafkaConsumer(
            'esg_news_stream', 
            bootstrap_servers=['kafka:9092'],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='esg_test_group',
            value_deserialiser=lambda x: json.loads(x.decode('utf-8'))
        )
        print("[*] Successfully connected! Waiting for ESG news...")
        
