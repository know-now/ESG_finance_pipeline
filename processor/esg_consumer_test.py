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
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("[*] Successfully connected! Waiting for ESG news...")
        for message in consumer:
            news_data= message.value
            print(f"\n[NEW MESSAGE RECIEVED] FROM KAFKA!")
            print(f" -> Title: {news_data['title']}")
            print(f" -> Source: {news_data['source']}")
            print(f" -> Time: {news_data['timestamp']}")
            print("-" * 50)
    except Exception as e:
        print(f"[-] Consumer failed: {e}")
if __name__ == "__main__":
    start_consumer()
