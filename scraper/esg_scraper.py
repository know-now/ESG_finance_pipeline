import boto3
import time
import json
import feedparser 
import os
from datetime import datetime
from kafka import KafkaProducer
from botocore.exceptions import ClientError

print("=== ESG Financial News Scraper (Kafka Producer) Started ===")

MINIO_USER = os.environ.get("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.environ.get("MINIO_ROOT_PASSWORD", "password123")
print("[*] Connecting to MinIO Data Lake... ")
s3_client = boto3.client('s3', 
                        endpoint_url = 'http://minio:9000',
                        aws_access_key_id=MINIO_USER,
                        aws_secret_access_key=MINIO_PASSWORD)
try:
    s3_client.head_bucket(Bucket='raw-news')
    print("[*] MinIO bucket 'raw-news' exists.")
except ClientError as e:
    error_code = e.response['Error']['Code']
    if error_code == '404':
        print("[*] Creating MinIO bucket 'raw-news'...")
        s3_client.create_bucket(Bucket='raw-news')
    else:
        print(f"[-] Error checking bucket: {e}")
    
# Connecting to our Kafka-broker
def connect_to_kafka(retries=10, delay=5):
    for attempt in range(retries):
        try:
            print(f"[*] Attempting to connect to Kafka (Attempt {attempt + 1}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')                
            )
            print("[*] Successfully connected to Kafka!")
            return producer
        except Exception as e:
            print(f"[-] Connection failed: {e}. Retrying in {delay} seconds...")
            time.sleep(delay)
    print("[-] Could not connect to Kafka after multiple attempts. Exiting.")
    exit(1)    

producer = connect_to_kafka()
    
# Keywords needed for choosing correct info
ESG_KEYWORDS = ['carbon', 'emission', 'climate', 'green', 'sustainable', 'pollution', 'esg', 'penalty', 'lawsuit', 'market', 'bank', 'finance']

#RSS of financial and ecologic news

FINANCE_FEEDS = [
    "https://finance.yahoo.com/news/rssindex",
    "https://www.investing.com/rss/news_25.rss", # Stock market news
    "https://www.ft.com/companies?format=rss",   # Financial Times Companies
    "https://search.cnbc.com/rs/search/combinedcms/view.xml?id=10000115", # CNBC Business
    "https://feeds.a.dj.com/rss/RSSLifestyle.xml"
]

seen_articles = set()


def fetch_esg_news():
    print(f"\n [*] Fetching news at {time.strftime('%H:%M:%S')}")
    found_articles=0
    
    for url in FINANCE_FEEDS:
        try:
            feed = feedparser.parse(url)
            #checking forst 10 news out of each feed
            for entry in feed.entries[:10]:
                title = entry.title 
                summary = entry.get('summary', '')
                
                print(f"[DEBUG] Found article: {title}")
                
                if title in seen_articles:
                    continue
                
                full_text = (title + " " + summary).lower()
                
                # Filter just ESG news before saving
                if any(keyword in full_text for keyword in ESG_KEYWORDS):
                    article_data = {
                        "title": title,
                        "source": url,
                        "timestamp": time.strftime('%Y-%m-%d %H:%M:%S'),
                        "summary": summary
                    }
                    
                    file_name =f"news_{datetime.now().strftime('%Y%m%d_%H%M%S_')}.json"
                    try:
                        s3_client.put_object(Bucket='raw-news', Key=file_name, Body=json.dumps(article_data))
                        print(f"[MINIO SAVE] -> {file_name}")
                    except Exception as e:
                        print(f"[-] Failed to save to MinIO: {e}")
                #Sending to Kafka
                    producer.send('esg_news_stream', value=article_data)
                    producer.flush()
                    print(f"[KAFKA SEND] -> {title}")
                    found_articles += 1
                    
                    #Adding to seen articles
                seen_articles.add(title)
        except Exception as e:
            print(f"[-] Error parsing {url}: {e}")
    print(f"[*] Cycle complete. Sent {found_articles} ESG articles to Kafka.")
    print(f"[*] Total unique articles in memory: {len(seen_articles)}")
            
if __name__ ==   "__main__":
    while True: 
        fetch_esg_news()
        print("Waiting 120 seconds before next fetch...")
        time.sleep(120)
