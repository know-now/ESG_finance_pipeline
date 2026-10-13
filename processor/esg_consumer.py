import json
import time
import os
from kafka import KafkaConsumer
from transformers import pipeline
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData

print("=== ESG DATA CONSUMER Started ===")

#Connecting to a database PostgreSQL
DB_USER = os.environ.get("POSTGRES_USER", "postgres")
DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
DB_HOST = "postgres_db"
DB_NAME = os.environ.get("POSTGRES_DB", "esg_finance_db")

engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}")
metadata = MetaData()

#Creating a table if it does not exist
esg_news_table = Table(
    'esg_analysis_v2', metadata,
    Column('id', Integer, primary_key=True), 
    Column('title', String), 
    Column('source', String),
    Column('distilbert_sentiment', String),
    Column('finbert_risk', Float),
    Column('timestamp', String)
)
metadata.create_all(engine)
print("[*] PostgreSQL Table Ready.")
print("[*] Loading Models (this might take a few minutes)...")

# Downloading NLP model (Sentiment Analysis)
print("[*] Loading HuggingFace NLP model (this might take a minute)...")
nlp_analyzer = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
print("[*] Model loaded successfully.")

def start_processing():
    #waiting until Kafka and Postgress is ready
    time.sleep(20)
    
    consumer = KafkaConsumer(
        'esg_news_stream',
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print("[*] Connected to Kafka. Waiting for ESG news...")
    
    for message in consumer:
        data = message.value
        title = data['title']
        
        #Analysing the text
        nlp_result = nlp_analyzer(title)[0]
        sentiment = nlp_result['label']
        score = nlp_result['score']
        
        #Calculating Risk Score (the worse the segment - the higher the risk)
        risk_score = score if sentiment == 'NEGATIVE' else (1.0 - score)
        
        print(f"\n[AI ANALYZED] {title}")
        print(f"-> Sentiment: {sentiment} (Risk: {risk_score: {risk_score:.2f}})")
        
        with engine.connect() as conn:
            conn.execute(esg_news_table.insert().values(
                title=title,
                source=data['source'],
                sentiment=sentiment,
                risk_score=risk_score,
                timestamp=data['timestamp']
            ))
            conn.commit()
            print(" -> [DB] Saved to PostgreSQL!")

if __name__ == "__main__":
    start_processing()