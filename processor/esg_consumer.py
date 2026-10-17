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
    'esg_analysis_v3', metadata,
    Column('id', Integer, primary_key=True), 
    Column('title', String), 
    Column('source', String),
    Column('distilbert_sentiment', String),
    Column('distilbert_risk', Float),
    Column('finbert_sentiment', String),
    Column('finbert_risk', Float),
    Column('timestamp', String)
)
metadata.create_all(engine)
print("[*] PostgreSQL Table Ready.")
print("[*] Loading Models (this might take a few minutes)...")

#basemodel (general english sentiment)
distilbert = pipeline("sentiment-analysis", model = "distilbert-base-uncased-finetuned-sst-2-english")
#Finance model
finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert")
print("[*] Both NLP Models loaded successfully")

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
        
        # Analydis of DistilBERT
        res_d = distilbert(title)[0]
        sent_d = res_d['label'].upper()
        risk_d = res_d['score'] if sent_d == 'NEGATIVE' else (1.0 - res_d['score'])
        
        #Analysis FinBERT (returns positive, negative, neutral)
        res_f = finbert(title)[0]
        sent_f = res_f['label'].upper()
        risk_f = res_f['score'] if sent_f == 'NEGATIVE' else (1.0 - res_f['score'])
        
        print(f"\n[AI] {title}")
        print(f" -> DistilBERT: {sent_d} ({risk_d:.2f}) | FinBERT: {sent_f} ({risk_f:.2f})")
        
        with engine.connect() as conn:
            conn.execute(esg_news_table.insert().values(
                title=title, 
                source=data['source'],
                distilbert_sentiment=sent_d,
                distilbert_risk=risk_d,
                finbert_sentiment=sent_f,
                finbert_risk=risk_f,
                timestamp=data['timestamp']
            ))
            conn.commit()
        
if __name__ == "__main__":
    start_processing()