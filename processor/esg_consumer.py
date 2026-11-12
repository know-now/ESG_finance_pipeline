import json
import time
import os
from kafka import KafkaConsumer
import psycopg2
from transformers import pipeline
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, MetaData
import google.generativeai as genai

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
#GEMINI SETUP
GOOGLE_API_KEY = os.environ.get("GEMINI_API_KEY")
if not GOOGLE_API_KEY:
    print("[-] WARNING: GEMINI_API_KEY not found. LLM filtering will be bypassed.")
else:
    genai.configure(api_key=GOOGLE_API_KEY)
gemini_model = genai.GenerativeModel('gemini-1.5-flash')

print("[*] Loading Models (this might take a few minutes)...")

#Basemodel (general English sentiment)
distilbert = pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")
#Finance model
finbert = pipeline("sentiment-analysis", model="ProsusAI/finbert")
print("[*] Both NLP Models loaded successfully")

def is_relevant_esg_news(title, text):
    """Filtering by Gemini. Returning True if the news is really ESG-news"""
    if not GOOGLE_API_KEY:
        return True #sending all news
    prompt = f"""
    Analyse the following financial news. Does this news specifically describe an Environmental, Social, or Governance (ESG) issue, risk, or milestone related to a specific company or sector?
    Answer ONLY 'YES' oe 'NO'.
    
    Title: {title}
    Text: {text[:800]}
    """
    try:
        response = gemini_model.generate_content(prompt)
        answer = response.text.strip().upper()
        return 'YES' in answer
    except Exception as e:
        print(f"[-] Gemini API Error: {e}")
        return True
def chunk_text(text, max_words=300):
    """Braking a long text into blocks 300 words (BERT limit)"""
    if not text:
        return [""]
    return [" ".join(words[i:i + max_words]) for i in range(0, len(words), max_words)]

def analyze_risk_with_chunks(text, model):
    """Running the text through the NLP-model in parts and finding maximum risk. """
    if not text or len(text.strip())== 0:
        return "NEUTRAL", 0.0
    chunks = chunk_text(text)
    max_risk_score = 0.0
    fimal_sentiment= "NEUTRAL"
    
    for chunk in chunks:
        try:
            res = model(chunk)[0]
            sent = res['label'].upper()
            
            

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