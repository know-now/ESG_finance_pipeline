import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import plotly.express as px

st.set_page_config(page_title="ESG Guardian", layout="wide")
st.title("📊 ESG Guardian: Real-Time NLP Analysis")
st.markdown("Comparison of a general model (DistilBert) and special (FinBERT)")


@st.cache_data(ttl=10)
def load_data():
    DB_USER = os.environ.get("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
    DB_HOST = "postgres_db"
    DB_NAME = os.environ.get("POSTGRES_DB", "esg_finance_db")
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}")
    try:
        return pd.read_sql("SELECT * FROM esg_analysis_v2 ORDER BY id DESC", engine)
    except:
        return pd.DataFrame()
df = load_data()
if df.empty:
    st.warning("Waiting data from Kafka...")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total amount of analysed articles", len(df))
    col2.etric("Critical ESG risks (FinBERT)", len(df[df['finbert_risk'] > 0.8]))
    col3.metric("Critical ESG risks (DistilBERT)", len(df[df['distilbert_risk'] > 0.8]))
    
    st.subheader("Comparison of risks (FinBERT vs DistilBERT)")
    fig = px.line(df, x=df.index, y=['finbert_risk', 'distilbert_risk'],
                    labels={'value': 'Risk Score', 'index': 'Articles'},
                    title="Dynamics of risk evaluation")
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Last news")
    st.dataframe(df[['timestamp', 'title', 'finbert_sentiment', 'distilbert_sentiment', 'source']])