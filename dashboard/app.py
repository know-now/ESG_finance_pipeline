import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os
import plotly.express as px

st.set_page_config(page_title="ESG Guardian", layout="wide")
st.title("📊 ESG Guardian: Real-Time NLP Analysis")
st.markdown("Comparison of a general model (DistilBert) and special (FinBERT)")

st.sidebar.header('Filter')
days_to_show = st.sidebar.slider("Show history data (days)", 1, 30, 7)

@st.cache_data(ttl=10)
def load_data(days):
    DB_USER = os.environ.get("POSTGRES_USER", "postgres")
    DB_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
    DB_HOST = "postgres_db"
    DB_NAME = os.environ.get("POSTGRES_DB", "esg_finance_db")
    engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}")
    try:
        query = f"""
            SELECT * FROM esg_analysis_v3
            WHERE CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '{days} days'
            ORDER BY id DESC
            """
            
        return pd.read_sql(query, engine)
    except:
        return pd.DataFrame()
df = load_data(days_to_show)

if df.empty:
    st.warning("Waiting data from Kafka...")
else:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total amount of analysed articles", len(df))
    col2.metric("Critical ESG risks (FinBERT)", len(df[df['finbert_risk'] > 0.8]))
    col3.metric("Critical ESG risks (DistilBERT)", len(df[df['distilbert_risk'] > 0.8]))
    
    st.subheader("Comparison of risks (FinBERT vs DistilBERT)")
    fig = px.line(df, x=df.index, y=['finbert_risk', 'distilbert_risk'],
                    labels={'value': 'Risk Score', 'index': 'Articles'},
                    title="Dynamics of risk evaluation")
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Last news")
    st.dataframe(df[['timestamp', 'title', 'finbert_sentiment', 'distilbert_sentiment', 'source']])
    
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df(df)
#creating a download button 
st.download_button(
    label="📥 Report Download (CSV)",
    data=csv,
    file_name='esg_risk_report.csv',
    mime='text/csv',
)
