import streamlit as st
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os
from fpdf import FPDF
import plotly.express as px 
from datetime import datetime
import google.generativeai as genai

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
    # Main metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Total amount of analysed articles", len(df))
    col2.metric("Critical ESG risks (FinBERT)", len(df[df['finbert_risk'] > 0.8]))
    col3.metric("Critical ESG risks (DistilBERT)", len(df[df['distilbert_risk'] > 0.8]))
    
    #2. Top- most risky events
    st.subheader(" Top-3 Critical Risks (Action Required)")
    critical_risks = df[df['finbert_risk'] > 0.8].drop_duplicates(subset=['title']).head(3)
    if not critical_risks.empty:
        for idx, row in critical_risks.iterrows():
            st.warning(f"**Risk Score: {row['finbert_risk']}** | {row['title']} | *{row['source']}*")
    else: 
        st.success("No critical risks detected in the selected period.")
    
    # 3. Graphic
    st.subheader("Comparison of risks (FinBERT vs DistilBERT)")
    fig = px.line(df, x=df.index, y=['finbert_risk', 'distilbert_risk'],
                    labels={'value': 'Risk Score', 'index': 'Articles'},
                    title="Dynamics of risk evaluation")
    st.plotly_chart(fig, use_container_width=True)
    
    # 4.Company Search
    st.subheader("🔍 Search Company or Event")
    search_term = st.text_input("Enter company name (e.g., 'Tesla', 'Bank', 'Oil'):")
    if search_term:
        filtered_df = df[df['title'].str.lower().str.contains(search_term.lower())]
        if not filtered_df.empty:
            st.success(f"Found {len(filtered_df)} mentions for '{search_term}'")
            st.dataframe(filtered_df[['timestamp', 'title', 'finbert_sentiment', 'finbert_risk']])
        else:
            st.info(f"No data found for '{search_term}'.")
    
    # 5. Table of latest news.
    st.subheader("Last news")
    st.dataframe(df[['timestamp', 'title', 'finbert_sentiment', 'distilbert_sentiment', 'source']])

# 6. Export of reports (CSV + PDF)
st.subheader("📄 Export Reports")
col_btn1, col_btn2 = st.columns(2)

# CSV button
@st.cache_data
def convert_df(df):
    return df.to_csv(index=False).encode('utf-8')

csv = convert_df(df)
with col_btn1:
    st.download_button(
    label="📥 Report Download (CSV)",
    data=csv,
    file_name='esg_risk_report.csv',
    mime='text/csv',
)

@st.cache_data(ttl=3600)
def get_ai_executive_summary(risks_df):
    if risks_df.empty:
        return "No critical ESG risks detected in the current period. Market conditions remain stable."
    #Creating context from news of the model
    news_context = "\n".join([f"-{row['title']} (Score: {row['finbert_risk']:.2f})" for idx, row in risks_df.iterrows()])
    prompt = f"""Act as a Senior Financial Risk Analyst. Analyze the following high-risk ESG events:
    {news_context}
    Write a professional executive summary (3-4 sentences) for investors.
    Specify which companies/sectors face risks, what causes these risks (e.g., cyberattacks, market drops), and potential financial impacts.
    Write in plain text without any markdown formatting or bullet points.
    """
    try:
        api_key = os.environ.get("GEMINI_API_KEY")
        genai.configure(api_key=api_key)
        model = genai.GenerativeModel('gemini-3.1-flash-lite')
        response = model.generate_content(prompt)
        
        #Cleaning symbols for a correct PDF 
        return response.text.replace('\n', ' ').replace('*', '')
    except Exception as e:
        #If API key is absent
        st.error(f"Error Gemini API: {e}")
        titles = ", ".join(risks_df['title'].tolist())
        return f"Recent monitoring indicates elevated systemic risks associated with the following events: {titles}. These incidents pose significant operational and reputational threats to the invilved entities. Investors are advised to recalibrate exposure in these sectors to mitigate potential capital erosion."
    
def create_pdf_report(dataframe, top_risks, ai_summary_text):
    pdf = FPDF()
    pdf.add_page()
    
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M')}", ln=True, align='C')
    pdf.ln(10)
    
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(200, 10, txt="1. AI Executive Insights:", ln=True)
    pdf.set_font("Arial", size=11)
    pdf.multi_cell(0, 7, txt=ai_summary_text.encode('latin-1', 'replace').decode('latin-1'))
    pdf.ln(10)
    
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(200, 10, txt="2. Top Critical Risks (FinBERT > 0.8):", ln=True)
    
    pdf.set_font("Arial", size=11)
    
    if not top_risks.empty:
        for idx, row in top_risks.iterrows():
            #preventing of errors of encoding in PDF
            safe_title = str(row['title']).replace("’", "'").encode('latin-1', 'replace').decode('latin-1')
            pdf.multi_cell(0, 10, txt=f"[-] Score: {row['finbert_risk']} - {safe_title}")
    else:
        pdf.cell(200, 10, txt="No critical risks identified.", ln=True)
    pdf.ln(10)
    pdf.set_font("Arial", 'B', 14)
    pdf.cell(200, 10, txt="3. Summary Statistics:", ln=True)
    pdf.set_font("Arial", size=12)
    pdf.cell(200, 10, txt=f"Total Articles Analyzed: {len(dataframe)}", ln=True)
    pdf.cell(200, 10, txt=f"Total High-Risk Events: {len(dataframe[dataframe['finbert_risk'] > 0.8])}", ln=True)
    
    return pdf.output(dest='S').encode('latin-1')
with col_btn2:
    ai_summary = get_ai_executive_summary(critical_risks)
    pdf_bytes = create_pdf_report(df, critical_risks, ai_summary)
    st.download_button(
        label="📄 Executive Report Download (PDF)",
        data=pdf_bytes, 
        file_name=f"esg_risk_report_{datetime.now().strftime('%Y%m%d')}.pdf",
        mime="application/pdf"
    )