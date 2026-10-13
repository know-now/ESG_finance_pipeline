import stream as st
import pandas as pd
from sqlalchemy import create_engine
import os
import ploty.express as px

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
if df.empty:
    st.warning("Waiting data from Kafka...")
else:
    