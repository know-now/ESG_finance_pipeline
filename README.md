# 🌿 ESG Guardian: Real-Time Financial Data & NLP Pipeline

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Apache Kafka](https://img.shields.io/badge/Apache_Kafka-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/PySpark-E25A1C?logo=apachespark&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![NLP](https://img.shields.io/badge/Machine_Learning-NLP-F7931E)

An event-driven Data Engineering and MLOps pipeline designed to ingest, process, and analyze real-time corporate financial news to detect ESG (Environmental, Social, and Governance) anomalies and potential "Greenwashing".

## 📌 Project Overview
In the European financial sector (specifically Luxembourg), ESG compliance is highly regulated. This project demonstrates an enterprise-grade Big Data architecture that evaluates corporate sustainability claims in real-time. 

It utilizes an event-driven streaming approach to capture live news, applies Natural Language Processing (NLP) to assess the sentiment and validity of environmental claims, and aggregates the results into a Data Mart for business intelligence.

### Key Features:
* **Event-Driven Streaming:** Real-time ingestion of corporate news and press releases using **Apache Kafka** as a distributed message broker.
* **Distributed Processing:** **PySpark** structured streaming is used to consume Kafka topics, clean the data, and apply transformations at scale.
* **NLP & MLOps:** Integration of a pre-trained ML model to perform Sentiment Analysis and detect "Greenwashing" indicators in the text.
* **Pipeline Orchestration:** **Apache Airflow** manages complex DAGs, data backfilling, and scheduled batch jobs.
* **Data Warehouse:** **PostgreSQL** serves as the analytical Data Mart, storing historical ESG scores and risk metrics.

---

## 🏗️ System Architecture

The infrastructure is fully containerized and consists of the following interconnected services:

1. **`news_scraper`**: Python-based producer that fetches financial RSS feeds/APIs and publishes raw JSON payloads to the `esg_news_stream` Kafka topic.
2. **`kafka_broker` & `zookeeper`**: The core message streaming cluster ensuring high-throughput, fault-tolerant data ingestion.
3. **`spark_processor`**: A PySpark consumer application that reads from Kafka, applies NLP transformations (scoring the ESG validity), and writes the structured output to the database.
4. **`airflow_webserver` & `airflow_scheduler`**: Orchestrates daily summary aggregations and monitors pipeline health via DAGs.
5. **`postgres_dwh`**: Stores the processed data in an OLAP schema designed for fast BI querying.

---

## 🚀 Getting Started

### Prerequisites
* Docker and Docker Compose (with at least 8GB RAM allocated for Kafka/Spark/Airflow).
* Python 3.10+
* `.env` file configuration (see below).

### 1. Configuration
Create a `.env` file in the root directory:
```env
POSTGRES_USER=your_username
POSTGRES_PASSWORD=secure_password_here
POSTGRES_DB=esg_finance_db
AIRFLOW_UID=50000