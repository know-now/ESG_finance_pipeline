from airflow import DAG 
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 24),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'esg_scraper_pipeline',
    default_args=default_args,
    description='Securely orchestrates the ESG ESG_finance_pipeline Scraper',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['esg', 'scraper','bronze_layer'],
) as dag:
    run_scraper=DockerOperator(
        task_id='run_esg_scraper',
        image='esg_finance_pipeline-scraper_app:latest',
        container_name='task_esg_scraper',
        api_version='auto',
        auto_remove=True,
        command="python /app/esg_scraper.py",
        docker_url='tcp://docker-socket-proxy:2375',
        network_mode="esg_finance_pipeline_default"
    )
    run_scraper