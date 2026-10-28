from airflow import DAG 
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 10, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'esg_scraper_pipeline',
    default_args=default_args,
    description='A simple DAG to run the ESG Scraper container',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['esg', 'scraper'],
) as dag:
    run_scraper=DockerOperator(
        task_id='run_esg_scraper',
        image='esg_finance_pipeline-scraper_app:latest',
        container_name='task_esg_scraper',
        api_version='auto',
        auto_remove=True,
        command="python /app/esg_scraper.py",
        docker_url='tcp://docker-socket-proxy:2375',
        network_mode="bridge"
    )
    run_scraper