"""Airflow DAG for the Draenei content ingestion pipeline.

This DAG wires together three tasks:
1) Extract metadata from Wallhaven.
2) Load metadata into Postgres (deduplication layer).
3) Download images and upload them to S3.

Task implementations live in `plugins/` to keep the DAG definition minimal.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Import task callables from local modules.
from collector.scraper import scrape_random_batch
from collector.db_loader import load_metadata_to_db
from uploader.pipeline import download_and_upload_images
from audit.run_audit import write_run_audit

# Default task arguments.
default_args = {
    'owner': 'evi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

with DAG(
    dag_id='draenei_content_loader_v4_clean',  # Versioned DAG id.
    default_args=default_args,
    description='ETL pipeline: Wallhaven -> Postgres -> S3',
    doc_md='![draenei](https://raw.githubusercontent.com/evigalar6/draenei-collector/8645632523d122310184a5a52c5d38978acd63fd/dags/85ezqj.jpg)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['draenei', 'portfolio', 'wallpapers'],
) as dag:

    # 1) Extract
    task_extract = PythonOperator(
        task_id='extract_metadata',
        python_callable=scrape_random_batch,
    )

    # 2) Load metadata into Postgres
    task_load_db = PythonOperator(
        task_id='load_metadata_to_db',
        python_callable=load_metadata_to_db,
    )

    # 3) Download and upload images to S3
    task_upload_s3 = PythonOperator(
        task_id='download_and_upload_to_s3',
        python_callable=download_and_upload_images,
    )

    # 4) Write run-level audit metrics to Postgres
    task_audit = PythonOperator(
        task_id='audit_run',
        python_callable=write_run_audit,
    )

    # Dependency chain.
    task_extract >> task_load_db >> task_upload_s3 >> task_audit
