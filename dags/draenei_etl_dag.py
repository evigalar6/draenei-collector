from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Імпортуємо чисті функції з наших модулів
from collector.scraper import scrape_random_batch
from collector.db_loader import load_metadata_to_db
from uploader.pipeline import download_and_upload_images

doc_md_DAG = """


<img src="/include/my_image.png" alt="My Image" width="500"/> 
"""

# --- Налаштування ---
default_args = {
    'owner': 'evi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# --- DAG Definition ---
with DAG(
    dag_id='draenei_content_loader_v4_clean', # Нова версія
    default_args=default_args,
    description='ETL pipeline: Wallhaven -> Postgres -> S3',
    doc_md='![draenei](https://raw.githubusercontent.com/evigalar6/draenei-collector/8645632523d122310184a5a52c5d38978acd63fd/dags/85ezqj.jpg)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['draenei', 'portfolio', 'wallpapers'],
) as dag:

    # 1. Extract
    task_extract = PythonOperator(
        task_id='extract_metadata',
        python_callable=scrape_random_batch,
    )

    # 2. Load Metadata (DB)
    task_load_db = PythonOperator(
        task_id='load_metadata_to_db',
        python_callable=load_metadata_to_db,
    )

    # 3. Upload Files (S3)
    task_upload_s3 = PythonOperator(
        task_id='download_and_upload_to_s3',
        python_callable=download_and_upload_images,
    )

    # Pipeline
    task_extract >> task_load_db >> task_upload_s3