from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from datetime import timedelta
import time
import random


# Імпорти твоїх модулів
from collector.scraper import scrape_draenei_metadata
from uploader.s3_manager import S3Manager

# --- Налаштування DAG ---
default_args = {
    'owner': 'evi',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        dag_id='draenei_content_loader_v3_fixed',  # Змінив ID, щоб точно оновилося
        default_args=default_args,
        description='ETL pipeline: Wallhaven -> Postgres -> S3',
        schedule_interval='@daily',
        start_date=days_ago(1),
        catchup=False,
        tags=['draenei', 'portfolio'],
) as dag:
    # --- STEP 1: EXTRACT ---
    # 1. Спочатку визначаємо функцію
    def extract_data(**kwargs):
        # Можеш змінити limit=10, щоб качати більше
        random_page = random.randint(1, 2)
        print(f"🎲 Тягнемо сторінку №{random_page}")
        data = scrape_draenei_metadata(query="draenei", limit=10, page=random_page)
        return data


    # 2. Потім створюємо таск
    task_extract = PythonOperator(
        task_id='extract_metadata',
        python_callable=extract_data,
    )


    # --- STEP 2: LOAD METADATA ---
    # 1. Спочатку функція!
    def load_metadata_to_db(ti):
        metadata_list = ti.xcom_pull(task_ids='extract_metadata')

        if not metadata_list:
            print("⚠️ Даних немає. Пропускаємо.")
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        insert_query = """
            INSERT INTO draenei_content.wallpapers 
            (wallhaven_id, url, resolution, category, purity, file_size)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (wallhaven_id) DO NOTHING;
        """

        rows_to_insert = []
        for item in metadata_list:
            rows_to_insert.append((
                item['wallhaven_id'],
                item['url'],
                item['resolution'],
                item['category'],
                item['purity'],
                item['file_size']
            ))

        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.executemany(insert_query, rows_to_insert)
        connection.commit()
        cursor.close()
        connection.close()
        print(f"✅ Вставлено {len(rows_to_insert)} записів.")


    # 2. Потім таск (тут була помилка)
    task_load_db = PythonOperator(
        task_id='load_metadata_to_db',
        python_callable=load_metadata_to_db,  # Python шукає цю назву ВИЩЕ по коду
    )


    # --- STEP 3: UPLOAD TO S3 ---
    # 1. Спочатку функція!
    def download_and_upload_images():
        manager = S3Manager()
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Беремо 5 штук за раз
        records = pg_hook.get_records("""
            SELECT id, url, wallhaven_id FROM draenei_content.wallpapers 
            WHERE s3_key IS NULL
            LIMIT 5;
        """)

        print(f"📦 Знайдено {len(records)} картинок для завантаження.")

        for row in records:
            db_id, image_url, wall_id = row
            print(f"⬇️ Качаю ID {db_id}: {image_url}")

            file_bytes = manager.download_image_as_bytes(image_url)

            if file_bytes:
                ext = image_url.split('.')[-1] if '.' in image_url else 'jpg'
                s3_key = f"wallpapers/{wall_id}.{ext}"

                if manager.upload_file(file_bytes, s3_key):
                    sql_update = """
                        UPDATE draenei_content.wallpapers 
                        SET s3_key = %s, updated_at = NOW() 
                        WHERE id = %s;
                    """
                    pg_hook.run(sql_update, parameters=(s3_key, db_id))
                    print(f"✨ Готово: {s3_key}")
                else:
                    print(f"⚠️ Помилка S3 для {db_id}")

            time.sleep(1)


    # 2. Потім таск
    task_upload_s3 = PythonOperator(
        task_id='download_and_upload_to_s3',
        python_callable=download_and_upload_images,
    )

    # --- Порядок ---
    task_extract >> task_load_db >> task_upload_s3