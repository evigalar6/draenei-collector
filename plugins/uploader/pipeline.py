"""Download images referenced in Postgres and upload them to S3."""

import time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from uploader.s3_manager import S3Manager


def download_and_upload_images():
    """Upload images that have not yet been stored in S3.

    Source:
        Postgres table `draenei_content.wallpapers`, filtered by `s3_key IS NULL`.

    Transform:
        Download each URL into memory and derive a destination key using the
        Wallhaven id and the URL extension (default: `jpg`).

    Destination:
        Upload objects to S3 and update `s3_key` and `updated_at` in Postgres.

    Idempotency:
        The query filters on `s3_key IS NULL`, so already-uploaded rows are
        skipped on retries/reruns.

    Side Effects:
        Reads/writes Postgres, performs HTTP downloads and S3 uploads, sleeps
        between uploads, and prints status messages to stdout.
    """
    manager = S3Manager()
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    # Limit batch size to keep each run bounded.
    records = pg_hook.get_records("""
        SELECT id, url, wallhaven_id FROM draenei_content.wallpapers 
        WHERE s3_key IS NULL
        LIMIT 5;
    """)

    print(f"📦 Знайдено {len(records)} незавантажених картинок.")

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
                print(f"✨ Базу оновлено для ID {db_id}")
            else:
                print(f"⚠️ Не вдалося залити в S3 ID {db_id}")

        time.sleep(1)
