"""Download images referenced in Postgres and upload them to S3."""

import logging
import os
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from uploader.s3_manager import S3Manager

logger = logging.getLogger(__name__)


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
    batch_limit = int(os.getenv("UPLOAD_BATCH_LIMIT", "5"))

    records = pg_hook.get_records(f"""
        SELECT id, url, wallhaven_id
        FROM draenei_content.wallpapers
        WHERE s3_key IS NULL
        LIMIT {batch_limit};
    """)

    logger.info("[draenei] Found %s images pending upload (limit=%s).", len(records), batch_limit)

    uploaded = 0
    download_failures = 0
    upload_failures = 0
    started = time.monotonic()

    for row in records:
        db_id, image_url, wall_id = row
        logger.info("[draenei] Downloading id=%s url=%s", db_id, image_url)

        file_bytes = manager.download_image_as_bytes(image_url)

        if not file_bytes:
            download_failures += 1
            continue

        ext = image_url.split('.')[-1] if '.' in image_url else 'jpg'
        ext = ext.split("?")[0].lower()
        s3_key = f"wallpapers/{wall_id}.{ext}"

        if manager.upload_file(file_bytes, s3_key):
            sql_update = """
                UPDATE draenei_content.wallpapers
                SET s3_key = %s, updated_at = NOW()
                WHERE id = %s;
            """
            pg_hook.run(sql_update, parameters=(s3_key, db_id))
            uploaded += 1
            logger.info("[draenei] Updated Postgres id=%s s3_key=%s", db_id, s3_key)
        else:
            upload_failures += 1
            logger.warning("[draenei] Failed S3 upload id=%s key=%s", db_id, s3_key)

        time.sleep(1)

    duration_s = int(time.monotonic() - started)
    logger.info(
        "[draenei] Upload task summary uploaded=%s download_failures=%s upload_failures=%s duration_s=%s",
        uploaded,
        download_failures,
        upload_failures,
        duration_s,
    )

    return {
        "images_uploaded": uploaded,
        "download_failures": download_failures,
        "upload_failures": upload_failures,
        "duration_s": duration_s,
    }
