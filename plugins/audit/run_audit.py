"""Write a lightweight per-run audit record to Postgres.

This is intentionally simple: one row per Airflow run, updated idempotently
via a (dag_id, run_id) unique constraint.
"""

from __future__ import annotations

import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

logger = logging.getLogger(__name__)


def write_run_audit(ti, dag_run=None, **context):
    """Collect and persist run metrics.

    Metrics are sourced from:
    - XCom returns of upstream tasks (load + upload)
    - Postgres query for remaining rows missing `s3_key`
    - Airflow run timestamps for duration
    """
    pg_hook = PostgresHook(postgres_conn_id="postgres_default")

    dag = context.get("dag")
    dag_id = getattr(dag, "dag_id", None) or context.get("dag_id") or "unknown_dag"
    run_id = getattr(dag_run, "run_id", None) or context.get("run_id") or "unknown_run"
    logical_date = getattr(dag_run, "logical_date", None) or context.get("logical_date")
    started_at = getattr(dag_run, "start_date", None)
    ended_at = timezone.utcnow()

    duration_seconds = None
    if started_at:
        duration_seconds = int((ended_at - started_at).total_seconds())

    load_metrics = ti.xcom_pull(task_ids="load_metadata_to_db") or {}
    upload_metrics = ti.xcom_pull(task_ids="download_and_upload_to_s3") or {}

    metadata_inserted = int(load_metrics.get("inserted", 0) or 0)
    metadata_updated = int(load_metrics.get("updated", 0) or 0)
    images_uploaded = int(upload_metrics.get("images_uploaded", 0) or 0)
    download_failures = int(upload_metrics.get("download_failures", 0) or 0)
    upload_failures = int(upload_metrics.get("upload_failures", 0) or 0)

    missing_s3_after = pg_hook.get_first(
        "SELECT COUNT(*) FROM draenei_content.wallpapers WHERE s3_key IS NULL;"
    )[0]

    sql = """
        INSERT INTO draenei_content.run_audit (
            dag_id,
            run_id,
            logical_date,
            started_at,
            ended_at,
            duration_seconds,
            metadata_inserted,
            metadata_updated,
            images_uploaded,
            download_failures,
            upload_failures,
            missing_s3_after_run,
            updated_at
        )
        VALUES (
            %(dag_id)s,
            %(run_id)s,
            %(logical_date)s,
            %(started_at)s,
            %(ended_at)s,
            %(duration_seconds)s,
            %(metadata_inserted)s,
            %(metadata_updated)s,
            %(images_uploaded)s,
            %(download_failures)s,
            %(upload_failures)s,
            %(missing_s3_after_run)s,
            NOW()
        )
        ON CONFLICT (dag_id, run_id) DO UPDATE SET
            logical_date = EXCLUDED.logical_date,
            started_at = EXCLUDED.started_at,
            ended_at = EXCLUDED.ended_at,
            duration_seconds = EXCLUDED.duration_seconds,
            metadata_inserted = EXCLUDED.metadata_inserted,
            metadata_updated = EXCLUDED.metadata_updated,
            images_uploaded = EXCLUDED.images_uploaded,
            download_failures = EXCLUDED.download_failures,
            upload_failures = EXCLUDED.upload_failures,
            missing_s3_after_run = EXCLUDED.missing_s3_after_run,
            updated_at = NOW();
    """

    params = {
        "dag_id": dag_id,
        "run_id": run_id,
        "logical_date": logical_date,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_seconds": duration_seconds,
        "metadata_inserted": metadata_inserted,
        "metadata_updated": metadata_updated,
        "images_uploaded": images_uploaded,
        "download_failures": download_failures,
        "upload_failures": upload_failures,
        "missing_s3_after_run": int(missing_s3_after),
    }

    pg_hook.run(sql, parameters=params)

    logger.info(
        "[draenei][audit] run_id=%s inserted=%s updated=%s uploaded=%s dl_fail=%s ul_fail=%s missing_s3=%s duration_s=%s",
        run_id,
        metadata_inserted,
        metadata_updated,
        images_uploaded,
        download_failures,
        upload_failures,
        missing_s3_after,
        duration_seconds,
    )

    return params

