"""Load scraped metadata into Postgres for deduplication and downstream use."""

import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)


def load_metadata_to_db(ti):
    """Insert scraped metadata pulled from XCom into Postgres.

    This task reads metadata produced by the upstream `extract_metadata` task and
    upserts it into `draenei_content.wallpapers`.

    Idempotency:
        Upserts are keyed by `wallhaven_id` via `ON CONFLICT ... DO UPDATE`.
        This keeps the table "fresh" while staying safe on reruns.

    Args:
        ti: Airflow TaskInstance used to pull data from XCom.

    Side Effects:
        Writes to Postgres and logs a per-run summary.
    """
    # Pull extracted metadata from the upstream task's XCom.
    metadata_list = ti.xcom_pull(task_ids='extract_metadata')

    if not metadata_list:
        logger.info("[draenei] No metadata found; skipping DB load.")
        return {"extracted": 0, "inserted": 0, "updated": 0, "invalid": 0}

    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    rows = []
    invalid = 0
    for item in metadata_list:
        wallhaven_id = item.get("wallhaven_id")
        url = item.get("url")
        if not wallhaven_id or not url:
            invalid += 1
            continue

        rows.append((
            wallhaven_id,
            url,
            item.get("resolution"),
            item.get("category"),
            item.get("purity"),
            item.get("file_size"),
        ))

    if not rows:
        logger.warning("[draenei] All extracted records were invalid; nothing to upsert.")
        return {"extracted": len(metadata_list), "inserted": 0, "updated": 0, "invalid": invalid}

    # Count inserted vs updated using a Postgres trick:
    # `xmax = 0` is true for freshly inserted rows in the current statement.
    upsert_sql = """
        INSERT INTO draenei_content.wallpapers
            (wallhaven_id, url, resolution, category, purity, file_size)
        VALUES %s
        ON CONFLICT (wallhaven_id) DO UPDATE SET
            url = EXCLUDED.url,
            resolution = EXCLUDED.resolution,
            category = EXCLUDED.category,
            purity = EXCLUDED.purity,
            file_size = EXCLUDED.file_size,
            updated_at = NOW()
        RETURNING (xmax = 0) AS inserted;
    """

    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    execute_values(cursor, upsert_sql, rows, page_size=500)
    inserted_flags = cursor.fetchall()
    connection.commit()
    cursor.close()
    connection.close()

    inserted = sum(1 for (is_inserted,) in inserted_flags if is_inserted)
    updated = len(inserted_flags) - inserted
    logger.info(
        "[draenei] DB upsert summary extracted=%s valid=%s invalid=%s inserted=%s updated=%s",
        len(metadata_list),
        len(rows),
        invalid,
        inserted,
        updated,
    )

    return {"extracted": len(metadata_list), "inserted": inserted, "updated": updated, "invalid": invalid}
