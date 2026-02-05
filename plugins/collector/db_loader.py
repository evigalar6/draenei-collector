"""Load scraped metadata into Postgres for deduplication and downstream use."""

from airflow.providers.postgres.hooks.postgres import PostgresHook


def load_metadata_to_db(ti):
    """Insert scraped metadata pulled from XCom into Postgres.

    This task reads metadata produced by the upstream `extract_metadata` task and
    inserts it into `draenei_content.wallpapers`. Inserts are idempotent via
    `ON CONFLICT (wallhaven_id) DO NOTHING`.

    Args:
        ti: Airflow TaskInstance used to pull data from XCom.

    Side Effects:
        Writes to Postgres and prints status messages to stdout.
    """
    # Pull extracted metadata from the upstream task's XCom.
    metadata_list = ti.xcom_pull(task_ids='extract_metadata')

    if not metadata_list:
        print("⚠️ Даних немає. Пропускаємо запис у БД.")
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
    print(f"✅ Метадані оброблено: {len(rows_to_insert)} записів.")
