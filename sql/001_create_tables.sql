-- Bootstrap schema for draenei-collector.
-- This runs automatically on first Postgres start via /docker-entrypoint-initdb.d.

CREATE SCHEMA IF NOT EXISTS draenei_content;

-- Primary metadata table (deduplication happens on wallhaven_id).
CREATE TABLE IF NOT EXISTS draenei_content.wallpapers (
    id            BIGSERIAL PRIMARY KEY,
    wallhaven_id  TEXT NOT NULL,
    url           TEXT NOT NULL,
    resolution    TEXT,
    category      TEXT,
    purity        TEXT,
    file_size     BIGINT,
    s3_key        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ
);

-- Enforce idempotency: one row per Wallhaven wallpaper id.
CREATE UNIQUE INDEX IF NOT EXISTS ux_wallpapers_wallhaven_id
    ON draenei_content.wallpapers (wallhaven_id);

-- Speed up the uploader query (only rows missing s3_key).
CREATE INDEX IF NOT EXISTS ix_wallpapers_pending_upload
    ON draenei_content.wallpapers (id)
    WHERE s3_key IS NULL;

-- Lightweight per-run audit table (written by an Airflow task at the end of the DAG).
CREATE TABLE IF NOT EXISTS draenei_content.run_audit (
    id                   BIGSERIAL PRIMARY KEY,
    dag_id               TEXT NOT NULL,
    run_id               TEXT NOT NULL,
    logical_date         TIMESTAMPTZ,
    started_at           TIMESTAMPTZ,
    ended_at             TIMESTAMPTZ,
    duration_seconds     INTEGER,
    metadata_inserted    INTEGER NOT NULL DEFAULT 0,
    metadata_updated     INTEGER NOT NULL DEFAULT 0,
    images_uploaded      INTEGER NOT NULL DEFAULT 0,
    download_failures    INTEGER NOT NULL DEFAULT 0,
    upload_failures      INTEGER NOT NULL DEFAULT 0,
    missing_s3_after_run INTEGER NOT NULL DEFAULT 0,
    notes                TEXT,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (dag_id, run_id)
);

