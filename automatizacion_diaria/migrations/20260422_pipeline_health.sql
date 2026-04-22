CREATE TABLE IF NOT EXISTS processed.pipeline_health (
    id                        BIGSERIAL PRIMARY KEY,
    run_id                    VARCHAR(80) NOT NULL,
    run_at                    TIMESTAMPTZ NOT NULL,
    pipeline_name             VARCHAR(60) NOT NULL DEFAULT 'reto_pipeline_diario',
    platform                  VARCHAR(20) NOT NULL,
    last_ingested_at          TIMESTAMPTZ,
    hours_since_last_ingest   DOUBLE PRECISION,
    rows_new_window           INTEGER NOT NULL DEFAULT 0,
    stagnated                 BOOLEAN NOT NULL DEFAULT FALSE,
    critical_stage_ok         BOOLEAN NOT NULL DEFAULT TRUE,
    failed_stages             TEXT,
    warnings                  TEXT,
    errors                    TEXT,
    created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_health_platform_run_at
    ON processed.pipeline_health (platform, run_at DESC);
