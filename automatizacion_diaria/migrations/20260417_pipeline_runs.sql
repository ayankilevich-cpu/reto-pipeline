-- ============================================================
-- Migración: processed.pipeline_runs
-- Fecha: 2026-04-17
--
-- Tabla de control para registrar ejecuciones del pipeline
-- diario (X). Permite mostrar en la app:
--   - si el pipeline corrió hoy / última fecha de corrida
--   - si la corrida detectó cambios o no
--   - estado (ok / error / partial)
--   - origen (scheduled / catch_up / manual)
--
-- No reemplaza processed.resumen_diario (esa tabla se
-- rellena sólo cuando hay datos cargados). pipeline_runs
-- deja constancia aunque no haya cambios.
-- ============================================================

CREATE TABLE IF NOT EXISTS processed.pipeline_runs (
    id              SERIAL          PRIMARY KEY,
    pipeline_name   VARCHAR(60)     NOT NULL DEFAULT 'reto_x_diario',
    started_at      TIMESTAMPTZ     NOT NULL,
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(16)     NOT NULL,            -- ok | error | partial
    changes_detected BOOLEAN        NOT NULL DEFAULT FALSE,
    ok_count        INTEGER         DEFAULT 0,
    fail_count      INTEGER         DEFAULT 0,
    triggered_by    VARCHAR(20)     DEFAULT 'scheduled', -- scheduled | catch_up | manual
    detail          TEXT,
    host            VARCHAR(100),
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started_at
    ON processed.pipeline_runs (started_at DESC);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_pipeline_started
    ON processed.pipeline_runs (pipeline_name, started_at DESC);
