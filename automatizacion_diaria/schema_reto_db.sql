-- ============================================================
-- RETO DB — Schema de datos del pipeline
-- Base de datos: reto_db
-- Fecha de creación: 2026-02-11
--
-- Schemas existentes (no tocar): delitos, public, reto
-- Schemas nuevos: raw, processed
--
-- Uso:
--   psql -h localhost -U postgres -d reto_db -f schema_reto_db.sql
-- ============================================================

-- ========================================================
-- SCHEMAS
-- ========================================================
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS processed;

-- ========================================================
-- RAW.MENSAJES  — datos crudos tal como llegan del pipeline
-- Fuente: consolidar_csv.py (X) / youtube equivalente
-- ========================================================
CREATE TABLE IF NOT EXISTS raw.mensajes (
    message_uuid    UUID            PRIMARY KEY,
    platform        VARCHAR(20)     NOT NULL,
    tweet_id        VARCHAR(50),
    created_at      TIMESTAMPTZ,
    content_original TEXT           NOT NULL,
    author_username VARCHAR(100),
    author_id       VARCHAR(50),
    source_media    VARCHAR(200),
    batch_id        VARCHAR(100),
    scrape_date     TIMESTAMPTZ,
    language        VARCHAR(10),
    url             TEXT,
    retweet_count   INTEGER         DEFAULT 0,
    reply_count     INTEGER         DEFAULT 0,
    like_count      INTEGER         DEFAULT 0,
    quote_count     INTEGER         DEFAULT 0,
    ingested_at     TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_mensajes_platform   ON raw.mensajes (platform);
CREATE INDEX IF NOT EXISTS idx_raw_mensajes_created_at ON raw.mensajes (created_at);
CREATE INDEX IF NOT EXISTS idx_raw_mensajes_source     ON raw.mensajes (source_media);

-- ========================================================
-- PROCESSED.MENSAJES  — anonimizados + candidatos
-- Fuente: filter_and_anonymize_x.py
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.mensajes (
    message_uuid        UUID        PRIMARY KEY REFERENCES raw.mensajes(message_uuid),
    platform            VARCHAR(20) NOT NULL,
    content_original    TEXT        NOT NULL,
    source_media        VARCHAR(200),
    created_at          TIMESTAMPTZ,
    language            VARCHAR(10),
    url                 TEXT,
    author_id_anon      VARCHAR(64),
    author_username_anon VARCHAR(64),
    matched_terms       TEXT,
    has_hate_terms_match BOOLEAN    DEFAULT FALSE,
    match_count         INTEGER     DEFAULT 0,
    strong_phrase       BOOLEAN     DEFAULT FALSE,
    is_candidate        BOOLEAN     DEFAULT FALSE,
    candidate_reason    TEXT,
    processed_at        TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_proc_mensajes_platform    ON processed.mensajes (platform);
CREATE INDEX IF NOT EXISTS idx_proc_mensajes_candidate   ON processed.mensajes (is_candidate);
CREATE INDEX IF NOT EXISTS idx_proc_mensajes_hate_match  ON processed.mensajes (has_hate_terms_match);
CREATE INDEX IF NOT EXISTS idx_proc_mensajes_created_at  ON processed.mensajes (created_at);

-- ========================================================
-- PROCESSED.SCORES  — resultados de modelos (baseline, etc.)
-- Fuente: score_baseline.py
-- PK compuesta: un score por modelo por mensaje
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.scores (
    message_uuid    UUID            NOT NULL REFERENCES processed.mensajes(message_uuid),
    model_version   VARCHAR(100)    NOT NULL,
    proba_odio      DOUBLE PRECISION,
    pred_odio       INTEGER,
    priority        VARCHAR(10),
    score_date      TIMESTAMPTZ     DEFAULT NOW(),
    PRIMARY KEY (message_uuid, model_version)
);

CREATE INDEX IF NOT EXISTS idx_scores_priority ON processed.scores (priority);
CREATE INDEX IF NOT EXISTS idx_scores_pred     ON processed.scores (pred_odio);

-- ========================================================
-- PROCESSED.ETIQUETAS_LLM  — clasificación del LLM
-- Fuente: etiquetar_completo_llm.py
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.etiquetas_llm (
    message_uuid            UUID        NOT NULL REFERENCES processed.mensajes(message_uuid),
    clasificacion_principal VARCHAR(20),
    categoria_odio_pred     VARCHAR(100),
    intensidad_pred         VARCHAR(5),
    resumen_motivo          TEXT,
    etiquetado_date         TIMESTAMPTZ DEFAULT NOW(),
    llm_version             VARCHAR(50) NOT NULL DEFAULT 'v1',
    PRIMARY KEY (message_uuid, llm_version)
);

CREATE INDEX IF NOT EXISTS idx_llm_clasif ON processed.etiquetas_llm (clasificacion_principal);

-- ========================================================
-- PROCESSED.VALIDACIONES_MANUALES  — correcciones humanas
-- Fuente: Google Sheet (columnas H-M) importadas al pipeline
-- Una validación por mensaje
-- Categorías válidas:
--   odio_etnico_cultural_religioso
--   odio_genero_identidad_orientacion
--   odio_condicion_social_economica_salud
--   odio_ideologico_politico
--   odio_personal_generacional
--   odio_profesiones_roles_publicos
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.validaciones_manuales (
    message_uuid     UUID        PRIMARY KEY REFERENCES processed.mensajes(message_uuid),
    odio_flag        BOOLEAN,
    categoria_odio   VARCHAR(100),
    intensidad       SMALLINT    CHECK (intensidad BETWEEN 1 AND 3),
    humor_flag       BOOLEAN,
    annotator_id     VARCHAR(50),
    annotation_date  DATE,
    coincide_con_llm BOOLEAN,
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_val_odio       ON processed.validaciones_manuales (odio_flag);
CREATE INDEX IF NOT EXISTS idx_val_categoria  ON processed.validaciones_manuales (categoria_odio);
CREATE INDEX IF NOT EXISTS idx_val_annotator  ON processed.validaciones_manuales (annotator_id);

-- ========================================================
-- PROCESSED.GOLD_DATASET  — labels reconciliados (humano + LLM)
-- Fuente: X_Etiquetado_LLM_V1.xlsx
-- Contiene los labels finales para entrenamiento ML
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.gold_dataset (
    message_uuid        UUID        PRIMARY KEY,
    y_odio_final        VARCHAR(20),
    y_odio_bin          SMALLINT    CHECK (y_odio_bin IN (0, 1)),
    y_categoria_final   VARCHAR(100),
    y_intensidad_final  SMALLINT    CHECK (y_intensidad_final BETWEEN 1 AND 3),
    corrigio_odio       BOOLEAN     DEFAULT FALSE,
    corrigio_categoria  BOOLEAN     DEFAULT FALSE,
    corrigio_intensidad BOOLEAN     DEFAULT FALSE,
    label_source        VARCHAR(50),
    split               VARCHAR(10) CHECK (split IN ('TRAIN', 'TEST')),
    ingested_at         TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_gold_split ON processed.gold_dataset (split);
CREATE INDEX IF NOT EXISTS idx_gold_odio  ON processed.gold_dataset (y_odio_bin);

-- ========================================================
-- PROCESSED.RESUMEN_DIARIO  — métricas agregadas por día
-- Se rellena al final del pipeline
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.resumen_diario (
    fecha               DATE        NOT NULL,
    platform            VARCHAR(20) NOT NULL,
    total_mensajes_raw  INTEGER     DEFAULT 0,
    total_candidatos    INTEGER     DEFAULT 0,
    total_odio_baseline INTEGER     DEFAULT 0,
    total_odio_llm      INTEGER     DEFAULT 0,
    score_promedio      DOUBLE PRECISION,
    PRIMARY KEY (fecha, platform)
);

-- ========================================================
-- PROCESSED.ANALISIS_SEMANAL  — análisis contextual semanal
-- Generado por analisis_contexto_semanal.py
-- ========================================================
CREATE TABLE IF NOT EXISTS processed.analisis_semanal (
    semana_inicio       DATE        PRIMARY KEY,
    semana_fin          DATE        NOT NULL,
    total_mensajes      INTEGER     DEFAULT 0,
    total_odio          INTEGER     DEFAULT 0,
    pct_odio            NUMERIC(5,2),
    es_spike            BOOLEAN     DEFAULT FALSE,
    promedio_referencia_pct NUMERIC(6,2),
    umbral_spike_pct    NUMERIC(6,2),
    n_semanas_base      INTEGER,
    categorias          JSONB,
    targets             JSONB,
    temas               JSONB,
    intensidad          JSONB,
    dia_pico            DATE,
    dia_pico_odio       INTEGER,
    dia_pico_pct        NUMERIC(5,2),
    resumen_contexto    TEXT,
    eventos_relacionados TEXT,
    analisis_date       TIMESTAMPTZ DEFAULT NOW()
);
