-- ============================================================
-- Migración T2.4 — Muestra y anotación interseccional
-- Fecha: 2026-10-08
-- Crea 2 tablas NUEVAS en processed. No modifica ninguna tabla existente.
-- Rollback: 20261008_anotacion_t24_rollback.sql
-- ============================================================
BEGIN;

-- 1) Muestra fija a anotar (bloque A = prevalencia aleatoria; B = interseccional dirigida)
CREATE TABLE IF NOT EXISTS processed.muestra_t24 (
    message_uuid     UUID        PRIMARY KEY REFERENCES processed.mensajes(message_uuid),
    bloque           CHAR(1)     NOT NULL CHECK (bloque IN ('A', 'B')),
    platform         VARCHAR(20) NOT NULL,
    estrato_mes      DATE,
    doble_anotacion  BOOLEAN     NOT NULL DEFAULT FALSE,
    creado_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2) Anotaciones con el esquema extendido (permite 2 anotadores por mensaje)
CREATE TABLE IF NOT EXISTS processed.anotacion_t24 (
    message_uuid                  UUID        NOT NULL REFERENCES processed.muestra_t24(message_uuid),
    annotator_id                  VARCHAR(50) NOT NULL,
    clasificacion                 VARCHAR(10) NOT NULL CHECK (clasificacion IN ('ODIO', 'NO_ODIO', 'DUDOSO')),
    colectivos                    TEXT[]      NOT NULL DEFAULT '{}',
    insulto_identitario_generico  BOOLEAN     NOT NULL DEFAULT FALSE,
    tipo_cruce                    VARCHAR(10) CHECK (tipo_cruce IN ('combinado', 'acumulado')),
    categoria_principal           VARCHAR(100),
    intensidad                    SMALLINT    CHECK (intensidad BETWEEN 1 AND 3),
    narrativas                    TEXT[]      NOT NULL DEFAULT '{}',
    humor_flag                    BOOLEAN     NOT NULL DEFAULT FALSE,
    observaciones                 TEXT,
    segundos_anotacion            INTEGER,
    sumado_a_gold                 BOOLEAN     NOT NULL DEFAULT FALSE,
    annotation_ts                 TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (message_uuid, annotator_id)
);

CREATE INDEX IF NOT EXISTS idx_anot_t24_annotator ON processed.anotacion_t24 (annotator_id);
CREATE INDEX IF NOT EXISTS idx_anot_t24_clasif    ON processed.anotacion_t24 (clasificacion);

COMMENT ON TABLE  processed.muestra_t24   IS 'Muestra fija del entregable T2.4 (A: aleatoria estratificada post 02/07/2026; B: ODIO con >=2 colectivos).';
COMMENT ON TABLE  processed.anotacion_t24 IS 'Anotación humana extendida T2.4: colectivos múltiples, tipo de cruce, narrativas. La primera anotación de cada mensaje se suma también a validaciones_manuales y gold_dataset (label_source = human_t24).';
COMMENT ON COLUMN processed.anotacion_t24.sumado_a_gold IS 'TRUE si esta anotación creó la fila en validaciones_manuales/gold_dataset. FALSE si el mensaje ya estaba validado antes o es la segunda anotación (doble).';

COMMIT;
