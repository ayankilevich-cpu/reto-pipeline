-- ============================================================
-- DDL — Tablas para evaluación Art. 510 del Código Penal
-- Ejecutar una sola vez contra reto_db.
-- ============================================================

CREATE TABLE IF NOT EXISTS processed.evaluacion_art510 (
    message_uuid        UUID        NOT NULL REFERENCES processed.mensajes(message_uuid),
    label_source        VARCHAR(20) NOT NULL,   -- 'llm' | 'humano'
    es_potencial_delito BOOLEAN     NOT NULL,
    apartado_510        VARCHAR(5),             -- '1a' | '1b' | '1c' | NULL
    grupo_protegido     VARCHAR(100),
    conducta_detectada  VARCHAR(100),
    justificacion       TEXT,
    confianza           VARCHAR(10),            -- 'alta' | 'media' | 'baja'
    evaluacion_date     TIMESTAMPTZ DEFAULT NOW(),
    llm_version         VARCHAR(50) DEFAULT 'v1',
    PRIMARY KEY (message_uuid, label_source)
);

CREATE TABLE IF NOT EXISTS processed.validacion_art510_humana (
    message_uuid            UUID        NOT NULL REFERENCES processed.mensajes(message_uuid),
    label_source            VARCHAR(20) NOT NULL,
    validacion_humana       VARCHAR(20) NOT NULL, -- 'confirmado' | 'rechazado' | 'corregido'
    apartado_510_final      VARCHAR(5),
    grupo_protegido_final   VARCHAR(100),
    conducta_final          VARCHAR(100),
    comentario              TEXT,
    annotator_id            VARCHAR(50) NOT NULL,
    annotation_date         DATE        NOT NULL,
    PRIMARY KEY (message_uuid, label_source)
);
