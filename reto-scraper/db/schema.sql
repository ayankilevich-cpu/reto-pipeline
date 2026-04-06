-- Esquema base para el piloto del scraper de discurso de odio.
-- Ejecutar con un rol con permisos de creación (ej. db_admin).

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Tipos auxiliares
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'crawl_job_status') THEN
    CREATE TYPE crawl_job_status AS ENUM ('pending', 'running', 'succeeded', 'failed', 'skipped');
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'label_class') THEN
    CREATE TYPE label_class AS ENUM ('hate', 'non_hate', 'ambiguous', 'undetermined');
  END IF;
END$$;

-- Tabla de control de ejecuciones
CREATE TABLE IF NOT EXISTS crawl_jobs (
    job_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    network TEXT NOT NULL,
    term TEXT NOT NULL,
    search_window tstzrange NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    status crawl_job_status NOT NULL DEFAULT 'pending',
    retries SMALLINT NOT NULL DEFAULT 0,
    error TEXT,
    metadata JSONB DEFAULT '{}'::JSONB
);

CREATE INDEX IF NOT EXISTS idx_crawl_jobs_network_term ON crawl_jobs (network, term);
CREATE INDEX IF NOT EXISTS idx_crawl_jobs_status ON crawl_jobs (status);

DO $$
BEGIN
  IF NOT EXISTS (
      SELECT 1
      FROM pg_constraint
      WHERE conname = 'uq_crawl_jobs_window'
  ) THEN
      ALTER TABLE crawl_jobs
          ADD CONSTRAINT uq_crawl_jobs_window UNIQUE (network, term, search_window);
  END IF;
END$$;

-- Mensajes sin procesar (raw)
CREATE TABLE IF NOT EXISTS raw_messages (
    message_uuid UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    network TEXT NOT NULL,
    network_message_id TEXT NOT NULL,
    job_id UUID REFERENCES crawl_jobs(job_id) ON DELETE SET NULL,
    author_id TEXT,
    author_handle TEXT,
    author_name TEXT,
    content TEXT NOT NULL,
    content_clean TEXT,
    language TEXT,
    published_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    permalink TEXT,
    like_count INTEGER,
    reply_count INTEGER,
    repost_count INTEGER,
    quote_count INTEGER,
    extra JSONB DEFAULT '{}'::JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE raw_messages
    ADD CONSTRAINT uq_raw_messages_network_id UNIQUE (network, network_message_id);

CREATE INDEX IF NOT EXISTS idx_raw_messages_network_published ON raw_messages (network, published_at DESC);
CREATE INDEX IF NOT EXISTS idx_raw_messages_job_id ON raw_messages (job_id);

-- Coincidencias de términos
CREATE TABLE IF NOT EXISTS term_hits (
    hit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_uuid UUID NOT NULL REFERENCES raw_messages(message_uuid) ON DELETE CASCADE,
    job_id UUID REFERENCES crawl_jobs(job_id) ON DELETE SET NULL,
    term TEXT NOT NULL,
    term_variant TEXT,
    match_type TEXT NOT NULL, -- exact, ngram, boundary, etc.
    match_start INTEGER,
    match_end INTEGER,
    snippet TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_term_hits_message ON term_hits (message_uuid);
CREATE INDEX IF NOT EXISTS idx_term_hits_term ON term_hits (term);

-- Datos etiquetados
CREATE TABLE IF NOT EXISTS labeled_data (
    labeled_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    message_uuid UUID NOT NULL REFERENCES raw_messages(message_uuid) ON DELETE CASCADE,
    label label_class NOT NULL,
    confidence NUMERIC(3,2),
    labeler_id TEXT,
    labeler_team TEXT,
    notes TEXT,
    labeled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    version SMALLINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE labeled_data
    ADD CONSTRAINT uq_labeled_message_version UNIQUE (message_uuid, version);

CREATE INDEX IF NOT EXISTS idx_labeled_label ON labeled_data (label);

-- Roles sugeridos (ajustar según estrategia de seguridad)
-- GRANT USAGE ON SCHEMA public TO etl_reader, research_reader;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO etl_reader, research_reader;
-- GRANT INSERT, UPDATE ON raw_messages, term_hits TO etl_writer;
-- GRANT SELECT, INSERT, UPDATE ON crawl_jobs TO etl_writer;
-- GRANT SELECT, INSERT ON labeled_data TO research_reader;
-- etc.

