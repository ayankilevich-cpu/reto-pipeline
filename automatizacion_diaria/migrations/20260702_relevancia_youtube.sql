-- Migración: columnas de relevancia LLM para YouTube en processed.mensajes
-- Fecha: 2026-07-02
-- Script: Medios/ML/etiquetado_llm/filtrar_relevancia_youtube.py
--
-- Aplica UNA sola vez:
--   psql $DATABASE_URL -f automatizacion_diaria/migrations/20260702_relevancia_youtube.sql
--
-- Verificar:
--   SELECT column_name, data_type
--   FROM information_schema.columns
--   WHERE table_schema = 'processed'
--     AND table_name = 'mensajes'
--     AND column_name IN ('relevante_llm', 'relevante_score', 'relevante_motivo');
--   → deben aparecer 3 filas.

ALTER TABLE processed.mensajes
    ADD COLUMN IF NOT EXISTS relevante_llm     VARCHAR(5),       -- 'SI' | 'NO' | NULL
    ADD COLUMN IF NOT EXISTS relevante_score   DOUBLE PRECISION, -- [0.0, 1.0]
    ADD COLUMN IF NOT EXISTS relevante_motivo  TEXT;             -- motivo del LLM

CREATE INDEX IF NOT EXISTS idx_proc_mensajes_relevante_llm
    ON processed.mensajes (platform, relevante_llm)
    WHERE platform = 'youtube';
