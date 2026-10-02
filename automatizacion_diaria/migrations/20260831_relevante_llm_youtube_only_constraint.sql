-- Migración: constraint preventivo para relevante_llm (solo YouTube)
-- Fecha: 2026-08-31
--
-- Contexto: se investigó una supuesta corrupción de relevante_llm en plataformas
-- distintas de YouTube (valores ODIO/NO_ODIO/DUDOSO o SI/NO en X). Verificación
-- en reto_db producción (2026-08-31): 0 filas afectadas — falso positivo, probable
-- grilla/cache vieja de pgAdmin. No se aplicó ningún UPDATE correctivo.
--
-- Este archivo solo agrega un CHECK para que relevante_llm solo pueda poblarse en
-- platform = 'youtube'.
--
-- Aplica UNA sola vez (requiere rol con ALTER TABLE, ej. neondb_owner):
--   psql $DATABASE_URL -f automatizacion_diaria/migrations/20260831_relevante_llm_youtube_only_constraint.sql
--
-- Verificar:
--   SELECT conname, pg_get_constraintdef(oid)
--   FROM pg_constraint
--   WHERE conname = 'chk_relevante_llm_youtube_only';

ALTER TABLE processed.mensajes
ADD CONSTRAINT chk_relevante_llm_youtube_only
CHECK (relevante_llm IS NULL OR platform = 'youtube');
