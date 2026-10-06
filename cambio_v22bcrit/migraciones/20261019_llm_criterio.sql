-- ============================================================================
-- Migración: criterio de etiquetado v22bcrit (opción A: sin serie paralela v1)
-- ============================================================================
-- ESTADO: PREPARADA, NO EJECUTADA. Se aplica en el paso 2 de PLAN.md.
--
-- Qué hace:
--   * processed.etiquetas_llm.llm_criterio  TEXT DEFAULT 'v1'
--       'v1' | 'v22bcrit'. La PK (message_uuid, llm_version) NO cambia y
--       llm_version sigue siendo 'v1'.
--   * processed.analisis_semanal.llm_criterio TEXT DEFAULT 'v1'
--       criterio de la semana (todos sus mensajes comparten criterio).
--   * processed.semaforo_diario: NO se toca. El semáforo X usa processed.scores
--       (modelo ML prefiltro, priority='alta'), no etiquetas_llm: no depende del
--       criterio LLM. YouTube usa volumen crudo.
--
-- Seguridad:
--   * ADD COLUMN con DEFAULT constante es solo de catálogo en PostgreSQL >= 11
--     (Neon lo es): no reescribe la tabla; las filas existentes leen 'v1'.
--   * lock_timeout evita dejar bloqueados a los dashboards: si no consigue el
--     bloqueo en 5 s, aborta sin cambios. Se puede reintentar.
--   * Es aditiva: el código actual (que no conoce la columna) sigue funcionando.
--   * La columna debe existir ANTES de desplegar los parches de código.
--
-- Uso (cuando toque):
--   psql "$DATABASE_URL" -f migraciones/20261019_llm_criterio.sql
-- Rollback: migraciones/20261019_llm_criterio_rollback.sql
-- ============================================================================

-- ── PRE-CHEQUEO (solo lectura; ejecutar ANTES y revisar el resultado) ────────
-- Vistas/objetos que dependen de etiquetas_llm o analisis_semanal. Añadir una
-- columna no rompe vistas existentes, pero conviene saber cuáles hay.
--   SELECT schemaname, viewname FROM pg_views
--    WHERE definition ILIKE '%etiquetas_llm%' OR definition ILIKE '%analisis_semanal%';
--   SELECT * FROM information_schema.triggers
--    WHERE event_object_table IN ('etiquetas_llm','analisis_semanal');
--   SELECT version();   -- debe ser >= 11

BEGIN;
SET LOCAL lock_timeout = '5s';

ALTER TABLE processed.etiquetas_llm
    ADD COLUMN IF NOT EXISTS llm_criterio TEXT DEFAULT 'v1';

ALTER TABLE processed.analisis_semanal
    ADD COLUMN IF NOT EXISTS llm_criterio TEXT DEFAULT 'v1';

COMMENT ON COLUMN processed.etiquetas_llm.llm_criterio IS
    'Criterio/prompt de etiquetado: v1 | v22bcrit. Se decide por la semana de PUBLICACION del mensaje (created_at). llm_version no cambia.';
COMMENT ON COLUMN processed.analisis_semanal.llm_criterio IS
    'Criterio de etiquetado de la semana: v1 | v22bcrit. Desde la semana de corte los umbrales no son comparables con los de v1.';

COMMIT;

-- ── VERIFICACIÓN (solo lectura) ──────────────────────────────────────────────
--   SELECT column_name, data_type, column_default
--     FROM information_schema.columns
--    WHERE table_schema='processed' AND column_name='llm_criterio';      -- 2 filas
--   SELECT llm_criterio, COUNT(*) FROM processed.etiquetas_llm GROUP BY 1; -- todo 'v1'
--   SELECT llm_criterio, COUNT(*) FROM processed.analisis_semanal GROUP BY 1; -- todo 'v1'
