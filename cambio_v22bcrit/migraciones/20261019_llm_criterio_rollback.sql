-- ============================================================================
-- ROLLBACK de 20261019_llm_criterio.sql   (PREPARADO, NO EJECUTADO)
-- ============================================================================
-- ORDEN: primero revertir el código (parches) y DESPUÉS ejecutar este archivo.
-- Si se borra la columna con el código nuevo aún desplegado, load_to_db.py,
-- etiquetar_completo_llm.py y analisis_contexto_semanal.py fallarán (INSERT con
-- una columna inexistente).
--
-- Antes de borrar la columna se guarda qué filas eran v22bcrit; si no, esa
-- información se perdería y esas etiquetas quedarían indistinguibles de las v1
-- (las semanas post-corte pasarían a mezclar criterios sin que se note).
-- Las tablas *_bak se pueden borrar a mano cuando ya no hagan falta.
-- ============================================================================

BEGIN;
SET LOCAL lock_timeout = '5s';

CREATE TABLE IF NOT EXISTS processed.etiquetas_llm_criterio_bak AS
    SELECT message_uuid, llm_version, llm_criterio
      FROM processed.etiquetas_llm
     WHERE llm_criterio IS DISTINCT FROM 'v1';

CREATE TABLE IF NOT EXISTS processed.analisis_semanal_criterio_bak AS
    SELECT semana_inicio, llm_criterio
      FROM processed.analisis_semanal
     WHERE llm_criterio IS DISTINCT FROM 'v1';

ALTER TABLE processed.etiquetas_llm   DROP COLUMN IF EXISTS llm_criterio;
ALTER TABLE processed.analisis_semanal DROP COLUMN IF EXISTS llm_criterio;

COMMIT;

-- ── VERIFICACIÓN (solo lectura) ──────────────────────────────────────────────
--   SELECT COUNT(*) FROM information_schema.columns
--    WHERE table_schema='processed' AND column_name='llm_criterio';      -- 0
--   SELECT COUNT(*) FROM processed.etiquetas_llm_criterio_bak;           -- filas v22bcrit guardadas
--
-- Restaurar la columna después (si se re-aplica la migración):
--   UPDATE processed.etiquetas_llm e SET llm_criterio = b.llm_criterio
--     FROM processed.etiquetas_llm_criterio_bak b
--    WHERE e.message_uuid = b.message_uuid AND e.llm_version = b.llm_version;
--   UPDATE processed.analisis_semanal a SET llm_criterio = b.llm_criterio
--     FROM processed.analisis_semanal_criterio_bak b
--    WHERE a.semana_inicio = b.semana_inicio;
