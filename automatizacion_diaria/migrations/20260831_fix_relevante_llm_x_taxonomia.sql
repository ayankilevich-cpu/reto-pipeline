-- Migración: anular relevante_llm con taxonomía de odio en plataformas ≠ YouTube
-- Fecha: 2026-08-31
-- Origen del bug: script huérfano Medios/ML/etiquetado_llm/filtrar_relevancia_llm.py
--   (nunca integrado al pipeline oficial) escribió en processed.mensajes.relevante_llm
--   valores ODIO / NO_ODIO / DUDOSO en registros de X. Esa columna es solo para YouTube (SI/NO).
--
-- Verificación previa (2026-08-31, reto_db producción):
--   SELECT platform, relevante_llm, COUNT(*) ...
--   WHERE platform <> 'youtube' AND relevante_llm IN ('ODIO','NO_ODIO','DUDOSO')
--   → 0 filas (base ya limpia; UPDATE no aplicado en esta fecha).
--
-- Aplica UNA sola vez si la verificación previa devuelve filas (esperado histórico: 256):
--   psql $DATABASE_URL -f automatizacion_diaria/migrations/20260831_fix_relevante_llm_x_taxonomia.sql
--
-- Post-check (debe dar 0):
--   SELECT COUNT(*) FROM processed.mensajes
--   WHERE platform <> 'youtube' AND relevante_llm IN ('ODIO','NO_ODIO','DUDOSO');
--
-- NO toca registros con relevante_llm IN ('SI','NO') en X — decisión pendiente aparte.

UPDATE processed.mensajes
SET relevante_llm = NULL,
    relevante_score = NULL,
    relevante_motivo = NULL
WHERE platform <> 'youtube' AND relevante_llm IN ('ODIO','NO_ODIO','DUDOSO');
