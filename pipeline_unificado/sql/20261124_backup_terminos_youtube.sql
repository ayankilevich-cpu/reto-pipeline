-- ============================================================
-- Backup de columnas de términos de YouTube antes del recálculo
-- Ejecutar en Neon ANTES de correr recalcular_terminos_youtube.py --apply
--
-- Nota: en processed.mensajes no existe columna "hate_types"; los tipos de
-- odio se guardan en "candidate_reason" (mapeo establecido en load_to_db.py).
-- ============================================================

-- Sustituir <FECHA_CORTE> por la fecha que reporta el script en el Paso 1
-- (ej. '2026-07-01'). Es el valor de min(created_at) WHERE matched_terms LIKE '%|%'.

CREATE TABLE IF NOT EXISTS processed.bak_mensajes_yt_terminos_20261124 AS
SELECT
    message_uuid,
    matched_terms,
    match_count,
    has_hate_terms_match,
    strong_phrase,
    is_candidate,
    candidate_reason
FROM processed.mensajes
WHERE platform = 'youtube'
  AND created_at < '<FECHA_CORTE>';

-- Verificar el recuento (debe coincidir con los mensajes a recalcular):
SELECT count(*) FROM processed.bak_mensajes_yt_terminos_20261124;
