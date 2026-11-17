-- ============================================================
-- Rollback: restaura columnas de términos de YouTube desde el backup
-- Ejecutar en Neon SOLO si es necesario deshacer el recálculo
-- Requiere que 20261124_backup_terminos_youtube.sql se haya ejecutado antes
-- ============================================================

UPDATE processed.mensajes m
SET
    matched_terms        = b.matched_terms,
    match_count          = b.match_count,
    has_hate_terms_match = b.has_hate_terms_match,
    strong_phrase        = b.strong_phrase,
    is_candidate         = b.is_candidate,
    candidate_reason     = b.candidate_reason
FROM processed.bak_mensajes_yt_terminos_20261124 b
WHERE m.message_uuid = b.message_uuid;

-- Verificar que el recuento de filas restauradas es correcto:
SELECT count(*) AS filas_restauradas
FROM processed.mensajes m
JOIN processed.bak_mensajes_yt_terminos_20261124 b USING (message_uuid);
