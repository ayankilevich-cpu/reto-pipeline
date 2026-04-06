-- Umbral y promedio de referencia congelados al cerrar cada semana (analisis_contexto_semanal.py).
-- Ejecutar una vez en reto_db:
--   psql -h ... -U ... -d reto_db -f migrations/20260209_analisis_semanal_umbral_congelado.sql

ALTER TABLE processed.analisis_semanal
    ADD COLUMN IF NOT EXISTS promedio_referencia_pct NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS umbral_spike_pct NUMERIC(6,2),
    ADD COLUMN IF NOT EXISTS n_semanas_base INTEGER;

COMMENT ON COLUMN processed.analisis_semanal.promedio_referencia_pct IS
    'Promedio % odio de semanas estrictamente anteriores (≥100 msgs), al momento del cierre.';
COMMENT ON COLUMN processed.analisis_semanal.umbral_spike_pct IS
    '1,5 × promedio_referencia al cierre; no se recalcula en reruns del pipeline.';
COMMENT ON COLUMN processed.analisis_semanal.n_semanas_base IS
    'Cantidad de semanas incluidas en el promedio de referencia (0 = fallback).';
