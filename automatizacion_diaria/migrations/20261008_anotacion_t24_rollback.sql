-- Rollback de 20261008_anotacion_t24.sql
-- ATENCIÓN: borra las anotaciones T2.4 de su tabla propia. Las filas ya sumadas a
-- validaciones_manuales / gold_dataset (label_source = 'human_t24') NO se tocan.
BEGIN;
DROP TABLE IF EXISTS processed.anotacion_t24;
DROP TABLE IF EXISTS processed.muestra_t24;
COMMIT;
