-- ============================================================================
-- ROLLBACK de 20261019_llm_criterio.sql   (PREPARADO, NO EJECUTADO)
-- ============================================================================
-- ORDEN: primero revertir el código (parches) y DESPUÉS ejecutar este archivo.
-- Si se borra la columna con el código nuevo aún desplegado, load_to_db.py,
-- etiquetar_completo_llm.py y analisis_contexto_semanal.py fallarán (INSERT con
-- una columna inexistente).
--
-- Para RESTAURAR el criterio desde las bak tras re-aplicar la migración:
--   UPDATE processed.etiquetas_llm e SET llm_criterio = b.llm_criterio
--     FROM processed.etiquetas_llm_criterio_bak b WHERE e.message_uuid = b.message_uuid AND e.llm_version = b.llm_version;
--   UPDATE processed.analisis_semanal a SET llm_criterio = b.llm_criterio
--     FROM processed.analisis_semanal_criterio_bak b WHERE a.semana_inicio = b.semana_inicio;
-- ============================================================================

BEGIN;
SET LOCAL lock_timeout = '5s';

-- Abortar si la columna ya fue eliminada (evita ejecución doble silenciosa)
DO $guard$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'processed'
           AND table_name   = 'etiquetas_llm'
           AND column_name  = 'llm_criterio'
    ) THEN
        RAISE EXCEPTION 'ROLLBACK ABORTADO: llm_criterio no existe en etiquetas_llm. '
            'Migración ya revertida o nunca aplicada.';
    END IF;
END $guard$;

-- Crear tablas de respaldo con esquema explícito y clave primaria
-- (si ya existen de un rollback anterior, no se borran: se acumulan filas)
CREATE TABLE IF NOT EXISTS processed.etiquetas_llm_criterio_bak (
    message_uuid uuid NOT NULL,
    llm_version  text NOT NULL,
    llm_criterio text NOT NULL,
    PRIMARY KEY (message_uuid, llm_version)
);

CREATE TABLE IF NOT EXISTS processed.analisis_semanal_criterio_bak (
    semana_inicio date NOT NULL,
    llm_criterio  text NOT NULL,
    PRIMARY KEY (semana_inicio)
);

-- Guardar filas no-v1 (ON CONFLICT DO NOTHING evita duplicados en re-ejecuciones)
INSERT INTO processed.etiquetas_llm_criterio_bak (message_uuid, llm_version, llm_criterio)
    SELECT message_uuid, llm_version, llm_criterio
      FROM processed.etiquetas_llm
     WHERE llm_criterio IS DISTINCT FROM 'v1'
ON CONFLICT DO NOTHING;

INSERT INTO processed.analisis_semanal_criterio_bak (semana_inicio, llm_criterio)
    SELECT semana_inicio, llm_criterio
      FROM processed.analisis_semanal
     WHERE llm_criterio IS DISTINCT FROM 'v1'
ON CONFLICT DO NOTHING;

-- Verificar cobertura: ninguna fila no-v1 puede quedar sin respaldo
DO $check$ DECLARE
    n_faltantes_etiq BIGINT;
    n_faltantes_anal BIGINT;
BEGIN
    SELECT COUNT(*) INTO n_faltantes_etiq
      FROM processed.etiquetas_llm e
     WHERE e.llm_criterio IS DISTINCT FROM 'v1'
       AND NOT EXISTS (
           SELECT 1 FROM processed.etiquetas_llm_criterio_bak b
            WHERE b.message_uuid = e.message_uuid
              AND b.llm_version  = e.llm_version
       );

    SELECT COUNT(*) INTO n_faltantes_anal
      FROM processed.analisis_semanal a
     WHERE a.llm_criterio IS DISTINCT FROM 'v1'
       AND NOT EXISTS (
           SELECT 1 FROM processed.analisis_semanal_criterio_bak b
            WHERE b.semana_inicio = a.semana_inicio
       );

    IF n_faltantes_etiq > 0 OR n_faltantes_anal > 0 THEN
        RAISE EXCEPTION
            'ROLLBACK ABORTADO: % fila(s) de etiquetas_llm y % fila(s) de analisis_semanal '
            'no cubiertas en la bak. No se borra llm_criterio.',
            n_faltantes_etiq, n_faltantes_anal;
    END IF;
END $check$;

ALTER TABLE processed.etiquetas_llm    DROP COLUMN IF EXISTS llm_criterio;
ALTER TABLE processed.analisis_semanal DROP COLUMN IF EXISTS llm_criterio;

COMMIT;

-- ── VERIFICACIÓN (solo lectura) ──────────────────────────────────────────────
--   SELECT COUNT(*) FROM information_schema.columns
--    WHERE table_schema='processed' AND column_name='llm_criterio';      -- 0
--   SELECT COUNT(*) FROM processed.etiquetas_llm_criterio_bak;           -- filas v22bcrit guardadas
--   SELECT COUNT(*) FROM processed.analisis_semanal_criterio_bak;        -- filas v22bcrit guardadas
