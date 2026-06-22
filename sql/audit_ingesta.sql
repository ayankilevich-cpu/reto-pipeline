-- =============================================================
-- audit_ingesta.sql
-- Diagnóstico de volumen de mensajes cargados al sistema (ReTo)
-- Campo de referencia: ingested_at (cuándo entró al sistema)
-- NO usar created_at (fecha del mensaje en la red social)
-- NO usar processed_at para "cargados" (puede cambiar en upserts)
--
-- ERROR: permission denied for schema raw
--   Tu usuario (p. ej. analista_01) solo tiene processed + delitos.
--   Opción A: Neon SQL Editor como neondb_owner → ejecutar sql/grant_raw_read.sql
--   Opción B: Conectar con el usuario del pipeline (.env DB_USER / owner)
--   Opción C: Usar la sección "PROCESSED" al final de este archivo (mismas métricas)
--
-- Notas ReTo (2026-06):
--   - platform en BD: 'x' | 'youtube' (usar IN ('x','twitter') por compatibilidad)
--   - batch_id en raw suele NULL; ver query 4b
-- =============================================================

-- Helper mental para filtros de plataforma X:
--   platform IN ('x', 'twitter')


-- -------------------------------------------------------------
-- 1. RESUMEN POR PERIODO — descomenta UN solo filtro en WHERE
-- -------------------------------------------------------------

SELECT
    platform,
    source_media,
    COUNT(*) AS mensajes_cargados
FROM raw.mensajes
WHERE
    -- ingested_at::date = CURRENT_DATE                    -- Hoy
    ingested_at::date = CURRENT_DATE - 1                   -- Ayer
    -- ingested_at::date = CURRENT_DATE - 2                -- Hace 2 días
    -- ingested_at >= CURRENT_DATE - INTERVAL '7 days'     -- Últimos 7 días
    -- ingested_at >= CURRENT_DATE - INTERVAL '30 days'    -- Últimos 30 días
GROUP BY platform, source_media
ORDER BY platform, mensajes_cargados DESC;


-- -------------------------------------------------------------
-- 2. SERIE DIARIA — últimos 14 días (detectar caídas de volumen)
-- -------------------------------------------------------------

SELECT
    ingested_at::date AS fecha_carga,
    platform,
    COUNT(*)          AS mensajes_cargados
FROM raw.mensajes
WHERE ingested_at >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY ingested_at::date, platform
ORDER BY fecha_carga DESC, platform;


-- -------------------------------------------------------------
-- 3. COMPARACIÓN DOS PERIODOS — ayer vs anteayer
-- -------------------------------------------------------------

SELECT
    periodo,
    platform,
    COUNT(*) AS mensajes_cargados
FROM (
    SELECT
        platform,
        CASE
            WHEN ingested_at::date = CURRENT_DATE - 1 THEN 'ayer'
            WHEN ingested_at::date = CURRENT_DATE - 2 THEN 'anteayer'
        END AS periodo
    FROM raw.mensajes
    WHERE ingested_at::date IN (CURRENT_DATE - 1, CURRENT_DATE - 2)
) sub
WHERE periodo IS NOT NULL
GROUP BY periodo, platform
ORDER BY periodo, platform;


-- -------------------------------------------------------------
-- 3b. COMPARACIÓN PIVOT (misma info, más legible)
-- -------------------------------------------------------------

SELECT
    platform,
    COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 1) AS ayer,
    COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 2) AS anteayer,
    COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 1)
    - COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 2) AS delta
FROM raw.mensajes
WHERE ingested_at::date IN (CURRENT_DATE - 1, CURRENT_DATE - 2)
GROUP BY platform
ORDER BY platform;


-- -------------------------------------------------------------
-- 4. DESGLOSE X POR DÍA DE CARGA (volumen operativo)
--    batch_id: usar solo si en tu BD tiene datos; si no, ignora 4b
-- -------------------------------------------------------------

SELECT
    ingested_at::date AS fecha_carga,
    COUNT(*)          AS mensajes_cargados_x
FROM raw.mensajes
WHERE
    platform IN ('x', 'twitter')
    AND ingested_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY ingested_at::date
ORDER BY fecha_carga DESC;


-- 4b. Por batch_id (Batch 2A / 2B) — válido cuando batch_id no sea NULL
SELECT
    ingested_at::date AS fecha_carga,
    COALESCE(NULLIF(TRIM(batch_id), ''), '(sin batch_id)') AS batch_id,
    COUNT(*) AS mensajes_cargados
FROM raw.mensajes
WHERE
    platform IN ('x', 'twitter')
    AND ingested_at >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY ingested_at::date, COALESCE(NULLIF(TRIM(batch_id), ''), '(sin batch_id)')
ORDER BY fecha_carga DESC, mensajes_cargados DESC;


-- -------------------------------------------------------------
-- 5. TOTALES RÁPIDOS POR PLATAFORMA (sanity check)
-- -------------------------------------------------------------

SELECT
    platform,
    COUNT(*) AS total_acumulado,
    MIN(ingested_at) AS primera_ingesta,
    MAX(ingested_at) AS ultima_ingesta
FROM raw.mensajes
GROUP BY platform
ORDER BY total_acumulado DESC;


-- =============================================================
-- PROCESSED — si tenés "permission denied for schema raw"
-- Misma lógica con processed.mensajes (ingested_at existe ahí también)
-- Sin author_* ni batch_id; sin texto original sin anonimizar
-- =============================================================

-- P1. Resumen por periodo (ayer)
SELECT
    platform,
    source_media,
    COUNT(*) AS mensajes_cargados
FROM processed.mensajes
WHERE ingested_at::date = CURRENT_DATE - 1
GROUP BY platform, source_media
ORDER BY platform, mensajes_cargados DESC;

-- P2. Serie diaria 14 días
SELECT
    ingested_at::date AS fecha_carga,
    platform,
    COUNT(*)          AS mensajes_cargados
FROM processed.mensajes
WHERE ingested_at >= CURRENT_DATE - INTERVAL '14 days'
GROUP BY ingested_at::date, platform
ORDER BY fecha_carga DESC, platform;

-- P3. Ayer vs anteayer (pivot)
SELECT
    platform,
    COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 1) AS ayer,
    COUNT(*) FILTER (WHERE ingested_at::date = CURRENT_DATE - 2) AS anteayer
FROM processed.mensajes
WHERE ingested_at::date IN (CURRENT_DATE - 1, CURRENT_DATE - 2)
GROUP BY platform
ORDER BY platform;
