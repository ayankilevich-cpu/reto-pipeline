-- ============================================================
-- Carga de la muestra T2.4 en processed.muestra_t24
-- Ejecutar UNA vez en la consola de Neon (reto_db, rama de producción),
-- después de aplicar la migración 20261008_anotacion_t24.sql.
-- Reproducible: el orden usa md5 con semilla fija ('T24'), siempre salen los mismos mensajes.
-- Idempotente: ON CONFLICT DO NOTHING (si se vuelve a correr no duplica).
-- ============================================================

-- ---------- BLOQUE A: prevalencia (400 X + 400 YouTube, proporcional por mes) ----------
INSERT INTO processed.muestra_t24 (message_uuid, bloque, platform, estrato_mes, doble_anotacion)
WITH base AS (
    SELECT DISTINCT ON (m.platform, md5(lower(m.content_original)))
           m.message_uuid,
           CASE WHEN m.platform IN ('x', 'twitter') THEN 'x' ELSE m.platform END AS platform,
           date_trunc('month', m.created_at)::date AS mes
    FROM processed.mensajes m
    WHERE m.created_at >= '2026-07-02'
      AND m.platform IN ('x', 'twitter', 'youtube')
      AND length(m.content_original) >= 15
    ORDER BY m.platform, md5(lower(m.content_original)), m.message_uuid
),
r AS (
    SELECT *,
           row_number() OVER (PARTITION BY platform, mes ORDER BY md5(message_uuid::text || 'T24')) AS rn,
           COUNT(*)     OVER (PARTITION BY platform, mes) AS n_mes,
           COUNT(*)     OVER (PARTITION BY platform)      AS n_plat
    FROM base
)
SELECT message_uuid, 'A', platform, mes,
       get_byte(decode(md5(message_uuid::text || 'T24-doble'), 'hex'), 0) < 51   -- ~20 % doble anotación
FROM r
WHERE rn <= CEIL(400.0 * n_mes / n_plat)
ON CONFLICT (message_uuid) DO NOTHING;

-- ---------- BLOQUE B: interseccional (300 X + todos los de YouTube) ----------
-- Mensajes ODIO (LLM) que mencionan >= 2 colectivos (mismos patrones que la consulta Q7 del inventario)
INSERT INTO processed.muestra_t24 (message_uuid, bloque, platform, estrato_mes, doble_anotacion)
WITH odio AS (
    SELECT m.message_uuid,
           CASE WHEN m.platform IN ('x', 'twitter') THEN 'x' ELSE m.platform END AS platform,
           date_trunc('month', m.created_at)::date AS mes,
           lower(m.content_original) AS t
    FROM processed.mensajes m
    JOIN processed.etiquetas_llm e USING (message_uuid)
    WHERE e.clasificacion_principal = 'ODIO'
),
flags AS (
    SELECT message_uuid, platform, mes,
        (t ~ '(inmigran|migrante|extranjero|sin papeles|\mmenas?\M)')::int
      + (t ~ '(\mmoros?\M|árabe|musulm|islam|marroqu|magreb)')::int
      + (t ~ '(latino|sudaca|panchito|venezolan|colombian|ecuatorian)')::int
      + (t ~ '(gitan|romaní)')::int
      + (t ~ '(\mnegr[oa]s?\M|african|subsahar)')::int
      + (t ~ '(\mmujer|feminista|feminazi|\mputas?\M|zorra)')::int
      + (t ~ '(homosexual|\mgays?\M|maric|lgtb|\mtrans\M|lesbiana|bollo)')::int
      + (t ~ '(pobre|mendig|indigente|okupa|paguita|chabol)')::int AS n_colectivos
    FROM odio
),
r AS (
    SELECT *, row_number() OVER (PARTITION BY platform ORDER BY md5(message_uuid::text || 'T24B')) AS rn
    FROM flags
    WHERE n_colectivos >= 2
)
SELECT message_uuid, 'B', platform, mes,
       get_byte(decode(md5(message_uuid::text || 'T24-doble'), 'hex'), 0) < 51
FROM r
WHERE (platform = 'x' AND rn <= 300) OR platform = 'youtube'
ON CONFLICT (message_uuid) DO NOTHING;

-- ---------- Verificación ----------
SELECT bloque, platform, COUNT(*) AS n, COUNT(*) FILTER (WHERE doble_anotacion) AS n_doble
FROM processed.muestra_t24
GROUP BY 1, 2
ORDER BY 1, 2;
