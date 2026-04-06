# Reto Scraper – Recolección de discursos de odio

## Objetivo
Capturar mensajes públicos en redes sociales que contengan términos definidos en el diccionario **HateMedia–ReTo v1.0** (7 122 lemas limpios) para alimentar un pipeline de análisis y etiquetado de discursos de odio, preservando la privacidad y cumpliendo el RGPD.

## Arquitectura resumida

1. **Planificación**: `jobs/generate_jobs.py` crea lotes por red/término y los agenda cada 15–30 min (cron o Airflow).
2. **Crawlers**: `crawlers/twitter.py`, `crawlers/youtube.py` (posteriormente Facebook/TikTok) ejecutan búsquedas, obtienen posts/comentarios y normalizan campos comunes con utilidades de `crawlers/common.py`.
3. **Procesamiento**:
   - `processors/cleaner.py`: normalización (lowercase, limpieza de URLs/hashtags/acentos).
   - `processors/matcher.py`: matching exacto, n-gramas y detección con contornos de palabra.
   - `processors/anonymizer.py`: anonimización inmediata de identificadores usando `app.anonym_salt`.
4. **Persistencia**: scripts en `db/` gestionan la creación del esquema, `UPSERT` idempotente y vistas (`v_message_anonymized`, `v_dashboard_summary`).
5. **Exportación**: `jobs/run_job.py` construye el flujo crawl→procesamiento→`PostgreSQL` y genera CSV anonimizado para etiquetado.
6. **Monitoreo**: `monitoring/metrics.py` calcula KPIs (coincidencias, falsos positivos, latencia, errores), `monitoring/healthcheck.py` entrega chequeos para alertas.

## Estructura de carpetas

```
reto-scraper/
├─ config/              # Configuración de redes, términos, secretos
├─ crawlers/            # Clientes específicos por red + utilidades comunes
├─ processors/          # Limpieza, matching, anonimización
├─ db/                  # Esquema, upserts, vistas
├─ jobs/                # Generación y ejecución de lotes
├─ monitoring/          # Métricas, health checks
└─ README.md
```

## Configuración inicial

1. Copiar `config/secrets_example.yaml` a `config/secrets.yaml` y completar credenciales/API tokens. Mantener `secrets.yaml` fuera del control de versiones.
2. Cargar el diccionario `hate_terms_clean.csv` en `config/terms.csv`.
3. Ajustar `config/networks.yaml` con límites de rate limit, ventana temporal y parámetros de búsqueda por red.
4. Definir `app.anonym_salt` en PostgreSQL:  
   ```sql
   ALTER SYSTEM SET app.anonym_salt = '<valor-aleatorio-32-bytes>';
   SELECT pg_reload_conf();
   ```

## Despliegue del esquema

```bash
psql -h <host> -U <usuario> -d <database> -f db/schema.sql
psql -h <host> -U <usuario> -d <database> -f db/views.sql
```

Los scripts crean tablas (`crawl_jobs`, `raw_messages`, `term_hits`, `labeled_data`) e índices necesarios para upsert, deduplicación y consultas analíticas. `db/upsert.sql` incluye funciones auxiliares para inserciones idempotentes.

## Ejecución de un job

1. Ejecutar `jobs/generate_jobs.py` para crear lotes según términos y ventanas pendientes.
2. Lanzar `jobs/run_job.py --job-id <id>` (o mediante scheduler) para orquestar crawling, procesamiento y almacenamiento.
3. Revisar métricas con `monitoring/metrics.py` y exportar CSV anonimizado desde la vista `v_message_anonymized`.

## Próximos pasos (piloto)

- Semana 1: desplegar PostgreSQL y ejecutar `db/schema.sql`.
- Semana 2: implementar `crawlers/twitter.py` + `processors/matcher.py`.
- Semana 3: habilitar vista anonimizada y exportaciones.
- Semana 4: armar dashboard en Looker Studio usando `v_dashboard_summary`.

Documentar resultados y decisiones en este README para mantener sincronizado el estado del piloto.

