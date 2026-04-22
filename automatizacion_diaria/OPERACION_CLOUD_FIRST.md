# ReTo - Operación Cloud-First de Automatización

## Fuente única oficial

Desde esta versión, la **única fuente oficial de automatización diaria** del pipeline ReTo es:

- Workflow: `.github/workflows/daily.yml`
- Repositorio: `ayankilevich-cpu/reto-pipeline`
- Rama: `main`
- Scheduler: GitHub Actions (`cron` + `workflow_dispatch`)

## Política operativa

- `GitHub Actions` = producción automática diaria.
- `launchd` / `cron` local = **solo fallback manual** (contingencia), no operación normal.
- No se deben mantener schedulers duplicados activos en la Mac para evitar ejecuciones parciales o conflictivas.

## Inventario de launchd legacy detectado (Mac local)

Labels detectados fuera del esquema cloud-first:

- `com.reto.pipeline_x`
- `com.reto.pipeline_youtube`
- `com.reto.tag_youtube_hate_auto`
- `com.reto.youtube_extract_hate`
- `com.retoscraper.youtube_extract`
- `com.retoscraper.youtube`

## Plan de desactivación legacy (operación normal)

1. Detener y descargar jobs legacy (opción recomendada: script):

```bash
bash Clases/RETO/automatizacion_diaria/disable_legacy_launchd.sh
```

Alternativa manual:

```bash
for label in \
  com.reto.pipeline_x \
  com.reto.pipeline_youtube \
  com.reto.tag_youtube_hate_auto \
  com.reto.youtube_extract_hate \
  com.retoscraper.youtube_extract \
  com.retoscraper.youtube
do
  launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
done
```

2. Verificar que no quedan activos:

```bash
launchctl list | grep -E "reto|youtube|pipeline" || true
```

3. Mantener los `.plist` solo como referencia de fallback manual, no cargados por defecto.

## Fallback manual documentado

Si GitHub Actions no está disponible temporalmente, ejecutar manualmente desde la raíz del proyecto:

```bash
python3 Clases/RETO/automatizacion_diaria/run_pipeline_diario.py
python3 Clases/RETO/automatizacion_diaria/run_pipeline_youtube.py
```

Notas:

- Este fallback es **manual y excepcional**.
- Al finalizar la contingencia, volver a operación cloud-first.

## Observabilidad mínima obligatoria

El workflow diario registra healthcheck en `processed.pipeline_health` por plataforma:

- última ingesta (`last_ingested_at`)
- horas desde última ingesta
- filas nuevas en ventana de 24h (`rows_new_window`)
- estancamiento (`stagnated`)
- estado de etapas críticas
- warnings/errors operativos

Reglas mínimas actuales:

- X: alerta/fallo si >24h sin ingesta nueva.
- YouTube: alerta/fallo si >48h sin ingesta nueva.
- alerta/fallo si etapas críticas no terminan en `success`.
- alerta/fallo si `rows_new_window=0` en corridas consecutivas por plataforma.
