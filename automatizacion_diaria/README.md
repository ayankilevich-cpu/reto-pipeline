# Pipeline diario RETO (fallback local)

> Operación oficial: revisar `OPERACION_CLOUD_FIRST.md`.
> La automatización automática oficial es GitHub Actions en `reto-pipeline/main`.
> Este README describe el modo local como contingencia/fallback manual.

Script maestro que ejecuta cada día, en orden, los 7 scripts del pipeline.

## Orden de ejecución

1. `X_Mensajes/sync_drive_csvs.py`
2. `X_Mensajes/consolidar_csv.py`
3. `X_Mensajes/Anon/filter_and_anonymize_x.py`
4. `Medios/X_terms_sheet.py`
5. `Etiquetado_Modelos/score_baseline.py`
6. `Etiquetado_Modelos/scored_prioridad_alta.py`
7. `Medios/ML/etiquetado_llm/etiquetar_completo_llm.py`
8. `automatizacion_diaria/load_to_db.py`

**Solo los lunes** (calendario local del equipo que ejecuta el script: `date.today().weekday() == 0`), después de los pasos anteriores:

9. `automatizacion_diaria/analisis_contexto_semanal.py` — cierre semanal, umbrales congelados y resumen contextual (LLM) en `processed.analisis_semanal`.

Si un script falla, se registra el error en el log y se continúa con el siguiente.

## Uso manual

Desde la raíz del proyecto (MASTER DATA SCIENCE):

```bash
python3 Clases/RETO/automatizacion_diaria/run_pipeline_diario.py
```

Si usas un entorno virtual (venv/conda), actívalo antes o indica el Python:

```bash
PYTHON_BIN=/ruta/a/tu/venv/bin/python3 python3 Clases/RETO/automatizacion_diaria/run_pipeline_diario.py
```

## Logs

- Por día: `logs/pipeline_YYYY-MM-DD.log`
- Salida estándar de cron (si configuras redirección): `logs/cron_stdout.log`

## Cron — fallback local (no operación normal)

El equipo debe estar encendido a esa hora. Editar crontab:

```bash
crontab -e
```

**Linux / WSL / servidor en UTC:** una línea `CRON_TZ=Europe/Madrid` al inicio hace que `0 10` sea siempre **10:00 en Madrid** (CET/CEST según calendario):

```cron
CRON_TZ=Europe/Madrid
0 10 * * * cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE" && PYTHON_BIN=/ruta/a/tu/venv/bin/python3 "Clases/RETO/automatizacion_diaria/run_pipeline_diario.py" >> "Clases/RETO/automatizacion_diaria/logs/cron_stdout.log" 2>&1
```

**macOS:** el `cron` del sistema usa la zona horaria del reloj del Mac. Configurá **Europa/Madrid** en Ajustes y usá la misma línea de tarea con `0 10 * * *` (si `CRON_TZ` no surte efecto, omití esa línea y confiá en la zona del sistema).

Si no usás venv, quitá `PYTHON_BIN=... ` y dejá que use el `python3` del PATH de cron.

Para ver la ruta del Python con dependencias instaladas (desde una terminal donde ya actives tu entorno):

```bash
which python3
```

Usa esa ruta en `PYTHON_BIN`.

## Análisis semanal sin pipeline diario (solo lunes)

Si no querés pasar por todo el pipeline pero sí cerrar el análisis contextual cada lunes (p. ej. porque los datos ya están en la BD), podés añadir una segunda línea en crontab:

```cron
CRON_TZ=Europe/Madrid
0 10 * * 1 cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE" && PYTHON_BIN=/ruta/a/tu/venv/bin/python3 "Clases/RETO/automatizacion_diaria/analisis_contexto_semanal.py" >> "Clases/RETO/automatizacion_diaria/logs/cron_analisis_semanal.log" 2>&1
```

El último campo (`1`) es **solo lunes**; `0 10` = **10:00 hora España** con `CRON_TZ` (o reloj del Mac en Madrid). Si ya tenés `CRON_TZ=Europe/Madrid` arriba para el pipeline diario, no hace falta repetirla antes de esta línea. Necesitás `OPENAI_API_KEY` (y `.env` donde ya lo tengas configurado).

## GitHub Actions (fuente única oficial)

El workflow `.github/workflows/daily.yml` es la fuente oficial automática.
Está programado en UTC (`0 8 * * *` ≈ 10:00 en España durante CEST; en CET será ~09:00).
Incluye validación de etapas críticas y healthcheck por plataforma persistido en BD.
