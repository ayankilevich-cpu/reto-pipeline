# Pipeline diario RETO

Script maestro que ejecuta cada día, en orden, los 7 scripts del pipeline.

## Orden de ejecución

1. `X_Mensajes/sync_drive_csvs.py`
2. `X_Mensajes/consolidar_csv.py`
3. `X_Mensajes/Anon/filter_and_anonymize_x.py`
4. `Medios/X_terms_sheet.py`
5. `Etiquetado_Modelos/score_baseline.py`
6. `Etiquetado_Modelos/scored_prioridad_alta.py`
7. `Medios/ML/etiquetado_llm/etiquetar_completo_llm.py`

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

## Cron (10:00 AM diario)

El Mac debe estar encendido a esa hora. Editar crontab:

```bash
crontab -e
```

Añadir una línea como esta (ajustar la ruta a tu Python si usas venv):

```cron
0 10 * * * cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE" && PYTHON_BIN=/ruta/a/tu/venv/bin/python3 "Clases/RETO/automatizacion_diaria/run_pipeline_diario.py" >> "Clases/RETO/automatizacion_diaria/logs/cron_stdout.log" 2>&1
```

Si no usas venv, quita la parte `PYTHON_BIN=... ` y deja que use el `python3` del PATH de cron.

Para ver la ruta del Python con dependencias instaladas (desde una terminal donde ya actives tu entorno):

```bash
which python3
```

Usa esa ruta en `PYTHON_BIN`.
