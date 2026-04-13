#!/usr/bin/env python3
"""
Script maestro del pipeline diario RETO.

Ejecuta en orden los 7 scripts configurados. Ante fallo de uno, registra el error
y continúa con el siguiente (opción B). Todo se registra en logs locales.

Uso:
  python run_pipeline_diario.py

  Para usar otro intérprete (p. ej. venv/conda):
  PYTHON_BIN=/ruta/al/venv/bin/python3 python run_pipeline_diario.py

Cron — **10:00 hora de España (península)**. Usar el mismo Python que tenga las dependencias:
  En Linux / servidor en UTC, al inicio del crontab (interpreta la hora en Madrid):
    CRON_TZ=Europe/Madrid
    0 10 * * * cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE" && PYTHON_BIN=/ruta/a/tu/venv/bin/python3 "Clases/RETO/automatizacion_diaria/run_pipeline_diario.py" >> "Clases/RETO/automatizacion_diaria/logs/cron_stdout.log" 2>&1
  En macOS, cron suele usar la zona del sistema: configurá el Mac en **España** y usá la misma línea `0 10 * * *` (sin CRON_TZ si no la reconoce).
  (Si no usas venv, quita PYTHON_BIN= y deja que use el python3 del PATH.)

Los LUNES, tras los pasos diarios, se ejecuta automáticamente analisis_contexto_semanal.py
(cierre de la semana anterior / fila de la semana en curso, umbrales y resumen LLM).
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime, date
from pathlib import Path

# Directorio de este script -> Clases/RETO/automatizacion_diaria
SCRIPT_DIR = Path(__file__).resolve().parent
# Raíz del proyecto (MASTER DATA SCIENCE)
REPO_ROOT = SCRIPT_DIR.parent.parent.parent

# Scripts a ejecutar en orden (rutas relativas a REPO_ROOT)
SCRIPTS = [
    "Clases/RETO/X_Mensajes/sync_drive_csvs.py",
    "Clases/RETO/X_Mensajes/consolidar_csv.py",
    "Clases/RETO/X_Mensajes/Anon/filter_and_anonymize_x.py",
    "Clases/RETO/Medios/X_terms_sheet.py",
    "Clases/RETO/Etiquetado_Modelos/score_baseline.py",
    "Clases/RETO/Etiquetado_Modelos/scored_prioridad_alta.py",
    "Clases/RETO/Medios/ML/etiquetado_llm/etiquetar_completo_llm.py",
    # Paso 8: cargar resultados a PostgreSQL
    "Clases/RETO/automatizacion_diaria/load_to_db.py",
]

# Scripts que se ejecutan solo los lunes (tras el pipeline diario)
WEEKLY_MONDAY_SCRIPTS = [
    "Clases/RETO/automatizacion_diaria/analisis_contexto_semanal.py",
]


def setup_logging(log_dir: Path) -> logging.Logger:
    """Configura logging a archivo y consola; devuelve el logger."""
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_{date_str}.log"

    logger = logging.getLogger("pipeline_diario")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def get_python_bin() -> str:
    """Python con el que ejecutar los sub-scripts (venv/conda o el actual)."""
    return os.environ.get("PYTHON_BIN", sys.executable)


def run_script(script_path: Path, cwd: Path, logger: logging.Logger) -> bool:
    """
    Ejecuta un script Python con el interpreter configurado. Devuelve True si salió con 0.
    """
    python_bin = get_python_bin()
    logger.info("Inicio: %s", script_path.name)
    try:
        result = subprocess.run(
            [python_bin, str(script_path)],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=3600,
        )
        if result.returncode != 0:
            logger.error("Falló: %s (exit code %s)", script_path.name, result.returncode)
            if result.stdout:
                logger.debug("stdout: %s", result.stdout.strip())
            if result.stderr:
                logger.error("stderr: %s", result.stderr.strip())
            return False
        logger.info("OK: %s", script_path.name)
        return True
    except subprocess.TimeoutExpired:
        logger.error("Timeout (1h): %s", script_path.name)
        return False
    except Exception as e:
        logger.exception("Excepción ejecutando %s: %s", script_path.name, e)
        return False


def main() -> int:
    log_dir = SCRIPT_DIR / "logs"
    logger = setup_logging(log_dir)

    logger.info("=== Pipeline diario RETO === Repo root: %s", REPO_ROOT)

    ok_count = 0
    fail_count = 0

    for rel in SCRIPTS:
        script_path = REPO_ROOT / rel
        if not script_path.is_file():
            logger.error("No existe el script: %s", script_path)
            fail_count += 1
            continue
        if run_script(script_path, REPO_ROOT, logger):
            ok_count += 1
        else:
            fail_count += 1
            # Opción B: continuar con el siguiente
            logger.info("Continuando con el siguiente script.")

    # --- Tareas semanales (solo lunes) ---
    if date.today().weekday() == 0:  # 0 = lunes
        logger.info("Hoy es lunes — ejecutando scripts semanales")
        for rel in WEEKLY_MONDAY_SCRIPTS:
            script_path = REPO_ROOT / rel
            if not script_path.is_file():
                logger.error("No existe el script semanal: %s", script_path)
                fail_count += 1
                continue
            if run_script(script_path, REPO_ROOT, logger):
                ok_count += 1
            else:
                fail_count += 1
                logger.info("Continuando con el siguiente script.")
    else:
        logger.info("No es lunes — scripts semanales omitidos")

    logger.info("=== Fin pipeline === OK: %s, Fallos: %s", ok_count, fail_count)
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
