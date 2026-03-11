#!/usr/bin/env python3
"""
Script maestro del pipeline diario YouTube — RETO.

Ejecuta en orden los scripts de extracción, etiquetado, carga a BD
y filtrado de relevancia de YouTube. Ante fallo de uno, registra el error
y continúa con el siguiente.

Uso:
  python run_pipeline_youtube.py

  Para usar otro intérprete (p. ej. venv/conda):
  PYTHON_BIN=/ruta/al/venv/bin/python3 python run_pipeline_youtube.py

Cron (09:30 AM diario):
  30 9 * * * cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE" && PYTHON_BIN="/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/reto_ml/bin/python" /opt/homebrew/bin/python3 "Clases/RETO/automatizacion_diaria/run_pipeline_youtube.py" >> "Clases/RETO/automatizacion_diaria/logs/cron_stdout.log" 2>&1
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent

SCRIPTS = [
    "Clases/RETO/Medios/youtube_extract_hate.py",
    "Clases/RETO/Medios/tag_youtube_hate_auto.py",
    "Clases/RETO/automatizacion_diaria/load_to_db.py",
    "Clases/RETO/Medios/ML/etiquetado_llm/filtrar_relevancia_youtube.py",
]


def setup_logging(log_dir: Path) -> logging.Logger:
    log_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = log_dir / f"pipeline_youtube_{date_str}.log"

    logger = logging.getLogger("pipeline_youtube")
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
    return os.environ.get("PYTHON_BIN", sys.executable)


def run_script(script_path: Path, cwd: Path, logger: logging.Logger) -> bool:
    python_bin = get_python_bin()
    logger.info("Inicio: %s", script_path.name)
    try:
        result = subprocess.run(
            [python_bin, str(script_path)],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            timeout=7200,
        )
        if result.returncode != 0:
            logger.error("Falló: %s (exit code %s)", script_path.name, result.returncode)
            if result.stdout:
                logger.debug("stdout:\n%s", result.stdout.strip()[-2000:])
            if result.stderr:
                logger.error("stderr:\n%s", result.stderr.strip()[-2000:])
            return False
        logger.info("OK: %s", script_path.name)
        return True
    except subprocess.TimeoutExpired:
        logger.error("Timeout (2h): %s", script_path.name)
        return False
    except Exception as e:
        logger.exception("Excepción ejecutando %s: %s", script_path.name, e)
        return False


def main() -> int:
    log_dir = SCRIPT_DIR / "logs"
    logger = setup_logging(log_dir)

    logger.info("=== Pipeline diario YouTube === Repo root: %s", REPO_ROOT)

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
            logger.info("Continuando con el siguiente script.")

    logger.info("=== Fin pipeline YouTube === OK: %s, Fallos: %s", ok_count, fail_count)
    return 0 if fail_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
