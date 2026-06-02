#!/usr/bin/env python3
"""
run_pipeline_completo.py — Orquestador unificado del pipeline diario RETO.

Reemplaza run_pipeline_diario.py + run_pipeline_youtube.py como punto de entrada único.

Orden de ejecución:
  1. youtube_extract_hate.py      — Extracción de comentarios YouTube
  2. tag_youtube_hate_auto.py     — Etiquetado automático YouTube
  3. sync_drive_csvs.py           — Sync CSVs desde Google Drive (X)
  4. consolidar_csv.py            — Consolidación CSV maestro X
  5. filter_and_anonymize_x.py    — Filtrado y anonimización X
  6. X_terms_sheet.py             — Términos y medios X
  7. score_baseline.py            — Scoring baseline
  8. scored_prioridad_alta.py     — Scoring prioridad alta
  9. etiquetar_completo_llm.py    — Etiquetado LLM completo
 10. load_to_db.py                — Carga a PostgreSQL (UNA SOLA VEZ, con datos X + YouTube)
 11. analisis_contexto_semanal.py — Solo lunes: análisis semanal

Garantías:
  - Un único lockfile: impide ejecuciones simultáneas.
  - Catch-up (--catch-up): si ya corrió hoy, no vuelve a correr.
  - load_to_db.py corre exactamente una vez por día.
  - Logs por fecha en automatizacion_diaria/logs/pipeline_completo_YYYY-MM-DD.log

Uso:
  python run_pipeline_completo.py                   # ejecución normal
  python run_pipeline_completo.py --catch-up        # solo si no corrió hoy (usar en launchd)
  python run_pipeline_completo.py --force           # ignora lockfile (admin)
  python run_pipeline_completo.py --skip-youtube    # solo pipeline X + carga
  python run_pipeline_completo.py --skip-x          # solo pipeline YouTube + carga
  PYTHON_BIN=/ruta/venv/bin/python3 python run_pipeline_completo.py

Variables de entorno:
  PYTHON_BIN   — intérprete Python a usar (default: sys.executable)
"""

from __future__ import annotations

import argparse
import errno
import json
import logging
import os
import socket
import subprocess
import sys
import time
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional

# ── Rutas base ──────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT   = SCRIPT_DIR.parent.parent.parent  # .../MASTER DATA SCIENCE
LOGS_DIR    = SCRIPT_DIR / "logs"
LOCK_FILE   = LOGS_DIR / ".pipeline_completo.lock"
LAST_RUN    = LOGS_DIR / "last_run_completo.json"

# ── Secuencia de scripts ─────────────────────────────────────────────────────
SCRIPTS_YOUTUBE: List[str] = [
    "Clases/RETO/Medios/youtube_extract_hate.py",
    "Clases/RETO/Medios/tag_youtube_hate_auto.py",
]

SCRIPTS_X: List[str] = [
    "Clases/RETO/X_Mensajes/sync_drive_csvs.py",
    "Clases/RETO/X_Mensajes/consolidar_csv.py",
    "Clases/RETO/X_Mensajes/Anon/filter_and_anonymize_x.py",
    "Clases/RETO/Medios/X_terms_sheet.py",
    "Clases/RETO/Etiquetado_Modelos/score_baseline.py",
    "Clases/RETO/Etiquetado_Modelos/scored_prioridad_alta.py",
    "Clases/RETO/Medios/ML/etiquetado_llm/etiquetar_completo_llm.py",
]

# load_to_db siempre al final, una sola vez con datos de ambas fuentes
SCRIPTS_CARGA: List[str] = [
    "Clases/RETO/automatizacion_diaria/load_to_db.py",
]

SCRIPTS_LUNES: List[str] = [
    "Clases/RETO/automatizacion_diaria/analisis_contexto_semanal.py",
]


# ── Logging ──────────────────────────────────────────────────────────────────
def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"pipeline_completo_{date_str}.log"

    logger = logging.getLogger("pipeline_completo")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ── Estado local ─────────────────────────────────────────────────────────────
def load_state() -> dict:
    if not LAST_RUN.exists():
        return {}
    try:
        return json.loads(LAST_RUN.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_state(state: dict) -> None:
    LAST_RUN.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def already_ran_today(state: dict) -> bool:
    last = state.get("last_success_date") or state.get("last_started_date")
    if not last:
        return False
    return last == date.today().isoformat()


# ── Lockfile ─────────────────────────────────────────────────────────────────
class LockError(RuntimeError):
    pass


def acquire_lock(force: bool = False) -> Optional[int]:
    """Adquiere el lockfile. Devuelve el fd o None si force=True con lock activo."""
    import fcntl
    LOCK_FILE.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(LOCK_FILE), os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as e:
        if e.errno in (errno.EACCES, errno.EAGAIN):
            os.close(fd)
            if force:
                return None
            raise LockError(
                f"Otra corrida está en curso (lock: {LOCK_FILE}). "
                "Usar --force para ignorar."
            ) from e
        raise
    os.ftruncate(fd, 0)
    info = f"pid={os.getpid()} host={socket.gethostname()} started={datetime.now().isoformat()}\n"
    os.write(fd, info.encode())
    return fd


def release_lock(fd: Optional[int]) -> None:
    if fd is None:
        return
    import fcntl
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except Exception:
        pass
    try:
        os.close(fd)
    except Exception:
        pass


# ── Ejecución de scripts ──────────────────────────────────────────────────────
def get_python_bin() -> str:
    return os.environ.get("PYTHON_BIN", sys.executable)


def run_script(rel_path: str, logger: logging.Logger) -> bool:
    script = REPO_ROOT / rel_path
    if not script.is_file():
        logger.error("Script no encontrado: %s", script)
        return False

    python_bin = get_python_bin()
    logger.info("→ Inicio: %s", script.name)
    start = time.time()
    try:
        result = subprocess.run(
            [python_bin, str(script)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=7200,  # 2h por script
        )
        elapsed = time.time() - start
        if result.returncode != 0:
            logger.error(
                "✗ Falló: %s (exit=%s, %.0fs)",
                script.name, result.returncode, elapsed,
            )
            if result.stderr:
                logger.error("stderr: %s", result.stderr.strip()[-3000:])
            if result.stdout:
                logger.debug("stdout: %s", result.stdout.strip()[-2000:])
            return False
        logger.info("✓ OK: %s (%.0fs)", script.name, elapsed)
        return True
    except subprocess.TimeoutExpired:
        logger.error("✗ Timeout (2h): %s", script.name)
        return False
    except Exception as exc:
        logger.exception("✗ Excepción en %s: %s", script.name, exc)
        return False


def run_group(
    scripts: List[str],
    group_name: str,
    logger: logging.Logger,
) -> tuple[int, int]:
    """Ejecuta un grupo de scripts. Devuelve (ok, fail)."""
    ok = fail = 0
    logger.info("── %s ──", group_name)
    for rel in scripts:
        if run_script(rel, logger):
            ok += 1
        else:
            fail += 1
            logger.info("  (continuando con el siguiente)")
    return ok, fail


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> int:
    parser = argparse.ArgumentParser(description="Orquestador unificado pipeline RETO")
    parser.add_argument("--catch-up", action="store_true",
                        help="Solo ejecutar si no corrió hoy.")
    parser.add_argument("--force", action="store_true",
                        help="Ignorar lockfile (admin).")
    parser.add_argument("--skip-youtube", action="store_true",
                        help="Omitir pasos de YouTube (solo pipeline X + carga).")
    parser.add_argument("--skip-x", action="store_true",
                        help="Omitir pasos de X (solo pipeline YouTube + carga).")
    args = parser.parse_args()

    logger = setup_logging()
    logger.info(
        "=== Pipeline completo RETO === host=%s python=%s",
        socket.gethostname(), sys.version.split()[0],
    )

    # Catch-up: si ya corrió hoy, no hacer nada
    state = load_state()
    if args.catch_up and already_ran_today(state):
        logger.info(
            "catch-up: pipeline ya ejecutado hoy (%s) — nada que hacer.",
            state.get("last_started_date"),
        )
        return 0

    # Adquirir lock
    lock_fd: Optional[int] = None
    try:
        lock_fd = acquire_lock(force=args.force)
    except LockError as e:
        logger.warning("%s — abortando.", e)
        return 0

    started_at = datetime.now()
    state.update({
        "last_started_at": started_at.isoformat(),
        "last_started_date": started_at.date().isoformat(),
    })
    save_state(state)

    total_ok = total_fail = 0

    try:
        # ── 1. YouTube ────────────────────────────────────────────────────────
        if not args.skip_youtube:
            ok, fail = run_group(SCRIPTS_YOUTUBE, "YouTube: extracción + etiquetado", logger)
            total_ok += ok
            total_fail += fail
        else:
            logger.info("── YouTube: omitido (--skip-youtube) ──")

        # ── 2. X (Twitter/X) ─────────────────────────────────────────────────
        if not args.skip_x:
            ok, fail = run_group(SCRIPTS_X, "X: sync → consolidar → anon → scoring → LLM", logger)
            total_ok += ok
            total_fail += fail
        else:
            logger.info("── X pipeline: omitido (--skip-x) ──")

        # ── 3. Carga a BD (una sola vez) ──────────────────────────────────────
        ok, fail = run_group(SCRIPTS_CARGA, "Carga a PostgreSQL (única)", logger)
        total_ok += ok
        total_fail += fail

        # ── 4. Análisis semanal (solo lunes) ─────────────────────────────────
        if date.today().weekday() == 0:  # 0 = lunes
            logger.info("Hoy es lunes — ejecutando análisis semanal")
            ok, fail = run_group(SCRIPTS_LUNES, "Análisis semanal (lunes)", logger)
            total_ok += ok
            total_fail += fail
        else:
            logger.info("── Análisis semanal: omitido (no es lunes) ──")

        finished_at = datetime.now()
        elapsed_total = (finished_at - started_at).total_seconds()

        status = "ok" if total_fail == 0 else ("partial" if total_ok > 0 else "error")
        logger.info(
            "=== Fin pipeline completo === status=%s OK=%d Fallos=%d elapsed=%.0fs",
            status, total_ok, total_fail, elapsed_total,
        )

        state.update({
            "last_finished_at": finished_at.isoformat(),
            "last_status": status,
            "ok_count": total_ok,
            "fail_count": total_fail,
            "elapsed_s": round(elapsed_total),
        })
        if status in ("ok", "partial"):
            state["last_success_date"] = finished_at.date().isoformat()
            state["last_success_at"] = finished_at.isoformat()
        save_state(state)

        return 0 if status != "error" else 1

    finally:
        release_lock(lock_fd)


if __name__ == "__main__":
    sys.exit(main())
