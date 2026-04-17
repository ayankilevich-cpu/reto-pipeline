#!/usr/bin/env python3
"""
run_pipeline_wrapper.py — Orquestador operativo del pipeline diario RETO.

Responsabilidades (sin tocar el ETL interno):
  1) Impedir corridas simultáneas (lockfile).
  2) Soportar "catch-up": si la Mac estuvo apagada a la hora programada,
     al encender se ejecuta el pipeline si aún no se corrió hoy.
  3) Detectar si hubo cambios comparando hash+size+mtime de outputs clave
     ANTES y DESPUÉS de ejecutar run_pipeline_diario.py.
  4) Registrar la corrida en processed.pipeline_runs (aunque no haya cambios).
  5) Centralizar logs (stdout/stderr del pipeline + metadatos de corrida).

El pipeline interno (run_pipeline_diario.py, load_to_db.py, LLM, YouTube)
queda exactamente como está.

Uso:
  python run_pipeline_wrapper.py                 # ejecución normal (scheduled)
  python run_pipeline_wrapper.py --catch-up      # sólo si no corrió ya hoy
  python run_pipeline_wrapper.py --force         # ignora lock (admin)
  python run_pipeline_wrapper.py --triggered-by manual
"""

from __future__ import annotations

import argparse
import errno
import hashlib
import json
import logging
import os
import platform
import socket
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List, Optional

# --- Paths base ---
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent.parent.parent  # .../MASTER DATA SCIENCE
PIPELINE_SCRIPT = SCRIPT_DIR / "run_pipeline_diario.py"
LOGS_DIR = SCRIPT_DIR / "logs"
STATE_DIR = SCRIPT_DIR / "logs"
LOCK_FILE = STATE_DIR / ".pipeline.lock"
LAST_RUN_FILE = STATE_DIR / "last_run.json"

# Importar db_utils desde el mismo directorio (sin romper si falla la BD)
sys.path.insert(0, str(SCRIPT_DIR))
try:
    from db_utils import get_conn, upsert_rows  # noqa: F401
    DB_AVAILABLE = True
except Exception as _imp_err:  # pragma: no cover
    DB_AVAILABLE = False
    _DB_IMPORT_ERROR = str(_imp_err)


# Outputs cuyo cambio implica "changes_detected=True".
# Se usan hash+size+mtime para ser tolerantes a re-escrituras idénticas.
TRACKED_OUTPUTS: List[Path] = [
    REPO_ROOT / "Clases/RETO/X_Mensajes/data/master/reto_x_master.csv",
    REPO_ROOT / "Clases/RETO/X_Mensajes/Anon/reto_x_master_anon.csv",
    REPO_ROOT / "Clases/RETO/Etiquetado_Modelos/x_manual_label_scored.csv",
]


# ============================================================
# Utilidades
# ============================================================

def setup_logging() -> logging.Logger:
    LOGS_DIR.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    log_file = LOGS_DIR / f"wrapper_{date_str}.log"

    logger = logging.getLogger("pipeline_wrapper")
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


def file_fingerprint(path: Path) -> Dict[str, Optional[object]]:
    """Devuelve fingerprint (size, mtime, sha256) o {exists:False} si no existe."""
    if not path.exists() or not path.is_file():
        return {"exists": False, "size": None, "mtime": None, "sha256": None}

    st = path.stat()
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return {
        "exists": True,
        "size": st.st_size,
        "mtime": st.st_mtime,
        "sha256": h.hexdigest(),
    }


def snapshot_outputs(paths: List[Path]) -> Dict[str, Dict]:
    return {str(p): file_fingerprint(p) for p in paths}


def diff_snapshots(before: Dict[str, Dict], after: Dict[str, Dict]) -> List[str]:
    """Devuelve la lista de paths cuyo fingerprint cambió."""
    changed: List[str] = []
    for k, fp_before in before.items():
        fp_after = after.get(k, {})
        if fp_before.get("sha256") != fp_after.get("sha256"):
            changed.append(k)
            continue
        if fp_before.get("size") != fp_after.get("size"):
            changed.append(k)
    return changed


# ============================================================
# Lockfile (evita corridas concurrentes)
# ============================================================

class PipelineLockError(RuntimeError):
    pass


@dataclass
class PipelineLock:
    path: Path
    _fd: Optional[int] = field(default=None, init=False, repr=False)

    def acquire(self, force: bool = False) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_CREAT | os.O_RDWR
        try:
            self._fd = os.open(self.path, flags, 0o644)
        except OSError as e:
            raise PipelineLockError(f"No se pudo abrir lock: {e}") from e

        import fcntl

        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as e:
            if e.errno in (errno.EACCES, errno.EAGAIN):
                if force:
                    os.close(self._fd)
                    self._fd = None
                    return
                raise PipelineLockError(
                    f"Otra corrida del pipeline está en curso (lock: {self.path})."
                ) from e
            raise

        os.ftruncate(self._fd, 0)
        info = f"pid={os.getpid()} host={socket.gethostname()} started={datetime.now().isoformat()}\n"
        os.write(self._fd, info.encode())

    def release(self) -> None:
        if self._fd is None:
            return
        import fcntl

        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(self._fd)
        except Exception:
            pass
        self._fd = None


# ============================================================
# Estado local: last_run.json
# ============================================================

def load_last_run() -> Dict:
    if not LAST_RUN_FILE.exists():
        return {}
    try:
        return json.loads(LAST_RUN_FILE.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_last_run(payload: Dict) -> None:
    LAST_RUN_FILE.parent.mkdir(parents=True, exist_ok=True)
    LAST_RUN_FILE.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def already_ran_today(state: Dict) -> bool:
    last_ok = state.get("last_success_date") or state.get("last_started_date")
    if not last_ok:
        return False
    try:
        return last_ok == date.today().isoformat()
    except Exception:
        return False


# ============================================================
# Registro en BD (processed.pipeline_runs)
# ============================================================

def register_run_in_db(
    logger: logging.Logger,
    pipeline_name: str,
    started_at: datetime,
    finished_at: datetime,
    status: str,
    changes_detected: bool,
    ok_count: int,
    fail_count: int,
    triggered_by: str,
    detail: str,
) -> None:
    """Inserta una fila en processed.pipeline_runs. No rompe si falla."""
    if not DB_AVAILABLE:
        logger.warning(
            "db_utils no disponible (%s) — no se registra en processed.pipeline_runs",
            _DB_IMPORT_ERROR if 'DB_IMPORT_ERROR' in globals() else "?",
        )
        return

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO processed.pipeline_runs (
                    pipeline_name, started_at, finished_at,
                    status, changes_detected,
                    ok_count, fail_count, triggered_by,
                    detail, host
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    pipeline_name,
                    started_at,
                    finished_at,
                    status,
                    changes_detected,
                    ok_count,
                    fail_count,
                    triggered_by,
                    detail[:4000] if detail else None,
                    socket.gethostname()[:100],
                ),
            )
            cur.close()
        logger.info(
            "Corrida registrada en processed.pipeline_runs (status=%s, changes=%s)",
            status, changes_detected,
        )
    except Exception as e:
        logger.error("No se pudo registrar corrida en BD: %s", e, exc_info=True)


# ============================================================
# Ejecución del pipeline interno
# ============================================================

def get_python_bin() -> str:
    return os.environ.get("PYTHON_BIN", sys.executable)


def run_inner_pipeline(logger: logging.Logger) -> Dict:
    """Ejecuta run_pipeline_diario.py. Extrae ok/fail del log del día."""
    python_bin = get_python_bin()
    logger.info("Ejecutando pipeline interno: %s", PIPELINE_SCRIPT.name)

    start_ts = time.time()
    try:
        result = subprocess.run(
            [python_bin, str(PIPELINE_SCRIPT)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
            timeout=4 * 3600,
        )
    except subprocess.TimeoutExpired:
        logger.error("Timeout (4h) ejecutando %s", PIPELINE_SCRIPT.name)
        return {
            "returncode": -1,
            "timed_out": True,
            "stdout": "",
            "stderr": "timeout 4h",
            "elapsed_s": time.time() - start_ts,
        }

    elapsed = time.time() - start_ts
    stdout = result.stdout or ""
    stderr = result.stderr or ""

    # Volcar stdout/stderr al log del wrapper (completo en archivo, resumido en consola)
    if stdout.strip():
        logger.debug("Pipeline STDOUT:\n%s", stdout.strip())
    if stderr.strip():
        logger.debug("Pipeline STDERR:\n%s", stderr.strip())

    return {
        "returncode": int(result.returncode),
        "timed_out": False,
        "stdout": stdout,
        "stderr": stderr,
        "elapsed_s": elapsed,
    }


def parse_ok_fail_from_stdout(stdout: str) -> (int, int):
    """Extrae OK/Fallos del log de run_pipeline_diario si existe."""
    ok = 0
    fail = 0
    for line in (stdout or "").splitlines()[::-1]:
        if "=== Fin pipeline ===" in line and "OK:" in line:
            try:
                ok_part = line.split("OK:")[1].split(",")[0].strip()
                fail_part = line.split("Fallos:")[1].strip()
                ok = int(ok_part)
                fail = int(fail_part)
            except Exception:
                pass
            break
    return ok, fail


# ============================================================
# Main
# ============================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Wrapper del pipeline diario RETO")
    parser.add_argument(
        "--catch-up",
        action="store_true",
        help="Sólo ejecutar si no hubo corrida hoy (se usa al arrancar la Mac).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Ignorar lockfile (sólo para recuperación manual).",
    )
    parser.add_argument(
        "--triggered-by",
        default=None,
        choices=["scheduled", "catch_up", "manual"],
        help="Origen del trigger (default: scheduled o catch_up según --catch-up).",
    )
    parser.add_argument(
        "--pipeline-name",
        default="reto_x_diario",
        help="Nombre lógico del pipeline para processed.pipeline_runs.",
    )
    args = parser.parse_args()

    logger = setup_logging()
    logger.info("=== run_pipeline_wrapper === host=%s py=%s",
                socket.gethostname(), platform.python_version())

    triggered_by = args.triggered_by or ("catch_up" if args.catch_up else "scheduled")

    # --- 1) Catch-up: si ya corrió hoy, salir sin hacer nada ---
    state = load_last_run()
    if args.catch_up and already_ran_today(state):
        logger.info("catch-up: el pipeline ya corrió hoy (%s) — nada que hacer.",
                    state.get("last_started_date"))
        return 0

    # --- 2) Adquirir lock ---
    lock = PipelineLock(LOCK_FILE)
    try:
        lock.acquire(force=args.force)
    except PipelineLockError as e:
        logger.warning("%s — abortando.", e)
        return 0  # no es un error de negocio; simplemente no corremos

    started_at = datetime.now()
    state["last_started_at"] = started_at.isoformat()
    state["last_started_date"] = started_at.date().isoformat()
    state["last_triggered_by"] = triggered_by
    save_last_run(state)

    try:
        # --- 3) Snapshot de outputs ANTES ---
        logger.info("Snapshot de outputs antes de la corrida...")
        before = snapshot_outputs(TRACKED_OUTPUTS)

        # --- 4) Ejecutar pipeline interno ---
        result = run_inner_pipeline(logger)

        finished_at = datetime.now()

        # --- 5) Snapshot DESPUÉS + diff ---
        logger.info("Snapshot de outputs después de la corrida...")
        after = snapshot_outputs(TRACKED_OUTPUTS)
        changed = diff_snapshots(before, after)
        changes_detected = bool(changed)

        # --- 6) Resumen de resultado ---
        ok_count, fail_count = parse_ok_fail_from_stdout(result["stdout"])
        if result["timed_out"]:
            status = "error"
            detail = "Pipeline timeout"
        elif result["returncode"] == 0:
            status = "ok"
            detail = "Pipeline OK"
        elif fail_count > 0 and ok_count > 0:
            status = "partial"
            detail = f"Pipeline terminó con {fail_count} fallo(s) y {ok_count} OK."
        else:
            status = "error"
            detail = f"Pipeline falló (returncode={result['returncode']})."

        if changed:
            detail += f" Cambios detectados en: {', '.join(Path(p).name for p in changed)}."
        else:
            detail += " Sin cambios en outputs clave (posiblemente no hubo scrape nuevo)."

        logger.info(
            "Resultado: status=%s ok=%d fail=%d changes=%s elapsed=%.1fs",
            status, ok_count, fail_count, changes_detected, result["elapsed_s"],
        )

        # --- 7) Persistir estado local ---
        state["last_finished_at"] = finished_at.isoformat()
        state["last_status"] = status
        state["last_changes_detected"] = changes_detected
        state["last_detail"] = detail
        if status == "ok":
            state["last_success_date"] = finished_at.date().isoformat()
            state["last_success_at"] = finished_at.isoformat()
        save_last_run(state)

        # --- 8) Registrar en BD ---
        register_run_in_db(
            logger=logger,
            pipeline_name=args.pipeline_name,
            started_at=started_at,
            finished_at=finished_at,
            status=status,
            changes_detected=changes_detected,
            ok_count=ok_count,
            fail_count=fail_count,
            triggered_by=triggered_by,
            detail=detail,
        )

        return 0 if status == "ok" else (0 if status == "partial" else 1)

    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
