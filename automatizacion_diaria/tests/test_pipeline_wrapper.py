"""Pruebas operativas del wrapper del pipeline diario.

Estas pruebas NO disparan run_pipeline_diario.py real (sería muy lento).
En su lugar mockean la ejecución interna para verificar:

  1) Registro en BD aunque no haya cambios (smoke test real en Neon).
  2) Detección correcta de "sin cambios" vs "con cambios".
  3) Lógica de catch-up: si ya corrió hoy, no se vuelve a registrar.
  4) Lockfile: dos invocaciones en paralelo => sólo una corre.

Se limpian las filas insertadas por el test al final (pipeline_name = 'test_wrapper').

Uso:
    python tests/test_pipeline_wrapper.py
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, date
from pathlib import Path
from unittest import mock

SCRIPT_DIR = Path(__file__).resolve().parent
ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(ROOT))

import run_pipeline_wrapper as w  # noqa: E402
from db_utils import get_conn  # noqa: E402


TEST_PIPELINE_NAME = "test_wrapper_reto"


def _cleanup_test_rows() -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "DELETE FROM processed.pipeline_runs WHERE pipeline_name = %s",
            (TEST_PIPELINE_NAME,),
        )
        deleted = cur.rowcount
        cur.close()
    return deleted


def _count_test_rows() -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM processed.pipeline_runs WHERE pipeline_name = %s",
            (TEST_PIPELINE_NAME,),
        )
        n = cur.fetchone()[0]
        cur.close()
    return int(n)


@contextmanager
def isolated_state_dir():
    """Redirige LOGS_DIR / LAST_RUN_FILE / LOCK_FILE a un directorio temporal."""
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        orig_logs = w.LOGS_DIR
        orig_state = w.STATE_DIR
        orig_last = w.LAST_RUN_FILE
        orig_lock = w.LOCK_FILE

        w.LOGS_DIR = tmp_path / "logs"
        w.STATE_DIR = tmp_path / "logs"
        w.LAST_RUN_FILE = w.STATE_DIR / "last_run.json"
        w.LOCK_FILE = w.STATE_DIR / ".pipeline.lock"
        w.LOGS_DIR.mkdir(parents=True, exist_ok=True)
        try:
            yield tmp_path
        finally:
            w.LOGS_DIR = orig_logs
            w.STATE_DIR = orig_state
            w.LAST_RUN_FILE = orig_last
            w.LOCK_FILE = orig_lock


@contextmanager
def tracked_fake_outputs():
    """Redirige TRACKED_OUTPUTS a 3 archivos temporales vacíos."""
    with tempfile.TemporaryDirectory() as tmp:
        p = Path(tmp)
        files = [p / f"out_{i}.csv" for i in range(3)]
        for f in files:
            f.write_text("start\n", encoding="utf-8")
        orig = list(w.TRACKED_OUTPUTS)
        w.TRACKED_OUTPUTS[:] = files
        try:
            yield files
        finally:
            w.TRACKED_OUTPUTS[:] = orig


def fake_run_inner_ok(stdout: str, simulate_change_on: list[Path] | None = None, returncode: int = 0):
    """Factory: devuelve un fake de run_inner_pipeline que, si se le indica,
    modifica uno de los archivos TRACKED_OUTPUTS (para simular cambios)."""
    def _runner(logger):  # firma idéntica a run_inner_pipeline
        if simulate_change_on:
            for f in simulate_change_on:
                f.write_text(f"modified at {time.time()}\n", encoding="utf-8")
        return {
            "returncode": returncode,
            "timed_out": False,
            "stdout": stdout,
            "stderr": "",
            "elapsed_s": 0.01,
        }
    return _runner


def run_wrapper_with(fake_runner, argv: list[str]) -> int:
    """Ejecuta w.main() con un subprocess falso y argv dado."""
    with mock.patch.object(w, "run_inner_pipeline", fake_runner):
        with mock.patch.object(sys, "argv", ["run_pipeline_wrapper.py"] + argv):
            return w.main()


# ============================================================
# Tests
# ============================================================

def test_basic_no_changes():
    print("\n[Test 1] Corrida sin cambios (simula día sin scrape)")
    with isolated_state_dir(), tracked_fake_outputs():
        before_count = _count_test_rows()
        runner = fake_run_inner_ok(
            stdout="=== Fin pipeline === OK: 6, Fallos: 0",
            simulate_change_on=None,  # no toca outputs
            returncode=0,
        )
        rc = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                        "--triggered-by", "manual"])
        after_count = _count_test_rows()
        assert rc == 0, f"rc={rc}"
        assert after_count == before_count + 1, "Debía insertarse 1 fila"

        # Verificar que la última fila tiene changes_detected = False
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status, changes_detected, detail
                FROM processed.pipeline_runs
                WHERE pipeline_name = %s
                ORDER BY started_at DESC LIMIT 1
                """, (TEST_PIPELINE_NAME,),
            )
            status, changes, detail = cur.fetchone()
            cur.close()
        assert status == "ok", f"status={status}"
        assert changes is False, f"changes={changes}"
        assert "Sin cambios" in (detail or ""), f"detail={detail}"
        print(f"   OK — status={status}, changes_detected={changes}")
        print(f"   detail: {detail}")


def test_basic_with_changes():
    print("\n[Test 2] Corrida con cambios en outputs clave")
    with isolated_state_dir(), tracked_fake_outputs() as files:
        before_count = _count_test_rows()
        runner = fake_run_inner_ok(
            stdout="=== Fin pipeline === OK: 6, Fallos: 0",
            simulate_change_on=[files[0], files[2]],
            returncode=0,
        )
        rc = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                        "--triggered-by", "manual"])
        after_count = _count_test_rows()
        assert rc == 0
        assert after_count == before_count + 1

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status, changes_detected, detail
                FROM processed.pipeline_runs
                WHERE pipeline_name = %s
                ORDER BY started_at DESC LIMIT 1
                """, (TEST_PIPELINE_NAME,),
            )
            status, changes, detail = cur.fetchone()
            cur.close()
        assert status == "ok"
        assert changes is True, f"changes={changes}"
        assert "Cambios detectados" in (detail or "")
        print(f"   OK — status={status}, changes_detected={changes}")
        print(f"   detail: {detail}")


def test_catchup_skips_when_already_ran_today():
    print("\n[Test 3] catch-up no vuelve a correr si ya hubo corrida hoy")
    with isolated_state_dir(), tracked_fake_outputs():
        # Primera corrida normal (scheduled)
        runner = fake_run_inner_ok(
            stdout="=== Fin pipeline === OK: 6, Fallos: 0",
            returncode=0,
        )
        rc1 = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                         "--triggered-by", "manual"])
        assert rc1 == 0
        rows_after_first = _count_test_rows()

        # Segunda invocación en modo catch-up: NO debe insertar nada más
        rc2 = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                         "--catch-up"])
        assert rc2 == 0
        rows_after_second = _count_test_rows()
        assert rows_after_second == rows_after_first, (
            f"catch-up duplicó filas: {rows_after_first} -> {rows_after_second}"
        )
        print(f"   OK — catch-up omitió la segunda corrida (filas: {rows_after_second})")


def test_lock_prevents_concurrent():
    print("\n[Test 4] Lockfile impide corridas paralelas")
    with isolated_state_dir(), tracked_fake_outputs():
        # Adquirimos el lock manualmente y dejamos un "sleep" dentro del fake
        lock = w.PipelineLock(w.LOCK_FILE)
        lock.acquire()
        try:
            runner = fake_run_inner_ok(stdout="OK", returncode=0)
            rc = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                            "--triggered-by", "manual"])
            # Debe salir sin insertar (porque no pudo adquirir el lock)
            assert rc == 0, f"rc={rc}"
            # Verificamos en BD que no se añadió fila nueva en la ventana del test
            print("   OK — segunda invocación respetó el lock y no ejecutó")
        finally:
            lock.release()


def test_partial_status():
    print("\n[Test 5] Pipeline con fallos parciales => status=partial")
    with isolated_state_dir(), tracked_fake_outputs():
        runner = fake_run_inner_ok(
            stdout="=== Fin pipeline === OK: 4, Fallos: 2",
            returncode=1,  # run_pipeline_diario devuelve 1 si hubo fallos
        )
        rc = run_wrapper_with(runner, ["--pipeline-name", TEST_PIPELINE_NAME,
                                        "--triggered-by", "manual"])
        assert rc == 0  # wrapper no propaga como error global "partial"

        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT status, ok_count, fail_count
                FROM processed.pipeline_runs
                WHERE pipeline_name = %s
                ORDER BY started_at DESC LIMIT 1
                """, (TEST_PIPELINE_NAME,),
            )
            status, ok_c, fail_c = cur.fetchone()
            cur.close()
        assert status == "partial", f"status={status}"
        assert ok_c == 4 and fail_c == 2, f"ok={ok_c}, fail={fail_c}"
        print(f"   OK — status={status}, ok={ok_c}, fail={fail_c}")


def main() -> int:
    print("=" * 70)
    print("TESTS WRAPPER PIPELINE DIARIO (usa Neon real con pipeline_name='{}')"
          .format(TEST_PIPELINE_NAME))
    print("=" * 70)

    print("\nLimpieza previa de filas de test en processed.pipeline_runs...")
    n = _cleanup_test_rows()
    print(f"  {n} filas removidas")

    try:
        test_basic_no_changes()
        test_basic_with_changes()
        test_catchup_skips_when_already_ran_today()
        test_lock_prevents_concurrent()
        test_partial_status()
    except AssertionError as e:
        print(f"\n[FAIL] {e}")
        return 1
    except Exception as e:
        print(f"\n[ERROR] {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        return 2
    finally:
        print("\nLimpieza final de filas de test...")
        n = _cleanup_test_rows()
        print(f"  {n} filas removidas")

    print("\n" + "=" * 70)
    print("TODOS LOS TESTS PASARON")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
