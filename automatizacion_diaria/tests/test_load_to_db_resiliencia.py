"""
test_load_to_db_resiliencia.py — Verifica la lógica de reintento ante errores
transitorios de conexión y el salto con criterio de etapas dependientes en
load_to_db.py.

Motivado por el fallo real del 2026-08-10 (run #166): un
"psycopg2.OperationalError: SSL connection has been closed unexpectedly" a
mitad del upsert de raw.mensajes (X, ~143k filas) hizo fallar esa etapa sin
reintento, y las etapas dependientes (processed.mensajes X, processed.scores)
fallaron a su vez por violación de FK — un solo fallo transitorio se
propagó en 3 "Fallos" confusos en el resumen del pipeline.

No requiere BD ni credenciales — todo mockeado.
Ejecutar: pytest automatizacion_diaria/tests/test_load_to_db_resiliencia.py -v
"""
import logging
import sys
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import MagicMock

import psycopg2
import pytest

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import load_to_db  # noqa: E402


@pytest.fixture
def logger():
    lg = logging.getLogger("test_load_to_db_resiliencia")
    lg.addHandler(logging.NullHandler())
    return lg


@pytest.fixture(autouse=True)
def no_real_sleep(monkeypatch):
    """No dormir de verdad durante los tests de backoff."""
    monkeypatch.setattr(load_to_db.time, "sleep", lambda _seconds: None)


def _fake_get_conn_factory():
    """Devuelve un context manager fake compatible con get_conn()."""
    @contextmanager
    def _fake_get_conn():
        yield MagicMock()
    return _fake_get_conn


# ============================================================
# _run_loader_with_retry
# ============================================================

def test_succeeds_on_first_try(monkeypatch, logger):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock()

    ok = load_to_db._run_loader_with_retry("demo", loader_fn, logger)

    assert ok is True
    assert loader_fn.call_count == 1


def test_retries_transient_error_then_succeeds(monkeypatch, logger):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock(
        side_effect=[
            psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
            None,  # segundo intento: éxito
        ]
    )

    ok = load_to_db._run_loader_with_retry(
        "raw.mensajes (X)", loader_fn, logger, max_retries=3, backoff_seconds=0.01
    )

    assert ok is True
    assert loader_fn.call_count == 2


def test_gives_up_after_max_retries_on_persistent_transient_error(monkeypatch, logger):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock(
        side_effect=psycopg2.OperationalError("SSL connection has been closed unexpectedly")
    )

    ok = load_to_db._run_loader_with_retry(
        "raw.mensajes (X)", loader_fn, logger, max_retries=3, backoff_seconds=0.01
    )

    assert ok is False
    assert loader_fn.call_count == 3  # exactamente max_retries, ni uno más


def test_interface_error_is_also_treated_as_transient(monkeypatch, logger):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock(
        side_effect=[psycopg2.InterfaceError("connection already closed"), None]
    )

    ok = load_to_db._run_loader_with_retry(
        "demo", loader_fn, logger, max_retries=3, backoff_seconds=0.01
    )

    assert ok is True
    assert loader_fn.call_count == 2


def test_does_not_retry_non_transient_error(monkeypatch, logger):
    """Un error de datos (FK, NOT NULL, tipo...) no debe reintentarse —
    volvería a fallar exactamente igual."""
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock(
        side_effect=psycopg2.IntegrityError(
            'insert or update on table "mensajes" violates foreign key constraint'
        )
    )

    ok = load_to_db._run_loader_with_retry(
        "processed.mensajes (X)", loader_fn, logger, max_retries=3, backoff_seconds=0.01
    )

    assert ok is False
    assert loader_fn.call_count == 1  # sin reintentos


def test_generic_exception_is_not_retried(monkeypatch, logger):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    loader_fn = MagicMock(side_effect=ValueError("dato inesperado"))

    ok = load_to_db._run_loader_with_retry(
        "demo", loader_fn, logger, max_retries=3, backoff_seconds=0.01
    )

    assert ok is False
    assert loader_fn.call_count == 1


# ============================================================
# main() — salto con criterio de etapas dependientes
# ============================================================

def _patch_all_loaders(monkeypatch, overrides=None):
    """Reemplaza los 9 loaders de load_to_db por mocks que devuelven éxito
    salvo los indicados en `overrides` (dict nombre_función -> excepción)."""
    overrides = overrides or {}
    names = [
        "load_raw_mensajes",
        "load_processed_mensajes",
        "load_raw_youtube",
        "load_processed_youtube",
        "load_scores",
        "load_etiquetas_llm",
        "load_etiquetas_llm_youtube",
        "load_evaluacion_art510",
        "load_resumen_diario",
    ]
    mocks = {}
    for n in names:
        if n in overrides:
            m = MagicMock(side_effect=overrides[n])
        else:
            m = MagicMock(return_value=None)
        monkeypatch.setattr(load_to_db, n, m)
        mocks[n] = m
    return mocks


def test_main_skips_processed_and_scores_when_raw_x_fails(monkeypatch, capsys):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    mocks = _patch_all_loaders(
        monkeypatch,
        overrides={
            "load_raw_mensajes": psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
        },
    )
    # backoff rápido para el test
    monkeypatch.setattr(load_to_db, "LOAD_DB_RETRY_BACKOFF_SEC", 0.01)

    exit_code = load_to_db.main()

    # raw.mensajes (X) falla persistentemente -> processed.mensajes (X) y
    # processed.scores deben SALTARSE, no intentarse (evita el ruido de FK).
    assert mocks["load_processed_mensajes"].call_count == 0
    assert mocks["load_scores"].call_count == 0
    # YouTube no depende de X: debe seguir cargando con normalidad.
    assert mocks["load_raw_youtube"].call_count == 1
    assert mocks["load_processed_youtube"].call_count == 1
    # etiquetas_llm / art510 / resumen_diario no dependen de raw de hoy.
    assert mocks["load_etiquetas_llm"].call_count == 1
    assert mocks["load_evaluacion_art510"].call_count == 1
    assert mocks["load_resumen_diario"].call_count == 1
    # El pipeline sigue marcándose fallido (exit 1) — no se silencia el error real.
    assert exit_code == 1


def test_main_all_succeed_exit_code_zero(monkeypatch):
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    _patch_all_loaders(monkeypatch)

    exit_code = load_to_db.main()

    assert exit_code == 0


def test_main_raw_youtube_failure_does_not_skip_x_stages(monkeypatch):
    """Un fallo en raw.mensajes (YouTube) no debe afectar a los loaders de X
    (son independientes) — solo a processed.mensajes (YouTube)."""
    monkeypatch.setattr(load_to_db, "get_conn", _fake_get_conn_factory())
    mocks = _patch_all_loaders(
        monkeypatch,
        overrides={
            "load_raw_youtube": psycopg2.OperationalError("SSL connection has been closed unexpectedly"),
        },
    )
    monkeypatch.setattr(load_to_db, "LOAD_DB_RETRY_BACKOFF_SEC", 0.01)

    load_to_db.main()

    assert mocks["load_processed_youtube"].call_count == 0  # saltado
    assert mocks["load_processed_mensajes"].call_count == 1  # X no afectado
    assert mocks["load_scores"].call_count == 1  # X no afectado
