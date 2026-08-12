"""
test_layout_refrescar_datos.py — Verifica que "Refrescar datos" invalide también
los KPIs cacheados en session_state.

Motivado por un caso real (2026-08-12): tras subir 174 mensajes nuevos con
relevante_llm='SI' a processed.mensajes, el KPI "Total relevantes (YT)" seguía
mostrando el valor viejo aunque se pulsara "Refrescar datos". Causa: los KPIs de
anotación viven en st.session_state (claves `_kpi_*`), fuera del alcance de
st.cache_data.clear().

No requiere BD ni credenciales — session_state mockeado.
Ejecutar: pytest automatizacion_diaria/tests/test_layout_refrescar_datos.py -v
"""
import sys
from pathlib import Path
from types import SimpleNamespace

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components import layout  # noqa: E402


def _fake_st(session_state: dict):
    return SimpleNamespace(session_state=session_state)


def test_clears_kpi_keys(monkeypatch):
    state = {
        "_kpi_ann_yt": {"total_relevantes": 5556},
        "_kpi_ann_yt_sig": ("alejandro", "day"),
        "_kpi_supervision": {"summary": {}},
        "_kpi_v510": {"pendientes": 12},
    }
    monkeypatch.setattr(layout, "st", _fake_st(state))

    layout._clear_kpi_session_state()

    assert state == {}


def test_preserves_unrelated_state(monkeypatch):
    """No debe tocar la cola de anotación ni la sesión del usuario: quien pulsa
    "Refrescar datos" no espera perder el mensaje que está anotando."""
    state = {
        "_kpi_ann_yt": {"total_relevantes": 5556},
        "user_role": "admin",
        "ann_skipped": {"uuid-1"},
        "_ann_yt_queue_cache": ((), "df"),
        "_ann_yt_current_uuid": "uuid-2",
    }
    monkeypatch.setattr(layout, "st", _fake_st(state))

    layout._clear_kpi_session_state()

    assert "_kpi_ann_yt" not in state
    assert state["user_role"] == "admin"
    assert state["ann_skipped"] == {"uuid-1"}
    assert state["_ann_yt_queue_cache"] == ((), "df")
    assert state["_ann_yt_current_uuid"] == "uuid-2"


def test_noop_on_empty_state(monkeypatch):
    state = {}
    monkeypatch.setattr(layout, "st", _fake_st(state))

    layout._clear_kpi_session_state()

    assert state == {}
