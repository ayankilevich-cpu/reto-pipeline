"""
test_roles.py — Verifica la lógica de acceso por rol del dashboard ReTo.

No requiere BD ni credenciales.
Ejecutar: pytest automatizacion_diaria/tests/test_roles.py -v
"""
import sys
from pathlib import Path

import pytest

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.auth import _ALL_SECTIONS, _RESTRICTED_SECTIONS, _get_sections_for_role

TOTAL_SECTIONS = 14

# Derivados directamente de _RESTRICTED_SECTIONS en auth.py:
#   viewer:  {"Comparativa modelos", "Calidad LLM", "Anotación y validación", "Análisis Art. 510"}
#   editor:  {"Comparativa modelos"}
#   admin:   set()
VIEWER_RESTRICTED = _RESTRICTED_SECTIONS["viewer"]
EDITOR_RESTRICTED = _RESTRICTED_SECTIONS["editor"]


def test_all_sections_count():
    """_ALL_SECTIONS debe tener exactamente 13 secciones."""
    assert len(_ALL_SECTIONS) == TOTAL_SECTIONS, (
        f"Se esperaban {TOTAL_SECTIONS} secciones, hay {len(_ALL_SECTIONS)}"
    )



def test_no_duplicates_in_all_sections():
    """_ALL_SECTIONS no debe tener duplicados."""
    assert len(_ALL_SECTIONS) == len(set(_ALL_SECTIONS)), (
        f"Hay duplicados en _ALL_SECTIONS: {_ALL_SECTIONS}"
    )


def test_admin_sees_all():
    """admin debe ver todas las secciones."""
    sections = set(_get_sections_for_role("admin"))
    for s in _ALL_SECTIONS:
        assert s in sections, f"admin no ve '{s}'"
    assert len(sections) == TOTAL_SECTIONS


def test_editor_count():
    """editor debe ver exactamente TOTAL - len(EDITOR_RESTRICTED) secciones."""
    sections = _get_sections_for_role("editor")
    expected = TOTAL_SECTIONS - len(EDITOR_RESTRICTED)
    assert len(sections) == expected, (
        f"editor debe ver {expected} secciones, ve {len(sections)}: {sections}"
    )


def test_viewer_count():
    """viewer debe ver exactamente TOTAL - len(VIEWER_RESTRICTED) secciones."""
    sections = _get_sections_for_role("viewer")
    expected = TOTAL_SECTIONS - len(VIEWER_RESTRICTED)
    assert len(sections) == expected, (
        f"viewer debe ver {expected} secciones, ve {len(sections)}: {sections}"
    )


def test_editor_cannot_see_restricted():
    """editor no debe ver sus secciones restringidas."""
    sections = set(_get_sections_for_role("editor"))
    for s in EDITOR_RESTRICTED:
        assert s not in sections, f"editor NO debe ver '{s}'"


def test_viewer_cannot_see_restricted():
    """viewer no debe ver sus secciones restringidas."""
    sections = set(_get_sections_for_role("viewer"))
    for s in VIEWER_RESTRICTED:
        assert s not in sections, f"viewer NO debe ver '{s}'"


@pytest.mark.parametrize("role", ["admin", "editor", "viewer"])
def test_sections_are_subset_of_all(role):
    """Las secciones de cualquier rol deben ser un subconjunto de _ALL_SECTIONS."""
    all_set = set(_ALL_SECTIONS)
    for s in _get_sections_for_role(role):
        assert s in all_set, f"'{s}' (rol {role}) no está en _ALL_SECTIONS"


@pytest.mark.parametrize("role", ["admin", "editor", "viewer"])
def test_sections_order_preserved(role):
    """El orden de _ALL_SECTIONS debe preservarse en el resultado de cada rol."""
    role_sections = _get_sections_for_role(role)
    all_filtered = [s for s in _ALL_SECTIONS if s not in _RESTRICTED_SECTIONS.get(role, set())]
    assert role_sections == all_filtered, (
        f"Orden incorrecto para {role}"
    )
