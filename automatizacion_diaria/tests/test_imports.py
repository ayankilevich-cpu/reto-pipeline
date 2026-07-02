"""
test_imports.py — Verifica que todos los módulos del dashboard importan sin errores.

No requiere BD ni credenciales.
Ejecutar: pytest automatizacion_diaria/tests/test_imports.py -v
"""
import importlib
import sys
from pathlib import Path

import pytest

# automatizacion_diaria/ en el path para db_utils y los módulos propios
_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))


COMPONENT_MODULES = [
    "components.art510_shared",
    "components.auth",
    "components.constants",
    "components.db_helpers",
    "components.exports",
    "components.layout",
    "components.theme",
    "components.ui",
    "components.validacion_shared",
]

SECCION_MODULES = [
    "secciones.analisis_510",
    "secciones.analisis_contextual",
    "secciones.anotacion_validacion",
    "secciones.buscador_analisis",
    "secciones.calidad_llm",
    "secciones.categorias_odio",
    "secciones.comparativa_modelos",
    "secciones.delitos_odio",
    "secciones.gold_dataset",
    "secciones.panel_general",
    "secciones.proyecto_reto",
    "secciones.ranking_medios",
    "secciones.terminos_frecuentes",
]


@pytest.mark.parametrize("module_name", COMPONENT_MODULES)
def test_component_imports(module_name):
    """Cada módulo de components/ debe importar sin excepciones."""
    importlib.import_module(module_name)


@pytest.mark.parametrize("module_name", SECCION_MODULES)
def test_seccion_imports(module_name):
    """Cada módulo de secciones/ debe importar sin excepciones."""
    importlib.import_module(module_name)
