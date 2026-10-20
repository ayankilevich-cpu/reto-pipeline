"""
test_anonimizacion_ui.py — Enmascarado de menciones/URLs para la UI (sin BD ni Streamlit).
Ejecutar: python3 -m pytest automatizacion_diaria/tests/test_anonimizacion_ui.py -v
"""
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from components.anonimizacion import (  # noqa: E402
    MENCION_MASCARA,
    enmascarar_texto_ui,
    url_contexto_segura,
)


def test_caso_reportado_20261020():
    t = "@RoxyRubina @nacion1905 Mujeres solo pensando en el dinero"
    out = enmascarar_texto_ui(t)
    assert "RoxyRubina" not in out and "nacion1905" not in out
    assert out.startswith(f"{MENCION_MASCARA} {MENCION_MASCARA} Mujeres")


def test_handle_youtube_con_punto_y_guion():
    out = enmascarar_texto_ui("Mira @juan.perez-99, qué dices")
    assert "juan" not in out
    assert out == f"Mira {MENCION_MASCARA}, qué dices"


def test_no_toca_emails():
    assert enmascarar_texto_ui("escribe a info@ciedes.org") == "escribe a info@ciedes.org"


def test_urls_con_handle_se_enmascaran():
    out = enmascarar_texto_ui("mira https://x.com/LeonLg775/status/123 y www.youtube.com/@canal")
    assert "LeonLg775" not in out and "canal" not in out
    assert out.count("[enlace]") == 2


def test_texto_vacio_o_nan():
    assert enmascarar_texto_ui(None) == ""
    assert enmascarar_texto_ui(float("nan")) == ""


def test_url_x_sin_handle():
    assert url_contexto_segura("https://x.com/LeonLg775/status/1831234567890") == \
        "https://x.com/i/status/1831234567890"
    assert url_contexto_segura("https://twitter.com/Foo_Bar/status/42?s=20") == \
        "https://x.com/i/status/42"


def test_url_perfil_x_se_descarta():
    assert url_contexto_segura("https://x.com/LeonLg775") is None


def test_url_youtube_intacta_y_basura_none():
    yt = "https://www.youtube.com/watch?v=abc123"
    assert url_contexto_segura(yt) == yt
    assert url_contexto_segura("") is None
    assert url_contexto_segura(None) is None
    assert url_contexto_segura("javascript:alert(1)") is None
