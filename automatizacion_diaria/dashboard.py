"""
Dashboard RETO — Monitorización de discurso de odio en redes sociales.

Entry point puro: configuración, autenticación, navegación (sidebar) y routing.
La lógica de cada sección vive en `secciones/`; los helpers compartidos en
`components/`.

Uso:
  streamlit run automatizacion_diaria/dashboard.py   (desde la raíz del repo)
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path
from typing import Callable, Dict

import streamlit as st

_AUTO_DIR = Path(__file__).resolve().parent
if str(_AUTO_DIR) not in sys.path:
    sys.path.insert(0, str(_AUTO_DIR))

# ── Shell y tema ──────────────────────────────────
from components.auth import _check_auth, _render_login
from components.theme import (
    _inject_global_css,
    _register_plotly_theme,
    _scroll_main_to_top,
    render_footer,
)
from components.layout import _ensure_db_connection, render_sidebar

# ── Secciones ─────────────────────────────────────
from secciones.proyecto_reto import render_proyecto
from secciones.panel_general import render_panel_general
from secciones.categorias_odio import render_categorias
from secciones.ranking_medios import render_ranking_medios
from secciones.analisis_contextual import render_analisis_contextual
from secciones.comparativa_modelos import render_comparativa
from secciones.calidad_llm import render_calidad_llm
from secciones.terminos_frecuentes import render_terminos
from secciones.buscador_analisis import render_buscador_terminos
from secciones.gold_dataset import render_gold_dataset_router
from secciones.analisis_510 import render_analisis_art510
from secciones.anotacion_validacion import render_anotacion
from secciones.delitos_odio import render_delitos

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="RETO — Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

_register_plotly_theme()

# ============================================================
# PRECALENTADO DE CACHÉ (una vez por proceso)
# ============================================================
def _precalentar_cache() -> None:
    """Ejecuta load_* de secciones públicas para llenar @st.cache_data.

    Corre en un hilo daemon: el primer render no espera. Best-effort —
    un fallo no debe romper el arranque de la app.
    """
    try:
        from components.db_helpers import _load_valid_media_map, load_filter_options
        from secciones.categorias_odio import (
            load_categorias,
            load_intensidad_por_categoria,
        )
        from secciones.panel_general import load_gold_stats, load_kpis

        _load_valid_media_map()
        # Filtros compartidos (Panel, Categorías, Términos, Comparativa…)
        load_filter_options(False)
        # Panel general — defaults sin filtros de plataforma/medio
        load_kpis(False)
        load_gold_stats()
        # Categorías de odio — defaults sin filtros
        load_categorias()
        load_intensidad_por_categoria()
        print("[WARMUP] cache_data de secciones públicas precalentada")
    except Exception as e:
        print(f"[WARMUP] falló el precalentado: {e}")


@st.cache_resource
def _start_cache_warmup() -> bool:
    """Dispara el precalentado una sola vez por proceso Streamlit (no por sesión)."""
    threading.Thread(target=_precalentar_cache, daemon=True).start()
    return True


# ============================================================
# ROUTING
# ============================================================
_SECTION_RENDERERS: Dict[str, Callable[[], None]] = {
    "Proyecto ReTo": render_proyecto,
    "Panel general": render_panel_general,
    "Categorías de odio (LLM)": render_categorias,
    "Ranking de medios": render_ranking_medios,
    "Análisis contextual": render_analisis_contextual,
    "Comparativa modelos": render_comparativa,
    "Calidad LLM": render_calidad_llm,
    "Términos frecuentes": render_terminos,
    "Buscador y Análisis": render_buscador_terminos,
    "Dataset Gold": render_gold_dataset_router,
    "Análisis Art. 510": render_analisis_art510,
    "Anotación y validación": render_anotacion,
    "Delitos de odio (oficial)": render_delitos,
}


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    # Arranca en background; no bloquea el primer render.
    _start_cache_warmup()

    _inject_global_css()
    _check_auth()
    if st.session_state.get("_show_login_form") and st.session_state.get(
        "user_role"
    ) not in ("admin", "editor"):
        _render_login()
        return

    section = render_sidebar()
    prev_section = st.session_state.get("_nav_section")
    section_changed = prev_section != section
    st.session_state["_nav_section"] = section

    # Proyecto ReTo no usa BD; el resto sí (evita bloqueo 30 min sin secrets)
    if section != "Proyecto ReTo" and not _ensure_db_connection():
        return

    renderer = _SECTION_RENDERERS.get(section)
    if renderer:
        renderer()
    else:
        st.error(
            f"Sección no reconocida: {section!r}. "
            "Recargá la página o informá al administrador."
        )

    if section_changed:
        _scroll_main_to_top()

    render_footer()


if __name__ == "__main__":
    main()
