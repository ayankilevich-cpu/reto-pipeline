"""
Dashboard RETO — Monitorización de discurso de odio en redes sociales.

Streamlit app con filtros interactivos que consulta PostgreSQL (reto_db).

Secciones del sidebar (orden actual):
  - Proyecto ReTo
  - Panel general
  - Categorías de odio (LLM)
  - Ranking de medios
  - Análisis contextual
  - Comparativa modelos
  - Calidad LLM
  - Términos frecuentes
  - Dataset Gold
  - Análisis Art. 510
  - Anotación y validación (YouTube, Art. 510, validación LLM YT y X)
  - Delitos de odio (oficial)

Checklist de verificación manual (Fase 0 — tras cambios en routing o UI):
  - Login / roles (admin, editor, viewer) y secciones restringidas
  - Cada ítem del sidebar abre sin error y muestra contenido esperado
  - Anotación: las cuatro pestañas y guardado donde aplique
  - Art. 510: filtros y, si se usa, llamada a API
  - Refrescar datos (sidebar) no rompe la sesión

Uso:
  streamlit run dashboard.py
"""

from __future__ import annotations

import base64
import html
import json
import re
import sys
import unicodedata
from io import BytesIO
from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.backends.backend_pdf import PdfPages

try:
    import plotly.io as pio
except Exception:  # pragma: no cover
    pio = None

# Dependencias opcionales para rediseño visual (fallback si Streamlit Cloud no las tiene)
try:
    from streamlit_option_menu import option_menu as _option_menu  # type: ignore
    _HAS_OPTION_MENU = True
except Exception:  # pragma: no cover
    _option_menu = None  # type: ignore[assignment]
    _HAS_OPTION_MENU = False

try:
    from streamlit_extras.stylable_container import stylable_container as _stylable_container  # type: ignore  # noqa: F401
    _HAS_EXTRAS = True
except Exception:  # pragma: no cover
    _stylable_container = None  # type: ignore[assignment]
    _HAS_EXTRAS = False

_AUTO_DIR = Path(__file__).resolve().parent
# Raíz del paquete ReTo (logo y carpeta logos/ están ahí, no dentro de automatizacion_diaria/)
_RETO_ROOT = _AUTO_DIR.parent


def _reto_asset_file(*parts: str) -> Optional[Path]:
    """Resuelve logo u otro asset: mismo dir del script o raíz ReTo (Streamlit Cloud / distintos entrypoints)."""
    for base in (_AUTO_DIR, _RETO_ROOT):
        p = base.joinpath(*parts)
        if p.is_file():
            return p
    return None


def _reto_logos_directory() -> Optional[Path]:
    for base in (_AUTO_DIR, _RETO_ROOT):
        d = base / "logos"
        if d.is_dir():
            return d
    return None


sys.path.insert(0, str(_AUTO_DIR))
from db_utils import get_conn, get_connection_params, postgres_configured

# ── Módulos refactorizados (Fase 1) ────────────────
from components.constants import (
    _expand_platforms,
    LABEL_SOURCE_LABELS,
    CATEGORIAS_ART510,
    CATEGORIA_TO_GRUPO_510,
)
from components.db_helpers import (
    load_filter_options,
    _load_valid_media_map,
    _public_medio_label,
    _load_vllm_yt_corrections,
    load_art510_summary,
    load_art510_validaciones_humanas,
    load_art510_candidates,
    load_gold_full,
)
from secciones.gold_dataset import render_gold_dataset
from secciones.analisis_contextual import render_analisis_contextual
from components.ui import (
    _apply_horizontal_bar_labels,
    _is_viewer,
    _render_section_header,
    _role_can_access_raw,
    _ui_label,
)
from components.exports import df_to_csv_bytes, render_section_exports
from components.auth import (
    _ROLE_DISPLAY,
    _check_auth,
    _get_sections_for_role,
    _render_login,
    _users_have_plain_text_passwords,
)
from components.constants import (
    CATEGORIAS_LABELS,
    CAT_COLORS,
    CAT_COLOR_MAP,
    COLORS,
    DELITOS_COLORS,
    EXCLUDED_SOURCE_MEDIA,
    INTENSITY_COLORS,
    PLATFORM_COLORS,
    PLATFORM_DISPLAY,
    SEMANTIC_COLORS,
    _PLATFORM_ALIASES,
    platform_label,
)
try:
    from contexto_resumen_limpieza import (
        generar_eventos_desde_stats,
        generar_resumen_desde_stats,
        limpiar_eventos_relacionados,
        limpiar_resumen_contexto,
    )
except ImportError:
    generar_resumen_desde_stats = None  # type: ignore[misc, assignment]
    generar_eventos_desde_stats = None
    limpiar_resumen_contexto = lambda t: t or ""  # type: ignore[assignment]
    limpiar_eventos_relacionados = lambda t: t or ""
try:
    from terminos_exclusion_oficial import TERMINOS_EXCLUSION_LEMAS
except ImportError:
    import importlib.util as _ilu
    for _p in (
        _AUTO_DIR / "terminos_exclusion_oficial.py",
        _AUTO_DIR.parent / "terminos_exclusion_oficial.py",
    ):
        if _p.exists():
            _spec = _ilu.spec_from_file_location("terminos_exclusion_oficial", _p)
            _mod = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            TERMINOS_EXCLUSION_LEMAS = _mod.TERMINOS_EXCLUSION_LEMAS
            break
    else:
        TERMINOS_EXCLUSION_LEMAS = frozenset()

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="RETO — Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# TEMA VISUAL GLOBAL (CSS + Plotly template)
# ============================================================
_GLOBAL_CSS = """
<style>
/* Forzar modo claro — evita que el dark mode del SO sobreescriba la UI */
html {
    color-scheme: only light !important;
}
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

html, body, [class*="css"], .stMarkdown, .stButton>button,
.stTextInput input, .stSelectbox, .stMultiSelect, [data-baseweb="tab"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
}
h1, h2, h3, .reto-section-header h1 {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
}

/* --- Selección de texto con color de marca --- */
::selection {
    background: rgba(31, 78, 121, 0.15) !important;
    color: inherit !important;
}
::-moz-selection {
    background: rgba(31, 78, 121, 0.15) !important;
    color: inherit !important;
}

/* --- Scrollbar personalizado (thin, branded) --- */
::-webkit-scrollbar {
    width: 5px;
    height: 5px;
}
::-webkit-scrollbar-track {
    background: transparent;
}
::-webkit-scrollbar-thumb {
    background: #CBD5E0;
    border-radius: 999px;
}
::-webkit-scrollbar-thumb:hover {
    background: #A0AEC0;
}

/* --- Cursor pointer en todos los elementos interactivos --- */
button, [role="button"], label, [data-testid="stRadio"] label,
[data-baseweb="select"], [data-baseweb="tab"],
[data-testid="stExpander"] summary,
[data-testid="stCheckbox"] label,
a, .reto-chip {
    cursor: pointer !important;
}

/* --- Tipografía y jerarquía --- */
h1 { font-weight: 700; letter-spacing: -0.02em; color: #1A202C; }
h2 { font-weight: 600; letter-spacing: -0.015em; color: #1A202C; }
h3 { font-weight: 600; color: #2D3748; }
h4 { font-weight: 600; color: #2D3748; }

/* --- Sidebar institucional --- */
section[data-testid="stSidebar"] {
    background-color: #F4F6F8 !important;
    border-right: 1px solid #E2E8F0;
}
section[data-testid="stSidebar"] > div,
section[data-testid="stSidebar"] [data-testid="stSidebarContent"] {
    background-color: #F4F6F8 !important;
}

/* --- Logo sidebar: tamaño controlado y centrado --- */
section[data-testid="stSidebar"] [data-testid="stImage"] img,
section[data-testid="stSidebar"] .stImage img {
    max-width: 140px !important;
    width: 140px !important;
    height: auto !important;
    display: block !important;
    margin: 0 auto 0.25rem auto !important;
}
section[data-testid="stSidebar"] .stButton>button {
    border-radius: 8px;
    font-weight: 500;
    border: 1px solid #CBD5E0;
    background: #FFFFFF;
    color: #2D3748;
}
section[data-testid="stSidebar"] .stButton>button:hover {
    background: #EBF8FF;
    border-color: #1F4E79;
    color: #1F4E79;
}

/* --- Botón Iniciar sesión: CTA primario --- */
section[data-testid="stSidebar"] [data-testid="stBaseButton-primary"],
section[data-testid="stSidebar"] button[kind="primary"] {
    background: #1F4E79 !important;
    color: #FFFFFF !important;
    border-color: #1F4E79 !important;
    font-weight: 600 !important;
}
section[data-testid="stSidebar"] [data-testid="stBaseButton-primary"]:hover,
section[data-testid="stSidebar"] button[kind="primary"]:hover {
    background: #2B6CB0 !important;
    border-color: #2B6CB0 !important;
}

/* --- Navegación viewer: radio como nav links --- */
section[data-testid="stSidebar"] [data-testid="stRadio"] > label:first-of-type {
    display: none !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] [role="radiogroup"] {
    display: flex;
    flex-direction: column;
    gap: 2px;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label {
    display: flex !important;
    align-items: center !important;
    padding: 8px 12px !important;
    border-radius: 8px !important;
    font-size: 0.875rem !important;
    font-weight: 500 !important;
    color: #4A5568 !important;
    cursor: pointer !important;
    transition: background 0.15s ease, color 0.15s ease !important;
    margin: 0 !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: #EDF2F7 !important;
    color: #1F4E79 !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: #DBEAFE !important;
    color: #1F4E79 !important;
    font-weight: 600 !important;
}
section[data-testid="stSidebar"] [data-testid="stRadio"] input[type="radio"] {
    appearance: none !important;
    width: 0 !important;
    height: 0 !important;
    margin: 0 !important;
    opacity: 0 !important;
    position: absolute !important;
}

/* --- Métricas: tarjetas limpias con hover --- */
[data-testid="stMetric"] {
    background: #FFFFFF;
    border: 1px solid #E2E8F0;
    border-radius: 12px;
    padding: 1rem 1.25rem;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
    transition: box-shadow 0.2s ease, transform 0.2s ease, border-color 0.2s ease;
}
[data-testid="stMetric"]:hover {
    box-shadow: 0 6px 16px rgba(31,78,121,0.12);
    transform: translateY(-1px);
    border-color: #BEE3F8;
}
[data-testid="stMetricLabel"] {
    color: #4A5568 !important;
    font-size: 0.78rem !important;
    font-weight: 600 !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}
[data-testid="stMetricValue"] {
    color: #1F4E79 !important;
    font-weight: 700 !important;
    font-size: 1.85rem !important;
}
[data-testid="stMetricDelta"] {
    font-weight: 500;
}

/* --- Tabs profesionales --- */
[data-baseweb="tab-list"] {
    gap: 4px;
    border-bottom: 1px solid #E2E8F0;
    background: transparent;
}
[data-baseweb="tab"] {
    padding: 10px 22px !important;
    border-radius: 8px 8px 0 0 !important;
    font-weight: 500 !important;
    color: #4A5568 !important;
    transition: all 0.2s ease;
}
[data-baseweb="tab"]:hover {
    background: #F7FAFC !important;
    color: #1F4E79 !important;
}
[data-baseweb="tab"][aria-selected="true"] {
    color: #1F4E79 !important;
    font-weight: 600 !important;
    background: transparent !important;
}
[data-baseweb="tab-highlight"] {
    background: #1F4E79 !important;
    height: 3px !important;
}

/* --- Dataframe mejorado --- */
.stDataFrame thead tr th {
    background: #1F4E79 !important;
    color: #FFFFFF !important;
    font-weight: 600 !important;
    font-size: 0.8rem !important;
    letter-spacing: 0.04em !important;
    text-transform: uppercase !important;
    border-bottom: 2px solid #1A3C5C !important;
    padding: 8px 12px !important;
}
.stDataFrame tbody tr td {
    border-bottom: 1px solid #EDF2F7 !important;
    font-size: 0.875rem !important;
    color: #2D3748 !important;
}
.stDataFrame tbody tr:nth-child(even) td {
    background: #F8FAFC !important;
}
.stDataFrame tbody tr:hover td {
    background: #EBF4FF !important;
}
/* Scroll container sin sombra rara */
.stDataFrame [data-testid="stDataFrameResizable"] {
    border: 1px solid #E2E8F0 !important;
    border-radius: 8px !important;
    overflow: hidden !important;
}

/* --- Section header con barra de acento --- */
.reto-section-header {
    display: flex;
    flex-direction: column;
    gap: 0.15rem;
    margin: 0.2rem 0 1.8rem 0;
    padding: 0.75rem 1.25rem 0.75rem 1.25rem;
    border-left: 5px solid #1F4E79;
    background: #E8EEF4;
    border-radius: 0 8px 8px 0;
}
.reto-section-header h1 {
    margin: 0 0 0.15rem 0 !important;
    font-size: 1.9rem !important;
    line-height: 1.25;
}
.reto-section-header .subtitle {
    color: #4A5568;
    font-size: 0.95rem;
    line-height: 1.45;
}

/* --- Chip de actualización / estado --- */
.reto-chip {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 4px 12px;
    background: #EBF8FF;
    color: #1F4E79;
    border-radius: 999px;
    font-size: 0.78rem;
    font-weight: 600;
    border: 1px solid #BEE3F8;
    letter-spacing: 0.02em;
}
.reto-chip.success { background: #F0FFF4; color: #276749; border-color: #9AE6B4; }
.reto-chip.warning { background: #FFFBEA; color: #975A16; border-color: #FAF089; }
.reto-chip.danger  { background: #FFF5F5; color: #9B2C2C; border-color: #FEB2B2; }

/* --- Panel general: tarjetas KPI visuales --- */
.pg-kpi-card {
    background: linear-gradient(135deg, #1F4E79 0%, #2B6CB0 100%);
    border: 1px solid #1A3C5C;
    border-radius: 12px;
    box-shadow: 0 4px 14px rgba(31, 78, 121, 0.22);
    padding: 0.9rem 1rem;
    min-height: 112px;
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    text-align: center;
}
.pg-kpi-label {
    color: #E6F0FF;
    font-size: 0.77rem;
    font-weight: 600;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    line-height: 1.25;
    margin-bottom: 0.38rem;
}
.pg-kpi-value {
    color: #FFFFFF;
    font-size: 2rem;
    font-weight: 800;
    line-height: 1.05;
}
.pg-kpi-delta {
    color: #D6E9FF;
    font-size: 0.8rem;
    margin-top: 0.28rem;
    font-weight: 600;
}
.pg-kpi-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 0.85rem;
    margin: 0.35rem 0 1rem 0;
    width: 100%;
}
.pg-kpi-section-label {
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #718096;
    margin: 0.6rem 0 0.4rem 0;
}
/* Ranking: barras % Odio visibles y en rojo (coherente con gráfico de odio) */
[data-testid="stDataFrame"] [data-testid="stProgressBar"] > div > div {
    background-color: #C0392B !important;
    min-width: 4px !important;
}

/* --- Separadores más sutiles --- */
hr { border-color: #E2E8F0; margin: 1.2rem 0; }

/* --- Buttons --- */
.stButton>button {
    border-radius: 8px;
    font-weight: 500;
    transition: all 0.2s ease;
}
.stButton>button[kind="primary"] {
    background: #1F4E79;
    border-color: #1F4E79;
}
.stButton>button[kind="primary"]:hover {
    background: #2B6CB0;
    border-color: #2B6CB0;
}
/* Botones secundarios genéricos (ej. "Refrescar datos"): ghost style */
.stButton>button[kind="secondary"] {
    background: transparent !important;
    color: #4A5568 !important;
    border: 1px solid #CBD5E0 !important;
    font-weight: 400 !important;
}
.stButton>button[kind="secondary"]:hover {
    background: #F7FAFC !important;
    border-color: #A0AEC0 !important;
    color: #2D3748 !important;
}

/* --- Expander --- */
[data-testid="stExpander"] {
    border: 1px solid #E2E8F0;
    border-radius: 10px;
    background: #FFFFFF;
}
[data-testid="stExpander"] summary {
    font-weight: 500;
    color: #2D3748;
}

/* --- Alert/info boxes: quitar color azul neon Streamlit --- */
div[data-baseweb="notification"] {
    border-radius: 10px !important;
}

/* --- Foco accesible --- */
*:focus-visible {
    outline: 2px solid #3182CE !important;
    outline-offset: 2px;
    border-radius: 4px;
}

/* --- Inputs en focus con color primario #1F4E79 --- */
.stTextInput input:focus,
.stTextArea textarea:focus {
    border-color: #1F4E79 !important;
    box-shadow: 0 0 0 2px rgba(31,78,121,0.18) !important;
    outline: none !important;
}
[data-baseweb="select"]:focus-within > div:first-child,
[data-baseweb="input"]:focus-within {
    border-color: #1F4E79 !important;
    box-shadow: 0 0 0 2px rgba(31,78,121,0.18) !important;
}

/* --- Slider con color primario --- */
[data-testid="stSlider"] [role="slider"] {
    background-color: #1F4E79 !important;
    border-color: #1F4E79 !important;
}
[data-testid="stSlider"] [data-testid="stSliderTrackFill"],
[data-testid="stSlider"] [class*="TrackFill"] {
    background: linear-gradient(90deg, #1F4E79, #4F81BD) !important;
}

/* --- Checkbox seleccionado con color primario (solo el recuadro, no el texto) --- */
[data-testid="stCheckbox"] label span[aria-checked="true"] {
    background-color: #1F4E79 !important;
    border-color: #1F4E79 !important;
}

/* ── Checkboxes: legibles en dark mode del OS ── */
[data-testid="stCheckbox"] {
    background-color: transparent !important;
}
[data-testid="stCheckbox"] label {
    color: #1a1a2e !important;
    background-color: rgba(255, 255, 255, 0.92) !important;
    padding: 2px 8px !important;
    border-radius: 4px !important;
}
[data-testid="stCheckbox"] label p {
    color: #1a1a2e !important;
}
/* Checkbox inline (en columnas y widgets) */
.stCheckbox > label {
    color: #1a1a2e !important;
    background-color: rgba(255, 255, 255, 0.92) !important;
    padding: 2px 8px !important;
    border-radius: 4px !important;
}

/* --- Botones de descarga (outline navy) --- */
[data-testid="stDownloadButton"] > button,
[data-testid="stDownloadButton"] button[kind="secondary"],
.stDownloadButton > button {
    background: transparent !important;
    color: #1F4E79 !important;
    border: 1.5px solid #1F4E79 !important;
    font-weight: 500 !important;
    font-size: 0.85rem !important;
    padding: 6px 14px !important;
    border-radius: 6px !important;
    transition: background 0.15s ease, color 0.15s ease !important;
}
[data-testid="stDownloadButton"] > button *,
[data-testid="stDownloadButton"] button[kind="secondary"] * {
    color: #1F4E79 !important;
}
[data-testid="stDownloadButton"] > button:hover,
[data-testid="stDownloadButton"] button[kind="secondary"]:hover {
    background: #EBF4FF !important;
    border-color: #163D61 !important;
}

/* --- Selectbox / multiselect borde más definido --- */
[data-baseweb="select"] > div:first-child {
    border-color: #CBD5E0 !important;
    border-radius: 6px !important;
    transition: border-color 0.15s ease !important;
}
[data-baseweb="select"]:hover > div:first-child {
    border-color: #A0AEC0 !important;
}

/* --- Panel de descargas --- */
.reto-download-panel {
    background: #F8FAFC;
    border: 1px solid #E2E8F0;
    border-radius: 10px;
    padding: 1rem 1.25rem;
    margin-top: 0.5rem;
}
.reto-download-panel-title {
    font-size: 0.8rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    color: #718096;
    margin-bottom: 0.6rem;
}
.reto-download-item {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.4rem 0;
    border-bottom: 1px solid #F0F4F8;
    font-size: 0.875rem;
}
.reto-download-item:last-child { border-bottom: none; }

/* --- KPI cards secundarias (actividad reciente) --- */
.metric-card-secondary {
    background-color: #1F4E79 !important;
    opacity: 0.75;
}
.metric-card-secondary .value {
    font-size: 1.5rem !important;
}
.metric-subgrid-label {
    font-size: 0.72rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    color: #718096;
    margin: 0.8rem 0 0.4rem 0;
}

/* --- Footer EU institucional --- */
section[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
section[data-testid="stSidebar"] .stCaption {
    font-size: 0.72rem !important;
    color: #718096 !important;
    line-height: 1.5;
}
/* Separador antes del logo EU */
section[data-testid="stSidebar"] hr:last-of-type {
    border-color: #E2E8F0;
    opacity: 0.6;
    margin: 0.5rem 0;
}
/* Footer principal de cada página */
footer, [data-testid="stFooter"] {
    display: none !important;
}
.reto-page-footer {
    margin-top: 2.5rem;
    padding-top: 1rem;
    border-top: 1px solid #E2E8F0;
    font-size: 0.75rem;
    color: #A0AEC0;
    line-height: 1.6;
}

/* ============================================================
   RESPONSIVE — TABLET (≤ 768px) Y MOBILE (≤ 480px)
   ============================================================ */

@media (max-width: 768px) {
    /* Contenedor principal: menos padding lateral */
    .block-container {
        padding: 1rem 0.75rem 2rem 0.75rem !important;
        max-width: 100% !important;
    }

    /* Modebar de Plotly: ocultar en mobile (no es usable en táctil
       y colisiona con el título del gráfico) */
    .modebar-container,
    .modebar,
    [class*="modebar"] {
        display: none !important;
    }

    /* Título del gráfico: más espacio y sin riesgo de solapamiento */
    .gtitle, .g-gtitle text {
        font-size: 13px !important;
    }
    .js-plotly-plot .plotly .svg-container {
        overflow: visible !important;
    }

    /* Section header: título más pequeño, menos padding */
    .reto-section-header {
        padding: 0.5rem 0.875rem !important;
        margin: 0.15rem 0 1.2rem 0 !important;
    }
    .reto-section-header h1 {
        font-size: 1.4rem !important;
    }
    .reto-section-header .subtitle {
        font-size: 0.85rem !important;
    }

    /* KPI grids: 1 columna en mobile */
    .pg-kpi-grid,
    .metric-grid,
    .guest-metric-grid {
        grid-template-columns: 1fr !important;
        gap: 0.6rem !important;
    }
    .pg-kpi-card {
        padding: 0.875rem 1rem !important;
    }
    .pg-kpi-value {
        font-size: 1.8rem !important;
    }

    /* Chips: texto más pequeño */
    .reto-chip {
        font-size: 0.72rem !important;
        padding: 3px 9px !important;
    }

    /* Hero (página Proyecto ReTo) */
    .reto-hero {
        padding: 1.5rem 1rem !important;
    }
    .reto-hero h1 {
        font-size: 1.6rem !important;
    }

    /* Botones de descarga: ancho completo en mobile */
    [data-testid="stDownloadButton"] > button {
        width: 100% !important;
        justify-content: center !important;
    }

    /* Download panel */
    .reto-download-panel {
        padding: 0.75rem !important;
    }

    /* Tablas: scroll horizontal controlado */
    [data-testid="stDataFrame"] {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
    }

    /* Expanders */
    [data-testid="stExpander"] summary {
        font-size: 0.9rem !important;
    }
}

@media (max-width: 480px) {
    /* Mobile puro: ajustes más agresivos */
    .block-container {
        padding: 0.75rem 0.5rem 2rem 0.5rem !important;
    }

    .reto-section-header h1 {
        font-size: 1.25rem !important;
    }

    /* KPI values más compactos */
    .pg-kpi-value {
        font-size: 1.6rem !important;
    }
    .pg-kpi-label {
        font-size: 0.75rem !important;
    }

    /* Ocultar subtítulos largos en hero */
    .reto-hero .subtitle {
        display: none !important;
    }

    /* Tabs: scroll horizontal si hay muchos */
    [data-testid="stTabs"] [role="tablist"] {
        overflow-x: auto !important;
        -webkit-overflow-scrolling: touch !important;
        flex-wrap: nowrap !important;
    }
    [data-testid="stTabs"] [role="tab"] {
        white-space: nowrap !important;
        font-size: 0.8rem !important;
        padding: 6px 10px !important;
    }
}
</style>
"""


def _inject_global_css() -> None:
    """Inyecta el CSS global en cada rerun para mantener estilos persistentes."""
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


# ============================================================
# CSS específico de los formularios de anotación / validación
# (4 subsecciones: odio · categoría · intensidad · humor)
# Inyección 1 vez por sesión; los selectores están scopeados con clases
# propias y con stylable_container para no afectar otros widgets.
# ============================================================
_ANN_FORM_CSS = """
<style>
.ann-form-title {
    font-family: 'Inter', sans-serif;
    color: #1B3A6B;
    font-weight: 700;
    font-size: 1.05rem;
    margin: 0.25rem 0 0.15rem 0;
}
.ann-form-subtitle {
    color: #5A6675;
    font-size: 0.85rem;
    margin: 0 0 0.85rem 0;
}
.ann-step-header {
    color: #1B3A6B;
    font-weight: 600;
    font-size: 0.95rem;
    margin: 0.6rem 0 0.35rem 0;
    display: flex;
    align-items: center;
    gap: 0.45rem;
}
.ann-step-num {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    background: #1B3A6B;
    color: white;
    border-radius: 50%;
    font-size: 0.78rem;
    font-weight: 700;
    flex-shrink: 0;
}
.ann-step-desc {
    color: #5A6675;
    font-size: 0.83rem;
    margin: 0 0 0.5rem 30px;
}
.ann-cond-banner {
    background: #EEF4FB;
    border-left: 4px solid #1B3A6B;
    border-radius: 6px;
    padding: 0.55rem 0.85rem;
    margin: 1rem 0 0.6rem 0;
    color: #1B3A6B;
    font-size: 0.82rem;
    font-weight: 500;
}
/* Encabezado de paso justo encima del bloque gris (fuera del stylable_container) */
.ann-step-header--standalone {
    margin: 0.35rem 0 0.65rem 0;
}
.ann-humor-hint {
    color: #5A6675;
    font-size: 0.82rem;
    margin: 0.15rem 0 0.35rem 30px;
}
</style>
"""

# CSS scopeado (vía stylable_container) para los 3 sub-bloques visuales
_ANN_CHIPS_CSS = """
div[role="radiogroup"] {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.5rem;
    width: 100%;
    box-sizing: border-box;
}
div[role="radiogroup"] > label {
    min-width: 0;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    padding: 0.65rem 0.45rem;
    border-radius: 8px;
    border: 1.5px solid #CBD5E0;
    background: #FFFFFF;
    cursor: pointer;
    transition: all 0.15s ease;
    margin: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
}
div[role="radiogroup"] > label > div:first-child { display: none; }
div[role="radiogroup"] > label > div:not(:first-child) {
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
}
div[role="radiogroup"] > label p {
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-size: 0.85rem;
    margin: 0 auto;
    text-align: center;
    width: 100%;
    display: block;
    line-height: 1.25;
    word-break: break-word;
    hyphens: auto;
}
div[role="radiogroup"] > label:nth-child(1) { border-color: #C0392B; color: #C0392B; }
div[role="radiogroup"] > label:nth-child(2) { border-color: #2F855A; color: #2F855A; }
div[role="radiogroup"] > label:nth-child(3) { border-color: #B7791F; color: #B7791F; }
div[role="radiogroup"] > label:nth-child(1):hover { background: #FEE2E2; }
div[role="radiogroup"] > label:nth-child(2):hover { background: #DCFCE7; }
div[role="radiogroup"] > label:nth-child(3):hover { background: #FEF3C7; }
div[role="radiogroup"] > label:nth-child(1):has(input:checked) {
    background: #C0392B; border-color: #C0392B;
}
div[role="radiogroup"] > label:nth-child(2):has(input:checked) {
    background: #2F855A; border-color: #2F855A;
}
div[role="radiogroup"] > label:nth-child(3):has(input:checked) {
    background: #B7791F; border-color: #B7791F;
}
div[role="radiogroup"] > label:has(input:checked) p { color: #FFFFFF; }
"""

# Chips de intensidad (mismo patrón que odio/no/dudoso; colores leve→hostil)
_ANN_INTENSITY_CHIPS_CSS = """
div[role="radiogroup"] {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.5rem;
    width: 100%;
    box-sizing: border-box;
}
div[role="radiogroup"] > label {
    min-width: 0;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    padding: 0.7rem 0.4rem;
    border-radius: 8px;
    border: 1.5px solid #CBD5E0;
    background: #FFFFFF;
    cursor: pointer;
    transition: all 0.15s ease;
    margin: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
}
div[role="radiogroup"] > label > div:first-child { display: none; }
div[role="radiogroup"] > label > div:not(:first-child) {
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
}
div[role="radiogroup"] > label p {
    font-weight: 600;
    text-transform: none;
    letter-spacing: 0.02em;
    font-size: 0.8rem;
    margin: 0 auto;
    text-align: center;
    width: 100%;
    display: block;
    line-height: 1.3;
    word-break: break-word;
    hyphens: auto;
}
div[role="radiogroup"] > label:nth-child(1) { border-color: #D97706; color: #B45309; }
div[role="radiogroup"] > label:nth-child(2) { border-color: #EA580C; color: #C2410C; }
div[role="radiogroup"] > label:nth-child(3) { border-color: #C0392B; color: #991B1B; }
div[role="radiogroup"] > label:nth-child(1):hover { background: #FFFBEB; }
div[role="radiogroup"] > label:nth-child(2):hover { background: #FFF7ED; }
div[role="radiogroup"] > label:nth-child(3):hover { background: #FEF2F2; }
div[role="radiogroup"] > label:nth-child(1):has(input:checked) {
    background: #D97706; border-color: #D97706;
}
div[role="radiogroup"] > label:nth-child(2):has(input:checked) {
    background: #EA580C; border-color: #EA580C;
}
div[role="radiogroup"] > label:nth-child(3):has(input:checked) {
    background: #C0392B; border-color: #C0392B;
}
div[role="radiogroup"] > label:has(input:checked) p { color: #FFFFFF; }
"""

_ANN_COND_CSS = """
{
    background: #F7FAFC;
    border-left: 4px solid #1B3A6B;
    border-radius: 0 8px 8px 0;
    padding: 0.85rem 1.1rem 0.75rem 1.1rem;
    margin: 0 0 0.5rem 0;
    overflow: visible !important;
    min-height: 0;
}
"""

_ANN_INTENSITY_RADIO_LABELS: Tuple[str, str, str] = (
    "1 — Leve",
    "2 — Ofensivo",
    "3 — Hostil",
)
_ANN_INTENSITY_LABEL_TO_INT: Dict[str, int] = {
    "1 — Leve": 1,
    "2 — Ofensivo": 2,
    "3 — Hostil": 3,
}


def _ann_intensity_radio_index(default_1_2_3: int) -> int:
    """Índice 0..2 para st.radio según intensidad por defecto (1, 2 o 3)."""
    try:
        d = int(default_1_2_3)
    except (TypeError, ValueError):
        d = 2
    d = max(1, min(3, d))
    return d - 1


def _ann_pick_sticky_row(
    queue: pd.DataFrame,
    state_key: str,
    id_col: str = "message_uuid",
) -> pd.Series:
    """Devuelve la fila "activa" de una cola y la fija en session_state.

    Evita que los reruns (p. ej. al cambiar el radio del paso 1 fuera del
    st.form) cambien el mensaje mostrado por culpa de queries con
    ORDER BY RANDOM() / df.sample(). El mensaje activo solo cambia tras
    Guardar/Saltar (la sección hace pop de `state_key`).
    """
    ids = queue[id_col].astype(str)
    current = st.session_state.get(state_key)
    if current is not None and current in set(ids):
        row = queue.loc[ids == current].iloc[0]
    else:
        row = queue.iloc[0]
        st.session_state[state_key] = str(row[id_col])
    return row


def _ann_get_or_load_queue(
    cache_key: str,
    loader: Callable[..., pd.DataFrame],
    cache_args: Tuple = (),
) -> pd.DataFrame:
    """Cachea la cola de mensajes en session_state hasta el próximo Guardar/Saltar.

    Sin esta caché, las queries con `df.sample(frac=1)` reordenan la cola en
    cada rerun (por ejemplo al cambiar el radio del paso 1 fuera del
    st.form) y el mensaje mostrado puede cambiar antes de que el anotador
    pulse Guardar. `cache_args` invalida la caché si cambia (p. ej. al
    cambiar el filtro de clasificación LLM).
    """
    cached = st.session_state.get(cache_key)
    if cached is not None:
        saved_args, df = cached
        if saved_args == cache_args:
            return df
    df = loader(*cache_args)
    st.session_state[cache_key] = (cache_args, df)
    return df


_ANN_FOOTER_CSS = """
{
    background: #F7FAFC;
    border-top: 1px solid #E2E8F0;
    border-radius: 0 0 8px 8px;
    padding: 0.85rem 1rem 0.6rem 1rem;
    margin-top: 0.85rem;
}
button[kind="primaryFormSubmit"], button[kind="primary"] {
    background: #1B3A6B !important;
    border-color: #1B3A6B !important;
}
button[kind="primaryFormSubmit"]:hover, button[kind="primary"]:hover {
    background: #2C5282 !important;
    border-color: #2C5282 !important;
}
"""


def _inject_anotacion_form_css() -> None:
    """Inyecta el CSS específico de los formularios de anotación (1 vez por sesión)."""
    if st.session_state.get("_reto_ann_css_injected"):
        return
    st.markdown(_ANN_FORM_CSS, unsafe_allow_html=True)
    st.session_state["_reto_ann_css_injected"] = True


@contextmanager
def _ann_styled_box(key: str, css: str):
    """Context manager para scopear CSS a un bloque del form.

    Usa streamlit-extras.stylable_container si está disponible; si no, hace
    fallback transparente a st.container() para no romper la app.
    """
    if _stylable_container is not None:
        with _stylable_container(key=key, css_styles=css):
            yield
    else:
        with st.container():
            yield


def _render_pg_kpi_grid(
    cards: List[Tuple[str, str, str]],
    *,
    secondary: bool = False,
) -> None:
    """Renderiza KPIs del panel general como grid responsive de tarjetas HTML/CSS."""
    cards_html = []
    card_style = ' style="opacity:0.75;"' if secondary else ""
    for label, value, delta in cards:
        d = (
            f'<div class="pg-kpi-delta">{html.escape(delta)}</div>'
            if delta
            else ""
        )
        cards_html.append(
            f'<div class="pg-kpi-card"{card_style}>'
            f'<div class="pg-kpi-label">{html.escape(label)}</div>'
            f'<div class="pg-kpi-value">{html.escape(value)}</div>'
            f"{d}"
            "</div>"
        )
    st.markdown(
        f'<div class="pg-kpi-grid">{"".join(cards_html)}</div>',
        unsafe_allow_html=True,
    )


# Iconos bootstrap por sección para el sidebar (streamlit-option-menu)
SECTION_ICONS: Dict[str, str] = {
    "Proyecto ReTo": "info-circle",
    "Panel general": "speedometer2",
    "Categorías de odio (LLM)": "tags",
    "Ranking de medios": "trophy",
    "Análisis contextual": "graph-up",
    "Comparativa modelos": "arrow-left-right",
    "Calidad LLM": "check2-circle",
    "Términos frecuentes": "cloud",
    "Buscador y Análisis": "search",
    "Dataset Gold": "database",
    "Análisis Art. 510": "file-earmark-text",
    "Anotación y validación": "pencil-square",
    "Delitos de odio (oficial)": "shield-exclamation",
}

# Visible en sidebar: confirmar que el despliegue (Streamlit Cloud, etc.) sirvió este archivo.
DASHBOARD_UI_VERSION = "2.3 · cabeceras sección + paleta Gold/terminos + footer UE"


def _register_plotly_theme() -> None:
    """Registra el template Plotly 'reto' con paleta y estilo unificados."""
    if pio is None:
        return
    template = go.layout.Template()
    template.layout = go.Layout(
        font=dict(family="Inter, -apple-system, sans-serif", size=13, color="#1A202C"),
        title=dict(
            font=dict(size=15, color="#1A202C", family="Inter"),
            x=0.0, xanchor="left", pad=dict(t=4, b=8),
        ),
        paper_bgcolor="#FFFFFF",
        plot_bgcolor="#FFFFFF",
        colorway=list(CAT_COLOR_MAP.values()),
        xaxis=dict(
            gridcolor="#EDF2F7", linecolor="#CBD5E0", zerolinecolor="#E2E8F0",
            tickcolor="#A0AEC0", tickfont=dict(size=12, color="#4A5568"),
        ),
        yaxis=dict(
            gridcolor="#EDF2F7", linecolor="#CBD5E0", zerolinecolor="#E2E8F0",
            tickcolor="#A0AEC0", tickfont=dict(size=12, color="#4A5568"),
        ),
        legend=dict(
            font=dict(size=12, color="#2D3748"),
            bgcolor="rgba(255,255,255,0)",
            bordercolor="rgba(0,0,0,0)",
        ),
        margin=dict(l=40, r=24, t=56, b=44),
        bargap=0.38,
        bargroupgap=0.1,
        hoverlabel=dict(
            bgcolor="#1F4E79", bordercolor="#1F4E79",
            font=dict(color="white", family="Inter", size=12),
        ),
    )
    pio.templates["reto"] = template
    pio.templates.default = "plotly_white+reto"


_register_plotly_theme()


# ── Hashing de contraseñas ──────────────────────────────────────────────────
# Las contraseñas en st.secrets pueden almacenarse como:
#   • plain text (legado): se comparan directamente y se muestra aviso al admin.
#   • hash pbkdf2: formato "pbkdf2:<iterations>:<hex_salt>:<hex_hash>"
#     Generá el hash con: _hash_password("mi_contraseña")
# ──────────────────────────────────────────────────────────────────────────────
import hashlib as _hashlib
import os as _os_auth
import binascii as _binascii


import time as _time


def _nav_section_label(section: str) -> str:
    """Etiqueta del menú lateral según rol."""
    if not _is_viewer():
        return section
    _labels = {
        "Categorías de odio (LLM)": "Categorías de odio por IA",
        "Dataset Gold": "Dataset validado",
    }
    return _labels.get(section, section.replace("(LLM)", "por IA").replace("LLM", "IA"))


# ============================================================
# HELPERS — build dynamic WHERE clauses
# ============================================================
def build_where(
    table_alias: str = "",
    platforms: Optional[List[str]] = None,
    medios: Optional[List[str]] = None,
    categorias: Optional[List[str]] = None,
    intensidades: Optional[List[str]] = None,
    prioridades: Optional[List[str]] = None,
    clasificaciones: Optional[List[str]] = None,
    extra_conditions: Optional[List[str]] = None,
) -> Tuple[str, list]:
    """Build a WHERE clause + params from filter selections."""
    prefix = f"{table_alias}." if table_alias else ""
    conditions = []
    params = []

    platforms = _expand_platforms(platforms)
    if platforms:
        conditions.append(f"{prefix}platform IN %s")
        params.append(tuple(platforms))
    if medios:
        conditions.append(f"{prefix}source_media IN %s")
        params.append(tuple(medios))
    if categorias:
        conditions.append(f"e.categoria_odio_pred IN %s")
        params.append(tuple(categorias))
    if intensidades:
        conditions.append(f"e.intensidad_pred IN %s")
        params.append(tuple(intensidades))
    if prioridades:
        conditions.append(f"s.priority IN %s")
        params.append(tuple(prioridades))
    if clasificaciones:
        conditions.append(f"e.clasificacion_principal IN %s")
        params.append(tuple(clasificaciones))
    if extra_conditions:
        conditions.extend(extra_conditions)

    where = " AND ".join(conditions)
    return (f"WHERE {where}" if where else ""), params


@st.cache_data(ttl=300)
def load_kpis(
    access_raw: bool,
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> dict:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None

    with get_conn() as conn:
        cur = conn.cursor()

        conds_p, params_p = [], []
        if platforms:
            conds_p.append("platform IN %s"); params_p.append(tuple(platforms))
        if medios:
            conds_p.append("source_media IN %s"); params_p.append(tuple(medios))
        wp = f"WHERE {' AND '.join(conds_p)}" if conds_p else ""
        wpc = f"WHERE is_candidate = TRUE" + (f" AND {' AND '.join(conds_p)}" if conds_p else "")

        if access_raw:
            conds_r, params_r = [], []
            if platforms:
                conds_r.append("platform IN %s")
                params_r.append(tuple(platforms))
            wr = f"WHERE {' AND '.join(conds_r)}" if conds_r else ""
            cur.execute(f"SELECT count(*) FROM raw.mensajes {wr}", params_r)
            total_raw = cur.fetchone()[0]
        else:
            cur.execute(f"SELECT count(*) FROM processed.mensajes {wp}", params_p)
            total_raw = cur.fetchone()[0]

        cur.execute(f"SELECT count(*) FROM processed.mensajes {wpc}", params_p)
        total_candidatos = cur.fetchone()[0]

        # scores
        q_scores = """
            SELECT count(*) FILTER (WHERE s.pred_odio = 1), AVG(s.proba_odio)
            FROM processed.scores s
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_s, params_s = [], []
        if platforms:
            conds_s.append("pm.platform IN %s"); params_s.append(tuple(platforms))
        if medios:
            conds_s.append("pm.source_media IN %s"); params_s.append(tuple(medios))
        ws = f"WHERE {' AND '.join(conds_s)}" if conds_s else ""
        cur.execute(f"{q_scores} {ws}", params_s)
        row = cur.fetchone()
        total_odio_baseline = row[0] or 0
        score_promedio = row[1] or 0

        # etiquetas_llm
        q_llm = """
            SELECT count(*),
                   count(*) FILTER (WHERE e.clasificacion_principal = 'ODIO')
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_l, params_l = [], []
        if platforms:
            conds_l.append("pm.platform IN %s"); params_l.append(tuple(platforms))
        if medios:
            conds_l.append("pm.source_media IN %s"); params_l.append(tuple(medios))
        wl = f"WHERE {' AND '.join(conds_l)}" if conds_l else ""
        cur.execute(f"{q_llm} {wl}", params_l)
        row2 = cur.fetchone()
        total_etiquetados_llm = row2[0] or 0
        total_odio_llm = row2[1] or 0

        # medios count (solo medios reales con >= 100 mensajes)
        _excl_params = [tuple(EXCLUDED_SOURCE_MEDIA)]
        _excl_cond = " AND source_media NOT IN %s"
        cur.execute(
            "SELECT count(*) FROM ("
            "  SELECT source_media FROM processed.mensajes "
            "  WHERE source_media IS NOT NULL AND source_media != ''"
            + _excl_cond
            + (f" AND platform IN %s" if platforms else "")
            + "  GROUP BY source_media HAVING COUNT(*) >= 100"
            ") sub",
            _excl_params + ([tuple(platforms)] if platforms else []),
        )
        total_medios = cur.fetchone()[0]

        # gold validados (odio confirmado por humano)
        q_gold = """
            SELECT count(*),
                   count(*) FILTER (WHERE g.y_odio_bin = 1)
            FROM processed.gold_dataset g
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_g, params_g = [], []
        if platforms:
            conds_g.append("pm.platform IN %s"); params_g.append(tuple(platforms))
        if medios:
            conds_g.append("pm.source_media IN %s"); params_g.append(tuple(medios))
        wg = f"WHERE {' AND '.join(conds_g)}" if conds_g else ""
        cur.execute(f"{q_gold} {wg}", params_g)
        row_g = cur.fetchone()
        total_gold = row_g[0] or 0
        total_gold_odio = row_g[1] or 0

        # Registros nuevos hoy
        tbl = "raw.mensajes" if access_raw else "processed.mensajes"
        q_hoy = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE"
        q_hoy_f = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE AND platform IN %s"
        if platforms:
            cur.execute(q_hoy_f, [tuple(platforms)])
        else:
            cur.execute(q_hoy)
        row_hoy = cur.fetchone()
        nuevos_x_hoy = row_hoy[0] or 0
        nuevos_yt_hoy = row_hoy[1] or 0

        # Registros nuevos ayer (CURRENT_DATE - 1)
        q_ayer = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE - 1"
        q_ayer_f = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE - 1 AND platform IN %s"
        if platforms:
            cur.execute(q_ayer_f, [tuple(platforms)])
        else:
            cur.execute(q_ayer)
        row_ayer = cur.fetchone()
        nuevos_x_ayer = row_ayer[0] or 0
        nuevos_yt_ayer = row_ayer[1] or 0

        cur.close()

    return {
        "total_raw": total_raw,
        "total_candidatos": total_candidatos,
        "total_odio_baseline": total_odio_baseline,
        "total_odio_llm": total_odio_llm,
        "total_etiquetados_llm": total_etiquetados_llm,
        "score_promedio": score_promedio,
        "total_medios": total_medios,
        "total_gold": total_gold,
        "total_gold_odio": total_gold_odio,
        "nuevos_x_hoy": nuevos_x_hoy,
        "nuevos_yt_hoy": nuevos_yt_hoy,
        "nuevos_x_ayer": nuevos_x_ayer,
        "nuevos_yt_ayer": nuevos_yt_ayer,
    }


@st.cache_data(ttl=60)
def load_last_pipeline_health_summary(pipeline_name: str = "reto_pipeline_diario") -> dict:
    """
    Lee la última corrida cloud registrada en processed.pipeline_health.

    Devuelve un resumen por plataforma (x / youtube) para usar como
    fuente principal del banner de estado en operación cloud-first.
    """
    try:
        with get_conn() as conn:
            last_run_df = pd.read_sql(
                """
                SELECT run_id, run_at
                FROM processed.pipeline_health
                WHERE pipeline_name = %s
                ORDER BY run_at DESC
                LIMIT 1
                """,
                conn,
                params=(pipeline_name,),
            )
            if last_run_df.empty:
                return {"exists": False}

            run_id = str(last_run_df.iloc[0]["run_id"] or "")
            run_at = last_run_df.iloc[0]["run_at"]

            details_df = pd.read_sql(
                """
                SELECT
                    platform,
                    last_ingested_at,
                    rows_new_window,
                    stagnated,
                    critical_stage_ok,
                    failed_stages,
                    warnings,
                    errors
                FROM processed.pipeline_health
                WHERE pipeline_name = %s
                  AND run_id = %s
                ORDER BY platform ASC
                """,
                conn,
                params=(pipeline_name, run_id),
            )
    except Exception:
        return {"exists": False}

    if details_df.empty:
        return {"exists": False}

    def _safe_text_cell(val) -> str:
        if val is None:
            return ""
        try:
            if pd.isna(val):
                return ""
        except Exception:
            pass
        if isinstance(val, (list, tuple, set)):
            parts = [str(x).strip() for x in val if str(x).strip()]
            return ", ".join(parts)
        return str(val).strip()

    platforms: dict[str, dict] = {}
    for _, row in details_df.iterrows():
        p = str(row.get("platform") or "").strip().lower()
        if not p:
            continue
        platforms[p] = {
            "platform": p,
            "last_ingested_at": row.get("last_ingested_at"),
            "rows_new_window": int(row["rows_new_window"]) if row.get("rows_new_window") is not None else 0,
            "stagnated": bool(row["stagnated"]) if row.get("stagnated") is not None else False,
            "critical_stage_ok": bool(row["critical_stage_ok"]) if row.get("critical_stage_ok") is not None else False,
            "failed_stages": _safe_text_cell(row.get("failed_stages")),
            "warnings": _safe_text_cell(row.get("warnings")),
            "errors": _safe_text_cell(row.get("errors")),
        }

    has_critical_error = any(not p["critical_stage_ok"] for p in platforms.values())
    any_stagnated = any(p["stagnated"] for p in platforms.values())
    has_errors_text = any(bool(p["errors"]) for p in platforms.values())
    has_warnings_text = any(bool(p["warnings"]) for p in platforms.values())

    return {
        "exists": True,
        "source": "pipeline_health",
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "run_at": run_at,
        "platforms": platforms,
        "has_critical_error": has_critical_error,
        "any_stagnated": any_stagnated,
        "has_errors_text": has_errors_text,
        "has_warnings_text": has_warnings_text,
    }


@st.cache_data(ttl=60)
def load_last_pipeline_run_legacy(pipeline_name: str = "reto_x_diario") -> dict:
    """
    Lee la última corrida registrada en processed.pipeline_runs.

    Sirve para mostrar en la app que la actualización diaria se ejecutó
    aunque no haya habido datos nuevos (changes_detected = False).
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(
                """
                SELECT
                    started_at,
                    finished_at,
                    status,
                    changes_detected,
                    ok_count,
                    fail_count,
                    triggered_by,
                    detail
                FROM processed.pipeline_runs
                WHERE pipeline_name = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                conn,
                params=(pipeline_name,),
            )
    except Exception:
        return {"exists": False}

    if df.empty:
        return {"exists": False}

    row = df.iloc[0]
    return {
        "exists": True,
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "changes_detected": bool(row["changes_detected"]) if row["changes_detected"] is not None else False,
        "ok_count": int(row["ok_count"]) if row["ok_count"] is not None else 0,
        "fail_count": int(row["fail_count"]) if row["fail_count"] is not None else 0,
        "triggered_by": row["triggered_by"] or "",
        "detail": row["detail"] or "",
    }


@st.cache_data(ttl=60)
def load_last_pipeline_run(pipeline_name: str = "reto_x_diario") -> dict:
    """
    Compatibilidad temporal: mantiene el nombre histórico de la función.
    """
    return load_last_pipeline_run_legacy(pipeline_name=pipeline_name)


def resolve_pipeline_banner_state(
    health_pipeline_name: str = "reto_pipeline_diario",
    legacy_pipeline_name: str = "reto_x_diario",
    desalineacion_horas: int = 24,
) -> dict:
    """
    Resuelve el estado operativo del banner con prioridad cloud-first:

    1) Fuente principal: processed.pipeline_health
    2) Fallback: processed.pipeline_runs (legacy)
    3) Señal de desalineación cuando legacy quedó viejo/en error pero cloud ya corrió
    """
    health = load_last_pipeline_health_summary(pipeline_name=health_pipeline_name)
    legacy = load_last_pipeline_run_legacy(pipeline_name=legacy_pipeline_name)

    if health.get("exists"):
        run_at = health.get("run_at")
        run_ts = pd.Timestamp(run_at) if run_at is not None else None
        platforms = health.get("platforms") or {}

        expected_platforms = {"x", "youtube"}
        missing_platforms = sorted(expected_platforms - set(platforms.keys()))
        has_platform_gap = len(missing_platforms) > 0

        has_critical_error = bool(health.get("has_critical_error"))
        any_stagnated = bool(health.get("any_stagnated"))
        has_errors_text = bool(health.get("has_errors_text"))
        has_warnings_text = bool(health.get("has_warnings_text"))

        if has_critical_error:
            severity = "error"
        elif any_stagnated or has_platform_gap:
            severity = "warning"
        elif has_errors_text:
            severity = "warning"
        elif has_warnings_text:
            severity = "info"
        else:
            severity = "success"

        issues = []
        for p in sorted(platforms.keys()):
            p_info = platforms[p]
            if not p_info.get("critical_stage_ok", True):
                fail_txt = p_info.get("failed_stages") or "etapas críticas"
                issues.append(f"{p}: fallo crítico ({fail_txt})")
            if p_info.get("stagnated", False):
                issues.append(f"{p}: estancado")
            if p_info.get("errors"):
                issues.append(f"{p}: {p_info['errors']}")
        if has_platform_gap:
            issues.append(f"Plataformas faltantes en healthcheck: {', '.join(missing_platforms)}")

        # Señal de desalineación con legacy (informativa)
        desalineado = False
        desalineado_msg = ""
        if legacy.get("exists"):
            legacy_ts_raw = legacy.get("started_at")
            legacy_ts = pd.Timestamp(legacy_ts_raw) if legacy_ts_raw is not None else None
            legacy_status = (legacy.get("status") or "").lower()
            if run_ts is not None and legacy_ts is not None:
                delta_horas = (run_ts - legacy_ts).total_seconds() / 3600.0
                if delta_horas > desalineacion_horas and legacy_status in {"error", "partial"}:
                    desalineado = True
                    desalineado_msg = (
                        "Se detecta desalineación: pipeline_runs (legacy) quedó más antiguo/en error, "
                        "pero pipeline_health (cloud) tiene corrida más reciente."
                    )

        return {
            "exists": True,
            "source": "pipeline_health",
            "severity": severity,
            "run_at": run_ts,
            "run_id": health.get("run_id") or "",
            "platforms": platforms,
            "issues": issues,
            "has_critical_error": has_critical_error,
            "any_stagnated": any_stagnated,
            "has_platform_gap": has_platform_gap,
            "missing_platforms": missing_platforms,
            "desalineado": desalineado,
            "desalineado_msg": desalineado_msg,
            "legacy_fallback_used": False,
        }

    if legacy.get("exists"):
        status = (legacy.get("status") or "").lower()
        if status == "error":
            severity = "error"
        elif status == "partial":
            severity = "warning"
        elif status == "ok":
            severity = "success" if legacy.get("changes_detected") else "info"
        else:
            severity = "info"

        return {
            "exists": True,
            "source": "pipeline_runs_legacy",
            "severity": severity,
            "run_at": legacy.get("started_at"),
            "run_id": "",
            "platforms": {},
            "issues": [],
            "has_critical_error": status == "error",
            "any_stagnated": False,
            "has_platform_gap": False,
            "missing_platforms": [],
            "desalineado": False,
            "desalineado_msg": "",
            "legacy_fallback_used": True,
            "legacy_info": legacy,
        }

    return {
        "exists": False,
        "source": "none",
        "severity": "info",
        "run_at": None,
        "run_id": "",
        "platforms": {},
        "issues": [],
        "has_critical_error": False,
        "any_stagnated": False,
        "has_platform_gap": False,
        "missing_platforms": [],
        "desalineado": False,
        "desalineado_msg": "",
        "legacy_fallback_used": False,
    }


def render_pipeline_status_banner(
    health_pipeline_name: str = "reto_pipeline_diario",
    legacy_pipeline_name: str = "reto_x_diario",
) -> None:
    """
    Banner operativo cloud-first:
    - Prioriza processed.pipeline_health (GitHub Actions).
    - Usa fallback en processed.pipeline_runs (legacy) si no hay health.
    - Señala desalineación entre fuentes cuando corresponda.
    """
    state = resolve_pipeline_banner_state(
        health_pipeline_name=health_pipeline_name,
        legacy_pipeline_name=legacy_pipeline_name,
    )
    if not state.get("exists"):
        st.info("Aún no hay registros operativos del pipeline (ni cloud ni fallback legacy).")
        return

    run_ts_raw = state.get("run_at")
    try:
        run_ts = pd.Timestamp(run_ts_raw) if run_ts_raw is not None else None
    except Exception:
        run_ts = None
    fecha_txt = run_ts.strftime("%d/%m/%Y %H:%M") if run_ts is not None else "—"

    severity = state.get("severity", "info")
    source = state.get("source")
    source_lbl = "GitHub Actions" if source == "pipeline_health" else "fallback legacy"
    icon = "✅" if severity == "success" else ("❌" if severity == "error" else "⚠️")
    msg = f"{icon} Última actualización: {fecha_txt} ({source_lbl})"

    if severity == "error":
        st.error(msg)
    elif severity in {"warning", "info"}:
        st.warning(msg)
    elif severity == "success":
        st.success(msg)
    else:
        st.info(msg)

    LEGACY_PIPELINE_RUNS_THRESHOLD_DAYS = 7
    if state.get("desalineado") and st.session_state.get("user_role") == "admin":
        legacy = load_last_pipeline_run_legacy(pipeline_name=legacy_pipeline_name)
        legacy_reciente = False
        if legacy.get("exists"):
            legacy_ts_raw = legacy.get("started_at")
            if legacy_ts_raw is not None:
                try:
                    legacy_ts = pd.Timestamp(legacy_ts_raw)
                    now = (
                        pd.Timestamp.now(tz=legacy_ts.tzinfo)
                        if legacy_ts.tzinfo is not None
                        else pd.Timestamp.now()
                    )
                    age_days = (now - legacy_ts).total_seconds() / 86400.0
                    legacy_reciente = age_days < LEGACY_PIPELINE_RUNS_THRESHOLD_DAYS
                except Exception:
                    legacy_reciente = False
        if legacy_reciente:
            st.caption("⚠️ Desalineación detectada: pipeline_runs legacy más antiguo/en error que pipeline_health cloud.")


@st.cache_data(ttl=60)
def load_llm_stats() -> dict:
    """Total de mensajes procesados por LLM, desglosado por plataforma."""
    with get_conn() as conn:
        row = pd.read_sql("""
            SELECT
                COUNT(*)                                           AS total_procesados,
                MAX(e.etiquetado_date::date)                       AS ultima_fecha,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    )
                )                                                  AS agregados_ultima,
                COUNT(*) FILTER (WHERE pm.platform IN ('x','twitter'))  AS total_x,
                COUNT(*) FILTER (WHERE pm.platform = 'youtube')         AS total_yt,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    ) AND pm.platform IN ('x','twitter')
                )                                                  AS agregados_x,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    ) AND pm.platform = 'youtube'
                )                                                  AS agregados_yt
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
        """, conn).iloc[0]
    return {
        "total_procesados": int(row["total_procesados"]),
        "ultima_fecha": row["ultima_fecha"],
        "agregados_ultima": int(row["agregados_ultima"]),
        "total_x": int(row["total_x"]),
        "total_yt": int(row["total_yt"]),
        "agregados_x": int(row["agregados_x"]),
        "agregados_yt": int(row["agregados_yt"]),
    }


@st.cache_data(ttl=300)
def load_categorias(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    intensidades: Optional[Tuple] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    intensidades = list(intensidades) if intensidades else None

    conds = [
        "e.clasificacion_principal = 'ODIO'",
        "e.categoria_odio_pred IS NOT NULL",
        "e.categoria_odio_pred != ''",
    ]
    params = []
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if intensidades:
        conds.append("e.intensidad_pred IN %s"); params.append(tuple(intensidades))

    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred
            ORDER BY total DESC
        """, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_intensidad_por_categoria(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    categorias = list(categorias) if categorias else None

    conds = [
        "e.clasificacion_principal = 'ODIO'",
        "e.categoria_odio_pred IS NOT NULL AND e.categoria_odio_pred != ''",
        "e.intensidad_pred IS NOT NULL AND e.intensidad_pred != ''",
    ]
    params = []
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if categorias:
        conds.append("e.categoria_odio_pred IN %s"); params.append(tuple(categorias))

    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, e.intensidad_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred, e.intensidad_pred
            ORDER BY e.categoria_odio_pred, e.intensidad_pred
        """, conn, params=params)
    return df


def load_muestra_ultima_corrida_llm(limit: int = 20) -> Tuple[pd.DataFrame, Optional[object]]:
    """
    Hasta `limit` mensajes elegidos al azar entre los etiquetados por LLM
    en la misma fecha calendario que load_llm_stats (última actualización).
    El texto viene de processed.mensajes (contenido ya anonimizado en pipeline).

    No usa @st.cache_data: la muestra aleatoria debe poder variar entre ejecuciones.
    """
    with get_conn() as conn:
        df_meta = pd.read_sql(
            """
            SELECT MAX(etiquetado_date::date) AS ultima_fecha
            FROM processed.etiquetas_llm
            """,
            conn,
        )
        ultima = df_meta.iloc[0]["ultima_fecha"]
        if pd.isna(ultima):
            return pd.DataFrame(), None

        df = pd.read_sql(
            f"""
            SELECT * FROM (
                SELECT DISTINCT ON (e.message_uuid)
                    e.message_uuid,
                    e.clasificacion_principal,
                    e.categoria_odio_pred,
                    e.intensidad_pred,
                    e.resumen_motivo,
                    e.etiquetado_date,
                    pm.content_original,
                    pm.platform,
                    pm.source_media
                FROM processed.etiquetas_llm e
                INNER JOIN processed.mensajes pm USING (message_uuid)
                WHERE e.etiquetado_date::date = %s
                ORDER BY e.message_uuid, e.etiquetado_date DESC NULLS LAST
            ) t
            ORDER BY random()
            LIMIT {int(limit)}
            """,
            conn,
            params=[ultima],
        )
    return df, ultima


def _anonimizar_texto_mensaje(texto: str) -> str:
    """Elimina menciones (@usuario) y URLs del texto del mensaje."""
    texto = re.sub(r'@\S+', '', texto)
    texto = re.sub(r'http\S+', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto


def _render_one_muestra_card(row) -> None:
    """Una sola tarjeta de mensaje (fila Series o dict-like)."""
    plat = platform_label(str(row.get("platform") or ""))
    medio_pub = _public_medio_label(row.get("source_media"))
    clasif = (row.get("clasificacion_principal") or "—").strip()
    raw_cat = (row.get("categoria_odio_pred") or "").strip()
    cat_label = CATEGORIAS_LABELS.get(raw_cat, raw_cat or "—")
    intens = (row.get("intensidad_pred") or "").strip() or "—"
    motivo = (row.get("resumen_motivo") or "").strip()
    texto = _anonimizar_texto_mensaje(str(row.get("content_original") or "").strip())
    if len(texto) > 4000:
        texto = texto[:4000] + "…"

    with st.container(border=True):
        if medio_pub:
            st.markdown(
                f"**Plataforma:** {plat} · **Medio monitorizado:** {medio_pub}"
            )
        else:
            st.markdown(f"**Plataforma:** {plat}")
        st.markdown(
            f"**Clasificación:** `{clasif}` · **Categoría:** {cat_label} · "
            f"**Intensidad:** `{intens}`"
        )
        if motivo:
            st.markdown(f"*Resumen (LLM):* {motivo}")
        st.markdown("**Mensaje (anonimizado)**")
        st.text(texto)


def _render_muestra_ultima_corrida_llm_section(*, key_suffix: str = "") -> None:
    """Bloque de UI: carrusel de una tarjeta + flechas (evita scroll vertical largo)."""
    ks = key_suffix or "main"
    idx_key = f"cat_llm_carousel_idx_{ks}"

    st.markdown("### Muestra de mensajes etiquetados por el LLM")
    st.caption(
        "Hasta **20** mensajes al azar del **último día calendario** con etiquetas en "
        "`processed.etiquetas_llm` (misma fecha que la métrica *Última actualización*). "
        "Navegación horizontal con **◀ ▶**; texto desde **processed.mensajes** (anonimizado). "
        "**No se muestran cuentas de usuario ni identificadores personales.** "
        "El nombre del medio solo aparece si consta en el catálogo oficial de medios monitorizados."
    )

    c_btn, _ = st.columns([1, 4])
    if c_btn.button("Nueva muestra aleatoria", key=f"cat_llm_muestra_reroll_{ks}"):
        st.session_state[idx_key] = 0
        st.rerun()

    df_muestra, fecha_muestra = load_muestra_ultima_corrida_llm(limit=20)
    if fecha_muestra is None:
        st.warning(
            "**Sin muestra:** no hay filas en `processed.etiquetas_llm` en la base a la que "
            "conecta esta app (revisá secrets / misma BD que el pipeline)."
        )
    elif df_muestra.empty:
        st.warning(
            "**Sin muestra:** hay fecha de etiquetado en BD pero ningún mensaje hace join con "
            "`processed.mensajes` ese día (UUID desalineados o mensajes no cargados)."
        )
    else:
        n = len(df_muestra)
        if idx_key not in st.session_state:
            st.session_state[idx_key] = 0
        st.session_state[idx_key] = max(0, min(int(st.session_state[idx_key]), n - 1))
        i = int(st.session_state[idx_key])

        if hasattr(fecha_muestra, "strftime"):
            fecha_txt = fecha_muestra.strftime("%d/%m/%Y")
        else:
            fecha_txt = str(fecha_muestra)
        st.caption(
            f"Muestra del **{fecha_txt}** — **{n}** mensajes en el lote · "
            f"mostrando **{i + 1}** de **{n}**."
        )

        col_prev, col_ctr, col_next = st.columns([1, 6, 1])
        with col_prev:
            prev_dis = n <= 1 or i <= 0
            if st.button("◀", key=f"cat_llm_prev_{ks}", help="Anterior", disabled=prev_dis):
                st.session_state[idx_key] = i - 1
                st.rerun()
        with col_ctr:
            st.markdown(
                f"<div style='text-align:center;padding:0.35rem 0;color:#5c6b7a;font-size:0.9rem;'>"
                f"Mensaje <b>{i + 1}</b> / {n}</div>",
                unsafe_allow_html=True,
            )
        with col_next:
            next_dis = n <= 1 or i >= n - 1
            if st.button("▶", key=f"cat_llm_next_{ks}", help="Siguiente", disabled=next_dis):
                st.session_state[idx_key] = i + 1
                st.rerun()

        row = df_muestra.iloc[i]
        _render_one_muestra_card(row)


@st.cache_data(ttl=3600)
def _load_ranking_medios_raw(min_msgs: int = 100, fecha_desde: Optional[str] = None, fecha_hasta: Optional[str] = None) -> pd.DataFrame:
    conds = ["pm.source_media IS NOT NULL AND pm.source_media != ''",
             "pm.source_media NOT IN %s"]
    params: list = [tuple(EXCLUDED_SOURCE_MEDIA)]
    if fecha_desde:
        conds.append("pm.created_at >= %s")
        params.append(fecha_desde)
    if fecha_hasta:
        conds.append("pm.created_at <= %s")
        params.append(fecha_hasta)
    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                pm.source_media,
                pm.platform,
                COUNT(DISTINCT pm.message_uuid) AS total_mensajes,
                COUNT(DISTINCT CASE WHEN pm.has_hate_terms_match
                    THEN pm.message_uuid END) AS candidatos_dict,
                COUNT(DISTINCT CASE WHEN s.pred_odio = 1
                    THEN s.message_uuid END) AS odio_baseline,
                COUNT(DISTINCT CASE WHEN e.clasificacion_principal = 'ODIO'
                    THEN e.message_uuid END) AS odio_llm,
                COUNT(DISTINCT CASE WHEN g.y_odio_bin = 1
                    THEN g.message_uuid END) AS odio_gold,
                COUNT(DISTINCT CASE
                    WHEN s.pred_odio = 1
                      OR e.clasificacion_principal = 'ODIO'
                      OR g.y_odio_bin = 1
                    THEN pm.message_uuid END) AS odio_cualquiera,
                ROUND(AVG(s.proba_odio)::numeric, 3) AS score_promedio
            FROM processed.mensajes pm
            LEFT JOIN processed.scores s USING (message_uuid)
            LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
            LEFT JOIN processed.gold_dataset g USING (message_uuid)
            WHERE {where}
            GROUP BY pm.source_media, pm.platform
            HAVING COUNT(DISTINCT pm.message_uuid) >= {int(min_msgs)}
            ORDER BY total_mensajes DESC
        """, conn, params=params)
    return df


@st.cache_data(ttl=3600)
def load_ranking_medios(
    platforms: Optional[Tuple] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> pd.DataFrame:
    df = _load_ranking_medios_raw(min_msgs=100, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta)
    # Solo incluir medios de la lista maestra validada
    df = df[df["source_media"].apply(lambda sm: _public_medio_label(sm) is not None)]
    if platforms:
        platforms_list = list(platforms)
        df = df[df["platform"].isin(platforms_list)]
    return df


@st.cache_data(ttl=300)
def load_comparativa(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
    prioridades: Optional[Tuple] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    categorias = list(categorias) if categorias else None
    prioridades = list(prioridades) if prioridades else None

    conds = []
    params = []
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if categorias:
        conds.append("e.categoria_odio_pred IN %s"); params.append(tuple(categorias))
    if prioridades:
        conds.append("s.priority IN %s"); params.append(tuple(prioridades))

    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                s.pred_odio AS baseline_pred,
                s.priority AS baseline_priority,
                CASE
                    WHEN e.clasificacion_principal = 'ODIO' THEN 1
                    WHEN e.clasificacion_principal = 'NO_ODIO' THEN 0
                    ELSE -1
                END AS llm_pred,
                e.clasificacion_principal AS llm_clasif,
                e.categoria_odio_pred AS llm_categoria,
                pm.source_media
            FROM processed.scores s
            INNER JOIN processed.etiquetas_llm e USING (message_uuid)
            INNER JOIN processed.mensajes pm USING (message_uuid)
            {where}
        """, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_calidad_llm(
    categorias: Optional[Tuple] = None,
    annotators: Optional[Tuple] = None,
    platforms: Optional[Tuple] = None,
) -> pd.DataFrame:
    categorias = list(categorias) if categorias else None
    annotators = list(annotators) if annotators else None
    platforms = list(platforms) if platforms else None

    conds = []
    params = []
    if categorias:
        conds.append("v.categoria_odio IN %s"); params.append(tuple(categorias))
    if annotators:
        conds.append("v.annotator_id IN %s"); params.append(tuple(annotators))
    if platforms:
        expanded = _expand_platforms(list(platforms))
        conds.append("pm.platform IN %s"); params.append(tuple(expanded))

    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                pm.platform,
                e.clasificacion_principal,
                e.categoria_odio_pred,
                e.intensidad_pred AS llm_intensidad,
                v.odio_flag AS humano_odio,
                v.categoria_odio AS humano_categoria,
                v.intensidad AS humano_intensidad,
                v.annotator_id,
                v.coincide_con_llm
            FROM processed.etiquetas_llm e
            INNER JOIN processed.validaciones_manuales v USING (message_uuid)
            INNER JOIN processed.mensajes pm USING (message_uuid)
            {where}
        """, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_calidad_llm_cobertura() -> pd.DataFrame:
    """Etiquetados LLM y validados (intersección) por plataforma."""
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT
                CASE WHEN pm.platform IN ('x', 'twitter') THEN 'x' ELSE pm.platform END AS platform,
                COUNT(DISTINCT e.message_uuid) AS etiquetados_llm,
                COUNT(DISTINCT v.message_uuid) AS validados
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            LEFT JOIN processed.validaciones_manuales v ON v.message_uuid = e.message_uuid
            GROUP BY 1
            ORDER BY 1
        """, conn)
    return df


@st.cache_data(ttl=300)
def load_annotators() -> list:
    with get_conn() as conn:
        df = pd.read_sql(
            "SELECT DISTINCT annotator_id FROM processed.validaciones_manuales "
            "WHERE annotator_id IS NOT NULL ORDER BY annotator_id", conn
        )
    return df["annotator_id"].tolist()


@st.cache_data(ttl=300)
def load_terminos(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
    solo_candidatos: bool = True,
    ultimas_horas: Optional[int] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    categorias = list(categorias) if categorias else None

    conds = ["pm.matched_terms IS NOT NULL", "pm.matched_terms != ''"]
    params = []
    need_llm_join = False

    if solo_candidatos:
        # «Candidatos» en UI: incluir is_candidate (p. ej. YouTube) aunque el ETL deje
        # has_hate_terms_match en FALSE, o filas solo marcadas por lexicón.
        conds.append("(pm.is_candidate = TRUE OR pm.has_hate_terms_match = TRUE)")
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if categorias:
        conds.append("e.categoria_odio_pred IN %s"); params.append(tuple(categorias))
        need_llm_join = True
    if ultimas_horas:
        # Ingreso al sistema (p. ej. YT cargado hoy con comentario publicado hace días)
        conds.append(
            "COALESCE(pm.processed_at, pm.created_at) >= NOW() - (%s::integer * interval '1 hour')"
        )
        params.append(ultimas_horas)

    where = " AND ".join(conds)
    join_clause = "INNER JOIN processed.etiquetas_llm e USING (message_uuid)" if need_llm_join else ""

    with get_conn() as conn:
        df = pd.read_sql(
            f"SELECT pm.matched_terms FROM processed.mensajes pm {join_clause} WHERE {where}",
            conn, params=params,
        )
    return df


TERMINOS_EXCLUSION_JSON = Path(__file__).resolve().parent / "terminos_excluidos_visualizacion.json"


def _normalize_term_for_filter(token: str) -> str:
    """
    Normaliza términos para conteo/exclusión:
    minúsculas, sin tildes, sin artefactos de formato y espacios colapsados.
    """
    s = str(token or "").strip().lower()
    if not s:
        return ""
    s = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    s = s.strip().strip("\"'`[](){}")
    s = re.sub(r"^[^\w]+|[^\w]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    if not re.search(r"[a-zñ]", s):
        return ""
    if len(s) <= 1:
        return ""
    return s


def _parse_and_normalize_matched_terms(value: Any) -> List[str]:
    """
    Convierte `matched_terms` (string/lista/JSON serializado) en tokens limpios.
    Mejora la exclusión evitando que entren términos con comillas/corchetes/ruido.
    """
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []

    raw_tokens: List[Any] = []

    if isinstance(value, (list, tuple, set)):
        raw_tokens.extend(list(value))
    else:
        raw = str(value).strip()
        if not raw:
            return []

        parsed = None
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None

        if isinstance(parsed, list):
            raw_tokens.extend(parsed)
        elif isinstance(parsed, str):
            raw_tokens.append(parsed)
        else:
            # Soporta delimitadores mixtos frecuentes en matched_terms.
            raw_tokens.extend([t for t in re.split(r"[|,;]", raw) if t is not None])

    normalized: List[str] = []
    for tok in raw_tokens:
        nt = _normalize_term_for_filter(tok)
        if nt:
            normalized.append(nt)
    return normalized


_TERMINOS_EXCLUSION_NORM: frozenset = frozenset(
    x for x in (_normalize_term_for_filter(t) for t in TERMINOS_EXCLUSION_LEMAS) if x
)


def load_terminos_exclusion_set() -> frozenset:
    """Lemas excluidos normalizados. Fuente única: `terminos_exclusion_oficial.py` (generado desde el JSON)."""
    return _TERMINOS_EXCLUSION_NORM


def _filter_counter_terminos_neutros(counter: Counter, exclude: frozenset) -> Counter:
    """Quita del contador las claves cuya forma normalizada está en `exclude`."""
    out = Counter()
    for term, n in counter.items():
        nt = _normalize_term_for_filter(term)
        if not nt or nt in exclude:
            continue
        out[term] = n
    return out


# ============================================================
# SIDEBAR
# ============================================================
def render_sidebar():
    role = st.session_state.get("user_role", "admin")
    user_name = st.session_state.get("user_name", "")

    logo_path = _reto_asset_file("logo_reto.png")
    if logo_path is not None:
        st.sidebar.image(str(logo_path), use_container_width=True)
    else:
        st.sidebar.title("ReTo")
    st.sidebar.caption("Red de Tolerancia contra los delitos de odio")

    st.sidebar.markdown(
        f"**{user_name}** · {_ROLE_DISPLAY.get(role, role)}"
    )

    def _do_logout():
        for k in list(st.session_state.keys()):
            del st.session_state[k]

    if st.session_state.get("user_role") == "viewer":
        if st.sidebar.button("Iniciar sesión", key="login_link_btn", type="primary", use_container_width=True):
            # Sin _show_login_form, _check_auth() volvería a asignar viewer al instante.
            st.session_state["_show_login_form"] = True
            st.session_state["user_role"] = None
            st.session_state["user_name"] = None
            st.rerun()
    else:
        st.sidebar.button("Cerrar sesión", key="logout_btn", on_click=_do_logout)

    st.sidebar.markdown("---")

    sections = _get_sections_for_role(role)

    # Viewer: radio con etiquetas IA (option_menu no admite format_func)
    if role == "viewer":
        section = st.sidebar.radio(
            "Sección",
            sections,
            index=0,
            format_func=_nav_section_label,
            key="nav_menu_viewer",
        )
    # Navegación profesional con streamlit-option-menu (fallback a radio si no está disponible)
    elif _HAS_OPTION_MENU and _option_menu is not None:
        with st.sidebar:
            try:
                icons = [SECTION_ICONS.get(s, "circle") for s in sections]
                section = _option_menu(
                    menu_title=None,
                    options=list(sections),
                    icons=icons,
                    default_index=0,
                    key="nav_menu",
                    styles={
                        "container": {
                            "padding": "0",
                            "background-color": "transparent",
                        },
                        "icon": {"color": "#1F4E79", "font-size": "15px"},
                        "nav-link": {
                            "font-size": "14px",
                            "text-align": "left",
                            "margin": "2px 0",
                            "padding": "8px 12px",
                            "border-radius": "8px",
                            "color": "#2D3748",
                        },
                        "nav-link-selected": {
                            "background-color": "#1F4E79",
                            "color": "white",
                            "font-weight": "600",
                        },
                    },
                )
            except Exception:
                section = st.sidebar.radio("Sección", sections, index=0)
    else:
        section = st.sidebar.radio("Sección", sections, index=0)

    st.sidebar.markdown("---")

    if st.sidebar.button("Refrescar datos"):
        st.cache_data.clear()
        st.rerun()

    # Información técnica: solo visible para admin y plegada por defecto
    if role == "admin":
        with st.sidebar.expander("Información técnica", expanded=False):
            st.caption("Datos: PostgreSQL (reto_db)")
            st.caption(f"Interfaz: {DASHBOARD_UI_VERSION}")
            _last_run = load_last_pipeline_run()
            if _last_run.get("exists"):
                try:
                    _ts = pd.Timestamp(_last_run["started_at"]).strftime("%d/%m %H:%M")
                except Exception:
                    _ts = "—"
                _status = (_last_run.get("status") or "").lower()
                if _status == "error":
                    _icon = "🔴"
                elif _status == "partial":
                    _icon = "🟡"
                elif _last_run.get("changes_detected"):
                    _icon = "🟢"
                else:
                    _icon = "⚪"
                _cambios = "con cambios" if _last_run.get("changes_detected") else "sin cambios"
                st.caption(f"{_icon} Última corrida: {_ts} ({_cambios})")

            # Aviso de contraseñas en texto plano
            if _users_have_plain_text_passwords():
                st.warning(
                    "⚠️ Contraseñas en texto plano detectadas en secrets. "
                    "Actualizalas con hashes pbkdf2 usando `_hash_password('tu_contraseña')` desde la consola."
                )

    eu_logo = _reto_asset_file("logos", "07_eu.png")
    if eu_logo is not None:
        st.sidebar.markdown("---")
        st.sidebar.image(str(eu_logo), use_container_width=True)
        st.sidebar.caption(
            "Proyecto financiado por la Unión Europea — Programa CERV (2024)."
        )

    return section


# ============================================================
# SECTIONS
# ============================================================
@st.cache_data(ttl=3600)
def load_gold_stats() -> dict:
    with get_conn() as conn:
        row = pd.read_sql("""
            WITH llm_comparison AS (
                SELECT
                    g.message_uuid,
                    UPPER(g.y_odio_final) != UPPER(e.clasificacion_principal)        AS corrigio_odio,
                    g.y_categoria_final IS DISTINCT FROM e.categoria_odio_pred
                        AND g.y_categoria_final IS NOT NULL                          AS corrigio_categoria,
                    g.y_intensidad_final IS DISTINCT FROM NULLIF(e.intensidad_pred,'')::smallint
                        AND g.y_intensidad_final IS NOT NULL                        AS corrigio_intensidad
                FROM processed.gold_dataset g
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE g.label_source = 'llm_validated'
                  AND g.y_odio_bin IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM processed.gold_dataset
                 WHERE y_odio_bin IS NOT NULL)                    AS total_gold,
                COUNT(*)                                          AS total_llm,
                COUNT(*) FILTER (WHERE corrigio_odio)             AS n_corrigio_odio,
                COUNT(*) FILTER (WHERE corrigio_categoria)        AS n_corrigio_categoria,
                COUNT(*) FILTER (WHERE corrigio_intensidad
                    AND corrigio_intensidad IS NOT NULL)          AS n_corrigio_intensidad,
                COUNT(*) FILTER (WHERE corrigio_intensidad
                    IS NOT NULL)                                  AS total_con_intensidad,
                (SELECT MAX(ingested_at) FROM processed.gold_dataset) AS fecha_validacion
            FROM llm_comparison
        """, conn).iloc[0]

    total_gold = int(row["total_gold"] or 0)
    total_llm  = int(row["total_llm"]  or 0)
    total_int  = int(row["total_con_intensidad"] or 0)

    return {
        "total_gold":              total_gold,
        "total_llm":               total_llm,
        "pct_concordancia_llm":    (1 - row["n_corrigio_odio"] / total_llm) * 100 if total_llm else None,
        "pct_corrigio_odio":       (row["n_corrigio_odio"]      / total_llm) * 100 if total_llm else None,
        "pct_corrigio_categoria":  (row["n_corrigio_categoria"]  / total_llm) * 100 if total_llm else None,
        "pct_corrigio_intensidad": (row["n_corrigio_intensidad"] / total_int) * 100 if total_int else None,
        "total_con_intensidad":    total_int,
        "fecha_validacion":        row["fecha_validacion"],
    }


def _render_gold_dataset_card() -> None:
    st.markdown("---")
    st.subheader("📋 Gold Dataset")
    try:
        g = load_gold_stats()
    except Exception:
        st.warning("Gold dataset no disponible")
        return
    if not g or g["total_gold"] == 0:
        st.warning("Gold dataset no disponible")
        return

    fecha_str = (
        pd.Timestamp(g["fecha_validacion"]).strftime("%d/%m/%Y")
        if g["fecha_validacion"] is not None else "—"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Total mensajes gold", f"{g['total_gold']:,}")
    c2.metric("Etiquetados con LLM", f"{g['total_llm']:,}")
    c3.metric("Última validación", fecha_str)

    st.markdown("**Calidad del etiquetado LLM** *(sobre los 733 mensajes llm_validated)*")

    c4, c5, c6, c7 = st.columns(4)
    def fmt_pct(v):
        return f"{v:.1f}%" if v is not None else "—"

    c4.metric("Concordancia LLM",     fmt_pct(g["pct_concordancia_llm"]))
    c5.metric("Corrección odio",      fmt_pct(g["pct_corrigio_odio"]))
    c6.metric("Corrección categoría", fmt_pct(g["pct_corrigio_categoria"]))
    c7.metric("Corrección intensidad",fmt_pct(g["pct_corrigio_intensidad"]))

    st.caption(
        f"Correcciones calculadas sobre {g['total_llm']:,} mensajes llm_validated · "
        f"Corrección de intensidad sobre {g['total_con_intensidad']:,} con intensidad registrada"
    )


def render_panel_general():
    _render_section_header(
        "Panel general",
        "Indicadores clave del proyecto ReTo · visión consolidada de volumen, "
        "clasificaciones y validación humana.",
    )

    if st.session_state.get("user_role") != "viewer":
        render_pipeline_status_banner()

    _access_raw = _role_can_access_raw()
    opts = load_filter_options(_access_raw)

    fc1, fc2 = st.columns(2)
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="pg_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="pg_med",
        placeholder="Todos los medios",
    )

    kpis = load_kpis(
        _access_raw,
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
    )

    mensajes_totales = kpis["total_raw"]
    candidatos_odio = kpis["total_candidatos"]
    etiquetados_llm = kpis["total_etiquetados_llm"]
    medios_monitorizados = kpis["total_medios"]
    mensajes_validados = kpis["total_gold"]
    mensajes_odio = kpis["total_gold_odio"]
    nuevos_x_hoy = kpis["nuevos_x_hoy"]
    nuevos_yt_hoy = kpis["nuevos_yt_hoy"]
    nuevos_x_ayer = kpis["nuevos_x_ayer"]
    nuevos_yt_ayer = kpis["nuevos_yt_ayer"]

    label_raw = "Mensajes totales (raw)" if _access_raw else "Mensajes procesados"
    label_llm = "Etiquetados por IA" if not _access_raw else "Etiquetados por LLM"

    _render_pg_kpi_grid([
        (label_raw, f"{mensajes_totales:,}", ""),
        ("Candidatos a odio", f"{candidatos_odio:,}", ""),
        (label_llm, f"{etiquetados_llm:,}", ""),
        ("Mensajes validados", f"{mensajes_validados:,}", f"{mensajes_odio:,} odio"),
        ("Medios monitorizados", f"{medios_monitorizados:,}", ""),
    ])
    st.markdown(
        '<div class="pg-kpi-section-label">Actividad reciente</div>',
        unsafe_allow_html=True,
    )
    _render_pg_kpi_grid([
        ("Nuevos X hoy", f"{nuevos_x_hoy:,}", ""),
        ("Nuevos YouTube hoy", f"{nuevos_yt_hoy:,}", ""),
        ("Nuevos X ayer", f"{nuevos_x_ayer:,}", ""),
        ("Nuevos YouTube ayer", f"{nuevos_yt_ayer:,}", ""),
    ], secondary=True)

    st.markdown("---")

    # --- Cargar datos combinados Gold + LLM para gráficos ---
    df_comb = _load_panel_combined(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
    )

    if df_comb.empty:
        st.info(
            _ui_label("No hay datos clasificados (Gold o LLM) para los filtros seleccionados.")
        )
    else:
        # Cuadro resumen de fuentes
        total_msgs = len(df_comb)
        n_gold = (df_comb["fuente"] == "Gold").sum()
        n_llm = (df_comb["fuente"] == "LLM").sum()
        if st.session_state.get("user_role", "admin") == "admin":
            st.caption(
                f"Visualizaciones basadas en **{total_msgs:,}** mensajes clasificados: "
                f"**{n_gold:,}** validados por humanos (Gold) · "
                f"**{n_llm:,}** etiquetados por LLM"
            )

        # 1. Torta: Odio vs No Odio vs Dudoso (paleta semántica unificada)
        pie_data = df_comb["odio_label"].value_counts().reset_index()
        pie_data.columns = ["Clasificación", "Cantidad"]

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            fig_pie = px.pie(
                pie_data, names="Clasificación", values="Cantidad",
                color="Clasificación", color_discrete_map=SEMANTIC_COLORS,
                hole=0.5, title="Distribución Odio vs No Odio",
            )
            fig_pie.update_traces(
                textinfo="percent",
                textposition="inside",
                textfont_size=14,
                textfont_color="white",
                marker=dict(line=dict(color="#FFFFFF", width=2)),
            )
            fig_pie.update_layout(
                height=380,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # 2. Barras: Odio por plataforma (semánticos + coherente con donut)
        with col_g2:
            plat_data = (
                df_comb.groupby(["plataforma", "odio_label"])
                .size().reset_index(name="Cantidad")
            )
            fig_plat = px.bar(
                plat_data, x="plataforma", y="Cantidad", color="odio_label",
                color_discrete_map=SEMANTIC_COLORS, barmode="group",
                labels={"plataforma": "Plataforma", "odio_label": "Clasificación"},
                title="Distribución de odio por plataforma",
            )
            fig_plat.update_layout(height=380)
            st.plotly_chart(fig_plat, use_container_width=True)

        df_odio = df_comb[df_comb["odio_label"] == "Odio"].copy()

        col_g3, col_g4 = st.columns(2)

        # 3. Distribución de intensidad (paleta coherente)
        with col_g3:
            df_int = df_odio[df_odio["intensidad"].notna()].copy()
            if not df_int.empty:
                df_int["intensidad"] = df_int["intensidad"].astype(int)
                int_data = df_int["intensidad"].value_counts().sort_index().reset_index()
                int_data.columns = ["Intensidad", "Cantidad"]
                int_data["Intensidad"] = int_data["Intensidad"].astype(str)
                fig_int = px.bar(
                    int_data, x="Intensidad", y="Cantidad",
                    color="Intensidad",
                    color_discrete_map=INTENSITY_COLORS,
                    title="Distribución de intensidad (mensajes de odio)",
                    text_auto=True,
                )
                fig_int.update_layout(height=380, showlegend=False)
                st.plotly_chart(fig_int, use_container_width=True)
            else:
                st.info("Sin datos de intensidad.")

        # 4. Distribución de categoría (paleta fija por categoría)
        with col_g4:
            df_cat = df_odio[df_odio["categoria"].notna()].copy()
            if not df_cat.empty:
                df_cat["categoria_label"] = df_cat["categoria"].map(
                    CATEGORIAS_LABELS
                ).fillna(df_cat["categoria"])
                cat_data = (
                    df_cat["categoria_label"].value_counts()
                    .reset_index()
                )
                cat_data.columns = ["Categoría", "Cantidad"]
                df_categoria = cat_data.sort_values("Cantidad", ascending=True)
                fig_cat = px.bar(
                    df_categoria, x="Cantidad", y="Categoría", orientation="h",
                    color="Categoría",
                    color_discrete_map=CAT_COLOR_MAP,
                    title="Distribución por categoría de odio",
                    text_auto=True,
                )
                fig_cat.update_layout(
                    height=380, showlegend=False,
                    yaxis=dict(autorange="reversed"),
                )
                _apply_horizontal_bar_labels(fig_cat)
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Sin datos de categoría.")

        # 5. Intensidad promedio por categoría
        df_cat_int = df_odio[
            df_odio["categoria"].notna() & df_odio["intensidad"].notna()
        ].copy()
        if not df_cat_int.empty:
            df_cat_int["intensidad"] = df_cat_int["intensidad"].astype(float)
            df_cat_int["categoria_label"] = df_cat_int["categoria"].map(
                CATEGORIAS_LABELS
            ).fillna(df_cat_int["categoria"])
            avg_int = (
                df_cat_int.groupby("categoria_label")["intensidad"]
                .mean().round(2).sort_values(ascending=False)
                .reset_index()
            )
            avg_int.columns = ["Categoría", "Intensidad promedio"]
            fig_avg = px.bar(
                avg_int, x="Intensidad promedio", y="Categoría", orientation="h",
                color="Intensidad promedio",
                color_continuous_scale=[[0, "#FBD38D"], [0.5, "#F59E0B"], [1, "#C0392B"]],
                title="Intensidad promedio por categoría de odio",
                text_auto=".2f",
            )
            fig_avg.update_layout(
                height=380, yaxis=dict(autorange="reversed"),
                coloraxis_colorbar=dict(title="Intensidad"),
            )
            _apply_horizontal_bar_labels(fig_avg)
            st.plotly_chart(fig_avg, use_container_width=True)

        render_section_exports(
            section_key="panel_general",
            section_title="Panel general",
            csv_items=[
                ("datos_combinados", df_comb),
                ("kpis", pd.DataFrame([kpis])),
            ],
            fig_items=[
                {"title": "Distribución odio/no odio", "fig": fig_pie if "fig_pie" in locals() else None, "kind": "plotly"},
                {"title": "Distribución por plataforma", "fig": fig_plat if "fig_plat" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad del odio", "fig": fig_int if "fig_int" in locals() else None, "kind": "plotly"},
                {"title": "Categorías de odio", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad promedio por categoría", "fig": fig_avg if "fig_avg" in locals() else None, "kind": "plotly"},
            ],
        )

    # Tarjeta de rendimiento del modelo: solo admin (no viewer ni editor)
    if st.session_state.get("user_role") == "admin":
        _render_gold_dataset_card()


@st.cache_data(ttl=300)
def _load_panel_combined(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> pd.DataFrame:
    """Carga datos combinados Gold + LLM para gráficos del panel general.

    Gold tiene prioridad: si un mensaje está en gold Y en LLM, se usa gold.
    """
    platforms_l = _expand_platforms(list(platforms) if platforms else None)
    medios_l = list(medios) if medios else None

    conds = [
        "(g.message_uuid IS NOT NULL OR e.message_uuid IS NOT NULL)",
    ]
    params: list = []
    if platforms_l:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms_l))
    if medios_l:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios_l))

    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                pm.platform,
                COALESCE(
                    g.y_odio_final,
                    CASE
                        WHEN e.clasificacion_principal = 'ODIO' THEN 'Odio'
                        WHEN e.clasificacion_principal IS NOT NULL THEN 'No Odio'
                    END
                ) AS odio_label,
                COALESCE(
                    g.y_categoria_final,
                    CASE WHEN e.clasificacion_principal = 'ODIO'
                         THEN e.categoria_odio_pred END
                ) AS categoria,
                COALESCE(
                    g.y_intensidad_final::text,
                    CASE WHEN e.clasificacion_principal = 'ODIO'
                         THEN e.intensidad_pred END
                ) AS intensidad,
                CASE WHEN g.message_uuid IS NOT NULL THEN 'Gold'
                     ELSE 'LLM' END AS fuente
            FROM processed.mensajes pm
            LEFT JOIN processed.gold_dataset g USING (message_uuid)
            LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
            WHERE {where}
        """, conn, params=params)

    if not df.empty:
        df["plataforma"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
        df["intensidad"] = pd.to_numeric(df["intensidad"], errors="coerce")

    return df


def render_categorias():
    if _is_viewer():
        _render_section_header(
            "Distribución por categoría de odio",
            "Clasificación por IA en las 6 categorías del proyecto ReTo. "
            "Métricas y gráficos de distribución por categoría e intensidad.",
        )
    else:
        _render_section_header(
            "Distribución por categoría de odio",
            "Clasificación del LLM en las 6 categorías del proyecto ReTo. "
            "<strong>Primero</strong> la muestra de mensajes; <strong>debajo</strong>, métricas y gráficos.",
        )
        _render_muestra_ultima_corrida_llm_section(key_suffix="")
        st.markdown("---")

    llm_stats = load_llm_stats()

    uf = llm_stats["ultima_fecha"]
    total_mensajes_procesados = llm_stats["total_procesados"]
    agregados_ultima_actualizacion = llm_stats["agregados_ultima"]
    ultima_actualizacion = uf.strftime("%d/%m/%Y") if uf is not None and not pd.isna(uf) else "—"
    x_total = llm_stats["total_x"]
    x_ultimos = llm_stats["agregados_x"]
    yt_total = llm_stats["total_yt"]
    yt_ultimos = llm_stats["agregados_yt"]

    st.markdown(f"""
<style>
.metric-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}}
.metric-card {{
    background-color: #1B3A6B;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}}
.metric-card .label {{
    font-size: 13px;
    font-weight: 400;
    opacity: 0.85;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.metric-card .value {{
    font-size: 28px;
    font-weight: 700;
    line-height: 1;
}}
.metric-card .sub {{
    font-size: 12px;
    opacity: 0.7;
    margin-top: 6px;
}}
</style>

<div class="metric-grid">
  <div class="metric-card">
    <div class="label">Total mensajes procesados</div>
    <div class="value">{total_mensajes_procesados:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Agregados en última actualización</div>
    <div class="value">{agregados_ultima_actualizacion:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Última actualización</div>
    <div class="value">{ultima_actualizacion}</div>
  </div>
  <div class="metric-card">
    <div class="label">X — Total</div>
    <div class="value">{x_total:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">X — Últimos agregados</div>
    <div class="value">{x_ultimos:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">YouTube — Total</div>
    <div class="value">{yt_total:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">YouTube — Últimos agregados</div>
    <div class="value">{yt_ultimos:,}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown("---")

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2, fc3 = st.columns(3)
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="cat_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="cat_med",
        placeholder="Todos los medios",
    )
    sel_intensidades = fc3.multiselect(
        "Intensidad", opts["intensidades"], default=[], key="cat_int",
        placeholder="Seleccionar…",
    )

    df = load_categorias(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        intensidades=tuple(sel_intensidades) if sel_intensidades else None,
    )
    if df.empty:
        st.warning("No hay datos de categorías con los filtros seleccionados.")
        return

    df["categoria_label"] = df["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df["categoria_odio_pred"])

    col1, col2 = st.columns(2)

    with col1:
        df_categoria = df.sort_values("total", ascending=True)
        fig = px.bar(
            df_categoria, x="total", y="categoria_label", orientation="h",
            color="categoria_label", color_discrete_map=CAT_COLOR_MAP,
            labels={"total": "Mensajes", "categoria_label": ""},
            title="Mensajes de odio por categoría",
            text_auto=True,
        )
        fig.update_layout(showlegend=False, height=400, yaxis=dict(autorange="reversed"))
        _apply_horizontal_bar_labels(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        fig2 = px.pie(
            df, values="total", names="categoria_label",
            color="categoria_label", color_discrete_map=CAT_COLOR_MAP,
            title="Proporción por categoría", hole=0.5,
        )
        fig2.update_traces(
            textposition="inside",
            textinfo="percent",
            textfont_color="white",
            marker=dict(line=dict(color="#FFFFFF", width=2)),
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("### Intensidad por categoría")

    sel_cats_int = st.multiselect(
        "Filtrar categorías",
        options=list(CATEGORIAS_LABELS.keys()),
        format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
        default=[],
        key="cat_int_filter",
        placeholder="Todas las categorías",
    )

    df_int = load_intensidad_por_categoria(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats_int) if sel_cats_int else None,
    )
    if not df_int.empty:
        df_int["categoria_label"] = df_int["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df_int["categoria_odio_pred"])
        fig3 = px.bar(
            df_int, x="categoria_label", y="total", color="intensidad_pred",
            barmode="group",
            color_discrete_map=INTENSITY_COLORS,
            labels={"total": "Mensajes", "categoria_label": "", "intensidad_pred": "Intensidad"},
            title="Distribución de intensidad (1=baja, 2=media, 3=alta)",
        )
        fig3.update_layout(height=400, xaxis_tickangle=-30)
        st.plotly_chart(fig3, use_container_width=True)

    render_section_exports(
        section_key="categorias_odio",
        section_title="Distribución por categoría de odio",
        csv_items=[
            ("categorias", df),
            ("intensidad_categoria", df_int if "df_int" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Mensajes por categoría", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
            {"title": "Proporción por categoría", "fig": fig2 if "fig2" in locals() else None, "kind": "plotly"},
            {"title": "Intensidad por categoría", "fig": fig3 if "fig3" in locals() else None, "kind": "plotly"},
        ],
    )


def _prepare_ranking_df(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula porcentajes y etiquetas de plataforma sobre el DataFrame de ranking."""
    if df.empty:
        return df
    safe_total = df["total_mensajes"].replace(0, 1)
    df = df.copy()
    df["pct_dict"] = (df["candidatos_dict"] / safe_total * 100).round(1)
    df["pct_odio_baseline"] = (df["odio_baseline"] / safe_total * 100).round(1)
    df["pct_odio_llm"] = (df["odio_llm"] / safe_total * 100).round(1)
    df["pct_odio_gold"] = (df["odio_gold"] / safe_total * 100).round(1)
    df["pct_odio_any"] = (df["odio_cualquiera"] / safe_total * 100).round(1)
    df["plataforma"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
    return df


def _render_ranking_simple(df: pd.DataFrame, top_n: int, key_suffix: str):
    """Top N medios: volumen y % odio. Sin filtros."""
    if df.empty:
        st.info("No hay datos de medios para esta vista.")
        return

    df_vol = df.sort_values("total_mensajes", ascending=False).head(top_n)
    df_pct = df.sort_values("pct_odio_any", ascending=False).head(top_n)
    chart_h = max(350, top_n * 30)

    col1, col2 = st.columns(2)

    with col1:
        fig1 = px.bar(
            df_vol, x="total_mensajes", y="source_media", orientation="h",
            labels={"total_mensajes": "Total mensajes", "source_media": ""},
            title=f"Top {top_n} medios — Volumen de mensajes",
            text_auto=",.0f",
        )
        fig1.update_traces(marker_color="#1F4E79", marker_line_color="#1A3C5C", marker_line_width=0.5)
        fig1.update_layout(
            height=chart_h, yaxis=dict(autorange="reversed"),
            showlegend=False, coloraxis_showscale=False,
        )
        _apply_horizontal_bar_labels(fig1)
        st.plotly_chart(fig1, use_container_width=True, key=f"rm_vol_{key_suffix}")

    with col2:
        fig2 = px.bar(
            df_pct, x="pct_odio_any", y="source_media", orientation="h",
            labels={"pct_odio_any": "% Odio", "source_media": ""},
            title=f"Top {top_n} medios — % Odio",
            text_auto=".1f",
        )
        fig2.update_traces(marker_color="#C0392B", marker_line_color="#8C2A20", marker_line_width=0.5)
        fig2.update_layout(
            height=chart_h, yaxis=dict(autorange="reversed"),
            showlegend=False, coloraxis_showscale=False,
        )
        _apply_horizontal_bar_labels(fig2)
        st.plotly_chart(fig2, use_container_width=True, key=f"rm_pct_{key_suffix}")

    detail_cols = {
        "source_media": "Medio",
        "total_mensajes": "Total",
        "odio_cualquiera": "Odio",
        "pct_odio_any": "% Odio",
    }
    available = [c for c in detail_cols if c in df_vol.columns]
    _column_config: Dict[str, Any] = {}
    try:
        _column_config = {
            "Total": st.column_config.NumberColumn("Total", format="%d"),
            "Odio": st.column_config.NumberColumn("Odio", format="%d"),
            "% Odio": st.column_config.ProgressColumn(
                "% Odio", format="%.1f%%", min_value=0, max_value=100,
                color=COLORS["danger"],
            ),
        }
    except Exception:
        _column_config = {}
    # Tabla del ranking: df_display es la copia mostrada (no el df cargado desde BD).
    df_display = df_vol[available].rename(columns=detail_cols)
    if st.session_state.get("user_role") == "viewer":
        df_display = df_display.drop(columns=["odio_gold"], errors="ignore")
    st.dataframe(
        df_display,
        use_container_width=True, hide_index=True,
        column_config=_column_config if _column_config else None,
        key=f"rm_table_{key_suffix}",
    )


def _filter_ranking_explore_detail_df(detail_df: pd.DataFrame) -> pd.DataFrame:
    """Solo admin ve desglose Odio Baseline / LLM / Gold (y score); resto: candidatos + odio agregado."""
    if st.session_state.get("user_role") == "admin":
        return detail_df
    visibles = {"Candidatos (diccionario)", "Odio — Cualquier fuente"}
    return detail_df[detail_df["Métrica"].isin(visibles)].reset_index(drop=True)


def _render_explorar_medio():
    """Pestaña exploratoria: seleccionar un medio y plataforma para ver sus métricas."""
    st.markdown("Seleccioná un medio y una plataforma para ver sus métricas de odio.")

    valid_names, handle_to_name = _load_valid_media_map()

    df_raw = _load_ranking_medios_raw(min_msgs=1)
    if df_raw.empty:
        st.warning("No hay datos de medios.")
        return

    df_raw = df_raw.copy()
    df_raw["source_media"] = df_raw["source_media"].map(
        lambda sm: handle_to_name.get(sm, sm)
    )

    df_explore = df_raw[df_raw["source_media"].isin(valid_names)].copy()
    if df_explore.empty:
        st.warning("No hay datos de medios reconocidos.")
        return

    num_cols = [
        "total_mensajes", "candidatos_dict", "odio_baseline",
        "odio_llm", "odio_gold", "odio_cualquiera",
    ]
    agg_map = {c: "sum" for c in num_cols}
    agg_map["score_promedio"] = "mean"
    df_explore = df_explore.groupby(
        ["source_media", "platform"], as_index=False,
    ).agg(agg_map)
    df_explore = _prepare_ranking_df(df_explore)

    df_consol = df_explore.groupby("source_media", as_index=False).agg(
        {c: "sum" for c in num_cols}
    )
    df_consol["platform"] = "consolidado"
    df_consol = _prepare_ranking_df(df_consol)
    df_full = pd.concat([df_explore, df_consol], ignore_index=True)

    _TODOS = "Todos"
    all_medios = [_TODOS] + sorted(df_full["source_media"].unique())

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        medio_sel = st.selectbox(
            "Medio", all_medios,
            index=0, key="explore_medio_sel",
        )
    with col_f2:
        plat_opts = ["Consolidado", "X", "YouTube"]
        plat_sel = st.selectbox(
            "Plataforma", plat_opts,
            index=0, key="explore_plat_sel",
        )

    plat_map = {"Consolidado": "consolidado", "X": "x", "YouTube": "youtube"}
    plat_key = plat_map[plat_sel]

    if medio_sel == _TODOS:
        if plat_key == "consolidado":
            agg_row = df_consol[num_cols].sum()
        else:
            plat_slice = df_explore[df_explore["platform"] == plat_key]
            if plat_slice.empty:
                st.info(f"No hay datos en **{plat_sel}**.")
                return
            agg_row = plat_slice[num_cols].sum()
        total = int(agg_row["total_mensajes"])
        odio = int(agg_row["odio_cualquiera"])
        pct = round(odio / max(total, 1) * 100, 1)

        st.markdown("---")
        k1, k2, k3 = st.columns(3)
        k1.metric("Total mensajes", f"{total:,}")
        k2.metric("Mensajes con odio", f"{odio:,}")
        k3.metric("% Odio", f"{pct}%")

        st.markdown("---")
        detail_data = {
            "Métrica": [
                "Candidatos (diccionario)",
                "Odio — Baseline",
                "Odio — LLM",
                "Odio — Gold (validado)",
                "Odio — Cualquier fuente",
            ],
            "Cantidad": [
                int(agg_row["candidatos_dict"]),
                int(agg_row["odio_baseline"]),
                int(agg_row["odio_llm"]),
                int(agg_row["odio_gold"]),
                odio,
            ],
            "% del total": [
                f"{round(agg_row['candidatos_dict'] / max(total, 1) * 100, 1)}%",
                f"{round(agg_row['odio_baseline'] / max(total, 1) * 100, 1)}%",
                f"{round(agg_row['odio_llm'] / max(total, 1) * 100, 1)}%",
                f"{round(agg_row['odio_gold'] / max(total, 1) * 100, 1)}%",
                f"{pct}%",
            ],
        }
        st.dataframe(
            _filter_ranking_explore_detail_df(pd.DataFrame(detail_data)),
            use_container_width=True, hide_index=True,
            key="explore_detail_table",
        )

        if plat_key == "consolidado":
            top_medios = df_consol.sort_values("total_mensajes", ascending=False).head(15)
            fig = px.bar(
                top_medios, x="total_mensajes", y="source_media", orientation="h",
                color="pct_odio_any", color_continuous_scale="Reds",
                labels={"total_mensajes": "Total mensajes", "source_media": "", "pct_odio_any": "% Odio"},
                title="Top 15 medios reconocidos — Volumen (color = % Odio)",
            )
            fig.update_layout(height=500, yaxis=dict(autorange="reversed"))
            _apply_horizontal_bar_labels(fig)
            st.plotly_chart(fig, use_container_width=True, key="explore_todos_chart")
        return

    row = df_full[
        (df_full["source_media"] == medio_sel) & (df_full["platform"] == plat_key)
    ]

    if row.empty:
        st.info(f"No hay datos de **{medio_sel}** en **{plat_sel}**.")
        return

    r = row.iloc[0]
    total = int(r["total_mensajes"])
    odio = int(r["odio_cualquiera"])
    pct = round(odio / max(total, 1) * 100, 1)

    st.markdown("---")

    k1, k2, k3 = st.columns(3)
    k1.metric("Total mensajes", f"{total:,}")
    k2.metric("Mensajes con odio", f"{odio:,}")
    k3.metric("% Odio", f"{pct}%")

    st.markdown("---")

    detail_data = {
        "Métrica": [
            "Candidatos (diccionario)",
            "Odio — Baseline",
            "Odio — LLM",
            "Odio — Gold (validado)",
            "Odio — Cualquier fuente",
            "Score promedio (baseline)",
        ],
        "Cantidad": [
            int(r["candidatos_dict"]),
            int(r["odio_baseline"]),
            int(r["odio_llm"]),
            int(r["odio_gold"]),
            odio,
            r["score_promedio"] if pd.notna(r.get("score_promedio")) else "—",
        ],
        "% del total": [
            f"{r['pct_dict']}%",
            f"{r['pct_odio_baseline']}%",
            f"{r['pct_odio_llm']}%",
            f"{r['pct_odio_gold']}%",
            f"{pct}%",
            "—",
        ],
    }
    st.dataframe(
        _filter_ranking_explore_detail_df(pd.DataFrame(detail_data)),
        use_container_width=True, hide_index=True,
        key="explore_detail_table",
    )

    plats_disponibles = df_explore[df_explore["source_media"] == medio_sel]["platform"].unique()
    if len(plats_disponibles) > 1:
        plat_data = df_explore[df_explore["source_media"] == medio_sel].copy()
        plat_data["plataforma"] = plat_data["platform"].map(PLATFORM_DISPLAY).fillna(plat_data["platform"])
        fig = px.bar(
            plat_data, x="plataforma", y=["total_mensajes", "odio_cualquiera"],
            barmode="group",
            labels={"value": "Mensajes", "variable": "", "plataforma": ""},
            title=f"{medio_sel} — Comparativa por plataforma",
        )
        fig.update_layout(height=350)
        fig.for_each_trace(lambda t: t.update(
            name="Total" if "total" in t.name else "Odio"
        ))
        st.plotly_chart(fig, use_container_width=True, key="explore_plat_chart")


def render_ranking_medios():
    _render_section_header(
        "Ranking de medios",
        "Top 10 medios de comunicación por volumen de mensajes y porcentaje de odio.",
    )

    col_fd, col_fh = st.columns(2)
    with col_fd:
        fecha_desde = st.date_input("Desde", value=None, key="ranking_fecha_desde")
    with col_fh:
        fecha_hasta = st.date_input("Hasta", value=None, key="ranking_fecha_hasta")

    fd_str = fecha_desde.isoformat() if fecha_desde else None
    fh_str = fecha_hasta.isoformat() if fecha_hasta else None

    top_n = 10

    df_all = load_ranking_medios(fecha_desde=fd_str, fecha_hasta=fh_str)
    if df_all.empty:
        st.warning("No hay datos de medios.")
        return
    df_all = _prepare_ranking_df(df_all)

    df_x = df_all[df_all["platform"] == "x"].copy()
    df_yt = df_all[df_all["platform"] == "youtube"].copy()

    # Consolidado
    sum_cols = [
        "total_mensajes", "candidatos_dict", "odio_baseline",
        "odio_llm", "odio_gold", "odio_cualquiera",
    ]
    agg_dict = {c: "sum" for c in sum_cols}
    df_consol = df_all.groupby("source_media", as_index=False).agg(agg_dict)
    df_consol["platform"] = "consolidado"
    df_consol = _prepare_ranking_df(df_consol)

    tab_all, tab_x, tab_yt, tab_explore = st.tabs(["Consolidado", "X", "YouTube", "Explorar medio"])

    with tab_all:
        _render_ranking_simple(df_consol, top_n, "all")

    with tab_x:
        if df_x.empty:
            st.info("No hay datos de medios en X.")
        else:
            _render_ranking_simple(df_x, top_n, "x")
            if _is_viewer():
                st.caption("📅 Monitorización activa: lunes y jueves.")

    with tab_yt:
        if df_yt.empty:
            st.info("No hay datos de medios en YouTube.")
        else:
            _render_ranking_simple(df_yt, top_n, "yt")

    with tab_explore:
        _render_explorar_medio()

    render_section_exports(
        section_key="ranking_medios",
        section_title="Ranking de medios",
        csv_items=[
            ("consolidado", df_consol if "df_consol" in locals() else pd.DataFrame()),
            ("x", df_x if "df_x" in locals() else pd.DataFrame()),
            ("youtube", df_yt if "df_yt" in locals() else pd.DataFrame()),
            ("todos", df_all if "df_all" in locals() else pd.DataFrame()),
        ],
        fig_items=[],
    )


def _require_role(*allowed_roles: str, section: str = "esta sección") -> bool:
    """Guard de acceso: detiene el renderer si el rol no está autorizado.
    Devuelve True si el acceso está permitido, False si no."""
    role = st.session_state.get("user_role")
    if role not in allowed_roles:
        st.error(f"No tenés permisos para acceder a {section}.")
        st.info("Si creés que es un error, iniciá sesión con las credenciales correctas.")
        st.stop()
        return False
    return True


def render_comparativa():
    if not _require_role("admin", "editor", section="Comparativa modelos"):
        return
    _render_section_header(
        "Comparativa: Baseline vs LLM",
        "Concordancia entre el modelo baseline (TF-IDF + LogReg) y el etiquetado LLM.",
    )

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2, fc3, fc4 = st.columns(4)
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="comp_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="comp_med",
        placeholder="Todos los medios",
    )
    sel_cats = fc3.multiselect(
        "Categoría LLM",
        options=list(CATEGORIAS_LABELS.keys()),
        format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
        default=[], key="comp_cat",
        placeholder="Todas las categorías",
    )
    sel_prio = fc4.multiselect(
        "Prioridad (baseline)", opts["prioridades"], default=[], key="comp_prio",
        placeholder="Seleccionar…",
    )

    df = load_comparativa(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats) if sel_cats else None,
        prioridades=tuple(sel_prio) if sel_prio else None,
    )
    if df.empty:
        st.warning("No hay datos con ambos modelos para comparar con los filtros seleccionados.")
        return

    df_clean = df[df["llm_pred"] >= 0].copy()

    total = len(df_clean)
    coinciden = (df_clean["baseline_pred"] == df_clean["llm_pred"]).sum()
    pct_acuerdo = coinciden / total * 100 if total > 0 else 0

    col1, col2, col3 = st.columns(3)
    col1.metric("Mensajes comparados", f"{total:,}")
    col2.metric("Coincidencias", f"{coinciden:,}")
    col3.metric("% Acuerdo", f"{pct_acuerdo:.1f}%")

    st.markdown("---")
    st.markdown("### Matriz de concordancia")

    ambos_odio = ((df_clean["baseline_pred"] == 1) & (df_clean["llm_pred"] == 1)).sum()
    base_odio_llm_no = ((df_clean["baseline_pred"] == 1) & (df_clean["llm_pred"] == 0)).sum()
    base_no_llm_odio = ((df_clean["baseline_pred"] == 0) & (df_clean["llm_pred"] == 1)).sum()
    ambos_no = ((df_clean["baseline_pred"] == 0) & (df_clean["llm_pred"] == 0)).sum()

    matrix = [[ambos_no, base_no_llm_odio], [base_odio_llm_no, ambos_odio]]

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=["LLM: No odio", "LLM: Odio"],
        y=["Baseline: No odio", "Baseline: Odio"],
        text=[[str(v) for v in row] for row in matrix],
        texttemplate="%{text}",
        textfont={"size": 18},
        colorscale=[[0, "#EBF8FF"], [0.5, "#63B3ED"], [1, "#1F4E79"]],
        showscale=False,
    ))
    fig.update_layout(title="Baseline vs LLM", height=350, xaxis_title="LLM", yaxis_title="Baseline")
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("### Discrepancias")
    col1, col2 = st.columns(2)
    col1.metric("Baseline ODIO → LLM NO", f"{base_odio_llm_no:,}", help="Posibles falsos positivos del baseline")
    col2.metric("Baseline NO → LLM ODIO", f"{base_no_llm_odio:,}", help="Posibles falsos negativos del baseline")

    dudosos = len(df[df["llm_pred"] == -1])
    if dudosos > 0:
        st.info(f"**{dudosos:,}** mensajes clasificados como DUDOSO por el LLM (excluidos de la comparativa).")

    # Desglose por categoría LLM
    if not df_clean.empty and "llm_categoria" in df_clean.columns:
        st.markdown("### Acuerdo por categoría LLM")
        df_odio = df_clean[(df_clean["llm_pred"] == 1) & (df_clean["llm_categoria"].notna()) & (df_clean["llm_categoria"] != "")].copy()
        if not df_odio.empty:
            df_odio["coincide"] = df_odio["baseline_pred"] == df_odio["llm_pred"]
            cat_agg = df_odio.groupby("llm_categoria").agg(
                total=("coincide", "count"),
                acuerdo=("coincide", "sum"),
            ).reset_index()
            cat_agg["pct_acuerdo"] = (cat_agg["acuerdo"] / cat_agg["total"] * 100).round(1)
            cat_agg["categoria_label"] = cat_agg["llm_categoria"].map(CATEGORIAS_LABELS).fillna(cat_agg["llm_categoria"])

            fig_cat = px.bar(
                cat_agg, x="pct_acuerdo", y="categoria_label", orientation="h",
                color="pct_acuerdo", color_continuous_scale="RdYlGn",
                range_color=[0, 100],
                labels={"pct_acuerdo": "% Acuerdo", "categoria_label": ""},
                title="% de acuerdo baseline-LLM por categoría (en mensajes ODIO del LLM)",
            )
            fig_cat.update_layout(height=350, yaxis=dict(autorange="reversed"))
            _apply_horizontal_bar_labels(fig_cat)
            st.plotly_chart(fig_cat, use_container_width=True)

    render_section_exports(
        section_key="comparativa_modelos",
        section_title="Comparativa Baseline vs LLM",
        csv_items=[
            ("comparativa_raw", df),
            ("comparativa_filtrada", df_clean if "df_clean" in locals() else pd.DataFrame()),
            ("comparativa_categoria", cat_agg if "cat_agg" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Matriz de concordancia", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
            {"title": "Acuerdo por categoría", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
        ],
    )


def _calidad_llm_odio_metrics(df: pd.DataFrame) -> dict:
    """Métricas de accuracy odio sí/no y coincide_con_llm sobre un subconjunto."""
    total = len(df)
    if total == 0:
        return {"total": 0, "accuracy": 0.0, "coincide_llm": None}
    llm_odio = df["clasificacion_principal"] == "ODIO"
    h = df["humano_odio"]
    # Alineado con SQL: (clasif = 'ODIO') = (odio_flag = TRUE); NULL humano → no coincide
    coincide_odio = ((llm_odio & h.eq(True)) | (~llm_odio & h.eq(False))).sum()
    accuracy = coincide_odio / total * 100
    coincide_llm = None
    if df["coincide_con_llm"].notna().any():
        coincide_llm = int(df["coincide_con_llm"].sum())
    return {"total": total, "accuracy": accuracy, "coincide_llm": coincide_llm}


def _calidad_llm_cat_accuracy(df: pd.DataFrame) -> pd.DataFrame:
    """Accuracy por categoría (solo casos odio ∩ odio)."""
    llm_odio = df["clasificacion_principal"] == "ODIO"
    humano_odio = df["humano_odio"] == True  # noqa: E712
    df_cat = df[humano_odio & llm_odio].copy()
    if df_cat.empty:
        return pd.DataFrame()
    df_cat["coincide_cat"] = df_cat["categoria_odio_pred"] == df_cat["humano_categoria"]
    cat_acc = df_cat.groupby("humano_categoria").agg(
        total=("coincide_cat", "count"),
        aciertos=("coincide_cat", "sum"),
    ).reset_index()
    cat_acc["accuracy"] = (cat_acc["aciertos"] / cat_acc["total"] * 100).round(1)
    cat_acc["humano_categoria"] = (
        cat_acc["humano_categoria"].map(CATEGORIAS_LABELS).fillna(cat_acc["humano_categoria"])
    )
    return cat_acc


def _calidad_llm_platform_mask(df: pd.DataFrame, platform_key: str) -> pd.Series:
    if platform_key == "x":
        return df["platform"].isin(["x", "twitter"])
    return df["platform"] == platform_key


def _render_calidad_llm_cobertura(cobertura_df: pd.DataFrame, platform_key: Optional[str] = None) -> None:
    """Muestra validados / etiquetados LLM por plataforma."""
    if cobertura_df.empty:
        return

    rows = cobertura_df
    if platform_key:
        rows = cobertura_df[cobertura_df["platform"] == platform_key]

    for _, row in rows.iterrows():
        plat = row["platform"]
        etiquetados = int(row["etiquetados_llm"])
        validados = int(row["validados"])
        pct = validados / etiquetados * 100 if etiquetados else 0
        label = PLATFORM_DISPLAY.get(plat, plat)
        st.caption(
            f"**Cobertura {label}:** {validados:,} de {etiquetados:,} etiquetados LLM validados "
            f"— {pct:.0f} %"
        )


def render_calidad_llm():
    if not _require_role("admin", "editor", section="Calidad LLM"):
        return
    _render_section_header(
        "Calidad del etiquetado LLM",
        "Comparación entre la clasificación del LLM y la validación humana.",
    )

    annotators = load_annotators()

    fc_plat, fc_cat, fc_annot = st.columns([1, 1, 1])
    sel_plat = fc_plat.radio(
        "Plataforma",
        options=["Todas", "X", "YouTube"],
        horizontal=True,
        key="cal_plat",
    )
    if annotators:
        sel_cats = fc_cat.multiselect(
            "Categoría (humano)",
            options=list(CATEGORIAS_LABELS.keys()),
            format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
            default=[], key="cal_cat",
            placeholder="Todas las categorías",
        )
        sel_annot = fc_annot.multiselect(
            "Validador", annotators, default=[], key="cal_annot",
            placeholder="Seleccionar…",
        )
    else:
        sel_cats, sel_annot = [], []

    platform_filter: Optional[Tuple[str, ...]] = None
    if sel_plat == "X":
        platform_filter = ("x",)
    elif sel_plat == "YouTube":
        platform_filter = ("youtube",)

    df = load_calidad_llm(
        categorias=tuple(sel_cats) if sel_cats else None,
        annotators=tuple(sel_annot) if sel_annot else None,
        platforms=platform_filter,
    )
    cobertura_df = load_calidad_llm_cobertura()

    if df.empty:
        st.warning(
            "Aún no hay validaciones manuales cargadas en `processed.validaciones_manuales`. "
            "Cuando se importen las validaciones desde el Google Sheet, esta sección mostrará "
            "métricas de accuracy, precision y recall del LLM."
        )
        st.markdown("### Métricas que se mostrarán")
        st.markdown("""
        - **Accuracy global**: % de veces que el LLM coincide con el humano
        - **Precision por categoría**: de los que el LLM etiquetó como categoría X, cuántos acertó
        - **Recall por categoría**: de los que el humano marcó como categoría X, cuántos detectó el LLM
        - **Matriz de confusión**: LLM vs humano por categoría
        - **Evolución por versión**: si hay v1, v2... comparar mejoras
        """)
        return

    show_yt_note = sel_plat in ("Todas", "YouTube")
    show_yt_errors = sel_plat in ("Todas", "YouTube")

    st.markdown("### Cobertura de validación")
    if sel_plat == "Todas":
        _render_calidad_llm_cobertura(cobertura_df)
    elif sel_plat == "X":
        _render_calidad_llm_cobertura(cobertura_df, "x")
    else:
        _render_calidad_llm_cobertura(cobertura_df, "youtube")

    if show_yt_note:
        st.info(
            "**Nota metodológica (YouTube):** la muestra de validación es parcial "
            "(ver cobertura arriba). El indicador `coincide_con_llm` puede reflejar "
            "diferencias de flujo de anotación y no solo errores del modelo; "
            "auditoría y backfill pendientes (Fase 3)."
        )

    st.markdown("### Métricas de concordancia")

    if sel_plat == "Todas":
        df_x = df[_calidad_llm_platform_mask(df, "x")]
        df_yt = df[_calidad_llm_platform_mask(df, "youtube")]
        m_total = _calidad_llm_odio_metrics(df)
        m_x = _calidad_llm_odio_metrics(df_x)
        m_yt = _calidad_llm_odio_metrics(df_yt)

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Validaciones (total)", f"{m_total['total']:,}")
        c2.metric("Validaciones X", f"{m_x['total']:,}")
        c3.metric("Validaciones YouTube", f"{m_yt['total']:,}")
        c4.metric("Accuracy total (odio sí/no)", f"{m_total['accuracy']:.1f}%")

        c5, c6, c7, c8 = st.columns(4)
        c5.metric("Accuracy X (odio sí/no)", f"{m_x['accuracy']:.1f}%")
        c6.metric("Accuracy YouTube (odio sí/no)", f"{m_yt['accuracy']:.1f}%")
        c7.metric(
            "Coincide LLM (total)",
            f"{m_total['coincide_llm']:,}" if m_total["coincide_llm"] is not None else "N/A",
        )
        c8.metric(
            "Coincide LLM YouTube",
            f"{m_yt['coincide_llm']:,}" if m_yt["coincide_llm"] is not None else "N/A",
        )
    else:
        metrics = _calidad_llm_odio_metrics(df)
        col1, col2, col3 = st.columns(3)
        col1.metric("Validaciones", f"{metrics['total']:,}")
        col2.metric("Accuracy (odio sí/no)", f"{metrics['accuracy']:.1f}%")
        col3.metric(
            "Coincide con LLM",
            f"{metrics['coincide_llm']:,}" if metrics["coincide_llm"] is not None else "N/A",
        )

    st.markdown("### Coincidencia por categoría")
    cat_acc = pd.DataFrame()
    fig = None

    if sel_plat == "Todas":
        parts = []
        for plat_key, plat_label, plat_df in [
            ("x", "X", df[_calidad_llm_platform_mask(df, "x")]),
            ("youtube", "YouTube", df[_calidad_llm_platform_mask(df, "youtube")]),
        ]:
            ca = _calidad_llm_cat_accuracy(plat_df)
            if not ca.empty:
                ca = ca.copy()
                ca["plataforma"] = plat_label
                parts.append(ca)
        if parts:
            cat_acc = pd.concat(parts, ignore_index=True)
            fig = px.bar(
                cat_acc, x="accuracy", y="humano_categoria", color="plataforma",
                orientation="h", barmode="group",
                color_discrete_map={"X": COLORS["primary"], "YouTube": COLORS["accent"]},
                labels={"accuracy": "Accuracy %", "humano_categoria": "", "plataforma": "Plataforma"},
                title="Accuracy del LLM por categoría (vs validación humana)",
            )
            fig.update_layout(height=400, yaxis=dict(autorange="reversed"))
            _apply_horizontal_bar_labels(fig)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No hay casos odio ∩ odio para calcular accuracy por categoría.")
    else:
        cat_acc = _calidad_llm_cat_accuracy(df)
        if not cat_acc.empty:
            fig = px.bar(
                cat_acc, x="accuracy", y="humano_categoria", orientation="h",
                color="accuracy", color_continuous_scale="RdYlGn",
                range_color=[0, 100],
                labels={"accuracy": "Accuracy %", "humano_categoria": ""},
                title="Accuracy del LLM por categoría (vs validación humana)",
            )
            fig.update_layout(height=350, yaxis=dict(autorange="reversed"))
            _apply_horizontal_bar_labels(fig)
            st.plotly_chart(fig, use_container_width=True)

    if show_yt_errors:
        with st.expander("Análisis de errores — YouTube", expanded=False):
            _render_vllm_yt_error_analysis()

    render_section_exports(
        section_key="calidad_llm",
        section_title="Calidad del etiquetado LLM",
        csv_items=[
            ("validaciones", df),
            ("accuracy_categoria", cat_acc if not cat_acc.empty else pd.DataFrame()),
            ("cobertura", cobertura_df),
        ],
        fig_items=[
            {"title": "Accuracy por categoría", "fig": fig, "kind": "plotly"},
        ],
    )


def render_terminos():
    _render_section_header(
        "Términos de odio más frecuentes",
        "Términos detectados en mensajes candidatos a odio; por defecto se filtran lemas neutros o genéricos.",
    )

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2, fc3, fc4, fc5 = st.columns([1, 1, 1, 1, 1])
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="term_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="term_med",
        placeholder="Todos los medios",
    )
    sel_cats = fc3.multiselect(
        "Categoría de odio",
        options=list(CATEGORIAS_LABELS.keys()),
        format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
        default=[], key="term_cat",
        placeholder="Todas las categorías",
    )
    PERIODO_OPTIONS = {"Todo": None, "24 hs": 24, "48 hs": 48, "72 hs": 72}
    sel_periodo = fc4.selectbox(
        "Período", options=list(PERIODO_OPTIONS.keys()), index=0, key="term_periodo",
    )
    solo_candidatos = fc5.checkbox(
        "Solo candidatos a odio",
        value=True,
        key="term_cand",
        help="Incluye mensajes con candidato a odio o con coincidencia en el lexicón (útil para YouTube).",
    )

    filtro_neutros = st.checkbox(
        "Ocultar términos neutros / genéricos (lista oficial)",
        value=True,
        key="term_filtro_neutros",
        help=(
            "Excluye lemas definidos en el repositorio (terminos_exclusion_oficial.py). "
            "Para ampliar la lista: JSON + sync en automatizacion_diaria."
        ),
    )
    st.caption(
        "**Período:** usa la fecha de **ingreso al sistema** (`processed_at`), no solo la publicación del mensaje, "
        "para que comentarios de YouTube recién cargados aparezcan en 24/48/72 h."
    )

    df = load_terminos(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats) if sel_cats else None,
        solo_candidatos=solo_candidatos,
        ultimas_horas=PERIODO_OPTIONS[sel_periodo],
    )

    if df.empty:
        st.warning("No hay términos detectados con los filtros seleccionados.")
        return

    all_terms: List[str] = []
    for terms_raw in df["matched_terms"]:
        all_terms.extend(_parse_and_normalize_matched_terms(terms_raw))

    counter = Counter(all_terms)
    n_tokens_antes = len(counter)
    exclude = load_terminos_exclusion_set() if filtro_neutros else frozenset()
    if filtro_neutros and len(exclude) == 0:
        st.warning(
            "La lista de exclusiones oficial está vacía. Revisa el despliegue de `terminos_exclusion_oficial.py`."
        )
    if filtro_neutros:
        counter = _filter_counter_terminos_neutros(counter, exclude)
    if not counter:
        st.warning(
            "No quedan términos tras aplicar el filtro. "
            "Desactiva «Ocultar términos neutros» o amplía plataforma/medio/período."
        )
        return
    if filtro_neutros and n_tokens_antes:
        st.caption(
            f"Términos distintos: {len(counter):,} tras filtro ({n_tokens_antes:,} antes; "
            f"{len(exclude):,} lemas en lista oficial)."
        )

    _nc = len(counter)
    _max_n = min(50, max(1, _nc))
    _min_n = min(10, _max_n)
    top_n = st.slider(
        "Cantidad de términos",
        _min_n,
        _max_n,
        min(25, _max_n),
        key="term_topn",
    )
    top_terms = counter.most_common(top_n)

    col1, col2 = st.columns([1, 1])

    with col1:
        df_terms = pd.DataFrame(top_terms, columns=["Término", "Frecuencia"])
        fig = px.bar(
            df_terms, x="Frecuencia", y="Término", orientation="h",
            color="Frecuencia",
            color_continuous_scale=[[0, "#FFF5F5"], [0.5, "#F56565"], [1, "#C0392B"]],
            title=f"Top {top_n} términos más frecuentes",
        )
        fig.update_layout(height=max(400, top_n * 22), yaxis=dict(autorange="reversed"), showlegend=False)
        _apply_horizontal_bar_labels(fig)
        st.plotly_chart(fig, use_container_width=True)

    with col2:
        if counter:
            wc = WordCloud(
                width=800, height=500, background_color="white",
                colormap="Reds", max_words=top_n, min_font_size=10,
            ).generate_from_frequencies(dict(counter))

            fig_wc, ax = plt.subplots(figsize=(10, 6))
            ax.imshow(wc, interpolation="bilinear")
            ax.axis("off")
            st.pyplot(fig_wc)

    st.markdown("### Detalle")
    df_all = pd.DataFrame(counter.most_common(100), columns=["Término", "Frecuencia"])
    st.dataframe(df_all, use_container_width=True, hide_index=True)

    render_section_exports(
        section_key="terminos_frecuentes",
        section_title="Términos de odio más frecuentes",
        csv_items=[
            ("terminos_top", df_terms if "df_terms" in locals() else pd.DataFrame()),
            ("terminos_detalle", df_all if "df_all" in locals() else pd.DataFrame()),
            ("mensajes_filtrados", df),
        ],
        fig_items=[
            {"title": "Top términos frecuentes", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
            {"title": "Nube de palabras", "fig": fig_wc if "fig_wc" in locals() else None, "kind": "matplotlib"},
        ],
    )


# ============================================================
# SECCIÓN: DATASET GOLD
# ============================================================


_INTENSIDAD_LABELS_GUEST = {
    1: "1 — Leve",
    2: "2 — Ofensivo",
    3: "3 — Hostil / Incitación",
}


def render_dataset_validado_guest() -> None:
    """Versión simplificada del dataset gold para el perfil visualizador/invitado."""
    _render_section_header(
        "Dataset validado",
        "Mensajes revisados manualmente por el proyecto ReTo.",
    )

    st.markdown(
        "Esta sección muestra el conjunto de mensajes revisados manualmente "
        "por el proyecto ReTo. La validación humana permite contrastar y mejorar "
        "el análisis automatizado de la plataforma, aportando criterios homogéneos, "
        "trazabilidad y mayor rigor metodológico."
    )

    df = load_gold_full()
    if df.empty:
        st.info("Todavía no hay mensajes validados disponibles para mostrar.")
        return

    total = int(len(df))
    n_odio = int((df["y_odio_bin"] == 1).sum())
    n_no_odio = int((df["y_odio_final"] == "No Odio").sum())
    n_dudoso = int((df["y_odio_final"] == "Dudoso").sum())

    def _pct(value: int) -> str:
        return f"{value / total * 100:.1f}%" if total else "0%"

    plataformas_presentes = sorted(
        [p for p in df["platform_label"].dropna().unique() if str(p).strip()]
    )
    plataformas_str = " · ".join(plataformas_presentes) if plataformas_presentes else "—"
    n_categorias = len(CATEGORIAS_LABELS)

    st.markdown(f"""
<style>
.guest-metric-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin: 8px 0 24px 0;
}}
.guest-metric-card {{
    background-color: #1B3A6B;
    border-radius: 12px;
    padding: 18px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    min-height: 124px;
}}
.guest-metric-card .label {{
    font-size: 13px;
    font-weight: 400;
    opacity: 0.85;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.guest-metric-card .value {{
    font-size: 26px;
    font-weight: 700;
    line-height: 1.1;
}}
.guest-metric-card .sub {{
    font-size: 12px;
    opacity: 0.75;
    margin-top: 6px;
}}
</style>

<div class="guest-metric-grid">
  <div class="guest-metric-card">
    <div class="label">Mensajes validados manualmente</div>
    <div class="value">{total:,}</div>
  </div>
  <div class="guest-metric-card">
    <div class="label">Mensajes clasificados como odio</div>
    <div class="value">{n_odio:,}</div>
    <div class="sub">{_pct(n_odio)} del total</div>
  </div>
  <div class="guest-metric-card">
    <div class="label">Mensajes no odio</div>
    <div class="value">{n_no_odio:,}</div>
    <div class="sub">{_pct(n_no_odio)} del total</div>
  </div>
  <div class="guest-metric-card">
    <div class="label">Mensajes dudosos</div>
    <div class="value">{n_dudoso:,}</div>
    <div class="sub">{_pct(n_dudoso)} del total</div>
  </div>
  <div class="guest-metric-card">
    <div class="label">Plataformas analizadas</div>
    <div class="value">{plataformas_str}</div>
  </div>
  <div class="guest-metric-card">
    <div class="label">Categorías monitorizadas</div>
    <div class="value">{n_categorias}</div>
    <div class="sub">categorías de odio</div>
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown("---")

    # A) Distribución del etiquetado humano
    st.markdown("### Distribución del etiquetado humano")
    odio_counts = (
        df["y_odio_final"].dropna().value_counts().rename_axis("Etiqueta")
        .reset_index(name="Mensajes")
    )
    fig_dist = px.pie(
        odio_counts,
        names="Etiqueta",
        values="Mensajes",
        color="Etiqueta",
        color_discrete_map=SEMANTIC_COLORS,
        hole=0.4,
    )
    fig_dist.update_traces(textinfo="label+percent")
    fig_dist.update_layout(height=380, showlegend=True)
    st.plotly_chart(fig_dist, use_container_width=True)

    st.markdown("---")

    # B) Categorías detectadas en los mensajes de odio
    st.markdown("### Categorías detectadas en los mensajes de odio")
    df_odio = df[df["y_odio_bin"] == 1].copy()
    if df_odio.empty or df_odio["y_categoria_final"].dropna().empty:
        st.info("Aún no hay categorías de odio registradas en los mensajes validados.")
    else:
        cat_counts = (
            df_odio["y_categoria_final"].dropna().value_counts()
            .rename_axis("categoria").reset_index(name="Mensajes")
        )
        cat_counts["Categoría"] = cat_counts["categoria"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x)
        )
        cat_counts = cat_counts.sort_values("Mensajes", ascending=True)
        fig_cat = px.bar(
            cat_counts,
            x="Mensajes",
            y="Categoría",
            orientation="h",
            color="Categoría",
            color_discrete_map=CAT_COLOR_MAP,
            text_auto=True,
        )
        fig_cat.update_layout(
            height=420, showlegend=False, yaxis=dict(autorange="reversed"),
            xaxis_title="Mensajes", yaxis_title="",
        )
        _apply_horizontal_bar_labels(fig_cat)
        st.plotly_chart(fig_cat, use_container_width=True)

    st.markdown("---")

    # C) Nivel de intensidad de los mensajes de odio
    st.markdown("### Nivel de intensidad de los mensajes de odio")
    if df_odio.empty or df_odio["y_intensidad_final"].dropna().empty:
        st.info("Aún no hay datos de intensidad para los mensajes de odio validados.")
    else:
        int_series = pd.to_numeric(
            df_odio["y_intensidad_final"], errors="coerce"
        ).dropna().astype(int)
        int_counts = (
            int_series.value_counts().sort_index()
            .rename_axis("nivel").reset_index(name="Mensajes")
        )
        int_counts["Intensidad"] = int_counts["nivel"].map(
            _INTENSIDAD_LABELS_GUEST
        ).fillna(int_counts["nivel"].astype(str))
        fig_int = px.bar(
            int_counts,
            x="Intensidad",
            y="Mensajes",
            color="Intensidad",
            color_discrete_map={
                _INTENSIDAD_LABELS_GUEST[1]: "#F39C12",
                _INTENSIDAD_LABELS_GUEST[2]: "#E67E22",
                _INTENSIDAD_LABELS_GUEST[3]: "#E74C3C",
            },
            text_auto=True,
        )
        fig_int.update_layout(height=360, showlegend=False, xaxis_title="", yaxis_title="Mensajes")
        st.plotly_chart(fig_int, use_container_width=True)

        # D) Intensidad por categoría de odio
        st.markdown("### Intensidad por categoría de odio")
        df_cat_int = df_odio.dropna(subset=["y_categoria_final", "y_intensidad_final"]).copy()
        if df_cat_int.empty:
            st.info("No hay suficiente información para cruzar categoría e intensidad.")
        else:
            df_cat_int["y_intensidad_final"] = pd.to_numeric(
                df_cat_int["y_intensidad_final"], errors="coerce"
            )
            df_cat_int = df_cat_int.dropna(subset=["y_intensidad_final"])
            df_cat_int["Categoría"] = df_cat_int["y_categoria_final"].map(
                lambda x: CATEGORIAS_LABELS.get(x, x)
            )
            df_cat_int["Intensidad"] = (
                df_cat_int["y_intensidad_final"].astype(int)
                .map(_INTENSIDAD_LABELS_GUEST)
            )
            agg = (
                df_cat_int.groupby(["Categoría", "Intensidad"])
                .size()
                .reset_index(name="Mensajes")
            )
            fig_int_cat = px.bar(
                agg,
                x="Categoría",
                y="Mensajes",
                color="Intensidad",
                barmode="stack",
                color_discrete_map={
                    _INTENSIDAD_LABELS_GUEST[1]: "#F39C12",
                    _INTENSIDAD_LABELS_GUEST[2]: "#E67E22",
                    _INTENSIDAD_LABELS_GUEST[3]: "#E74C3C",
                },
            )
            fig_int_cat.update_layout(
                height=420, xaxis_tickangle=-25, xaxis_title="", yaxis_title="Mensajes",
            )
            st.plotly_chart(fig_int_cat, use_container_width=True)
            st.caption(
                "No todos los mensajes tienen la misma gravedad: la intensidad permite "
                "diferenciar tonos leves de incitaciones hostiles."
            )

    st.markdown("---")

    # E) Mensajes validados por plataforma
    st.markdown("### Mensajes validados por plataforma")
    plat_counts = (
        df["platform_label"].dropna().value_counts()
        .rename_axis("Plataforma").reset_index(name="Mensajes")
    )
    if plat_counts.empty:
        st.info("Sin datos de plataforma en los mensajes validados.")
    else:
        fig_plat = px.bar(
            plat_counts,
            x="Plataforma",
            y="Mensajes",
            color="Plataforma",
            color_discrete_map=PLATFORM_COLORS,
            text_auto=True,
        )
        fig_plat.update_layout(height=340, showlegend=False, xaxis_title="", yaxis_title="Mensajes")
        st.plotly_chart(fig_plat, use_container_width=True)

    # F) Porcentaje de mensajes de odio por plataforma
    st.markdown("### Porcentaje de mensajes de odio por plataforma")
    if plat_counts.empty:
        st.info("Sin datos de plataforma para calcular el porcentaje de odio.")
    else:
        plat_pct = (
            df.dropna(subset=["platform_label"])
            .groupby("platform_label")["y_odio_bin"].mean()
            .mul(100).round(1)
            .reset_index()
            .rename(columns={"platform_label": "Plataforma", "y_odio_bin": "% Odio"})
        )
        fig_pct = px.bar(
            plat_pct,
            x="Plataforma",
            y="% Odio",
            color="Plataforma",
            color_discrete_map=PLATFORM_COLORS,
            text="% Odio",
        )
        fig_pct.update_traces(texttemplate="%{text:.1f}%", textposition="outside")
        fig_pct.update_layout(
            height=340, showlegend=False, xaxis_title="",
            yaxis_title="% mensajes de odio",
            yaxis_range=[0, max(100, float(plat_pct["% Odio"].max()) * 1.1) if not plat_pct.empty else 100],
        )
        st.plotly_chart(fig_pct, use_container_width=True)


def render_gold_dataset_router() -> None:
    """Despacha a la vista simplificada para el perfil visualizador, o a la completa para el resto."""
    if _is_viewer():
        render_dataset_validado_guest()
    else:
        render_gold_dataset()


# ============================================================
# SECCIÓN: ANÁLISIS ART. 510 — Potenciales delitos de odio
# ============================================================


def _clean_api_key(raw: str) -> str:
    """Elimina caracteres invisibles/non-ASCII de una API key."""
    return (raw or "").encode("ascii", errors="ignore").decode("ascii").strip()


def _get_openai_api_key() -> str:
    """Intenta obtener la API key de OpenAI desde múltiples fuentes."""
    import os as _os

    # Probar todas las variantes posibles en st.secrets
    for key_name in ("OPENAI_API_KEY", "openai_api_key", "api_key"):
        try:
            key = str(st.secrets[key_name])
            if key and key.startswith("sk"):
                return _clean_api_key(key)
        except Exception:
            pass

    # Probar secciones en st.secrets
    for section in ("openai", "OPENAI"):
        for sub_key in ("api_key", "API_KEY", "OPENAI_API_KEY"):
            try:
                key = str(st.secrets[section][sub_key])
                if key and key.startswith("sk"):
                    return _clean_api_key(key)
            except Exception:
                pass

    # Variable de entorno
    key = (_os.environ.get("OPENAI_API_KEY") or "").strip()
    if key:
        return _clean_api_key(key)

    return ""


APARTADO_LABELS = {
    "1a": "Art. 510.1a — Incitación",
    "1b": "Art. 510.1b — Distribución material",
    "1c": "Art. 510.1c — Negación/trivialización",
}


ART510_COLORS = {
    "1a": "#E74C3C",
    "1b": "#3498DB",
    "1c": "#F39C12",
}

# ── Prompt y lógica de evaluación LLM Art. 510 ──

_ART510_SYSTEM = (
    "Eres un analista jurídico especializado en delitos de odio del Código "
    "Penal español. Tu tarea es evaluar si un mensaje de redes sociales "
    "constituye potencialmente un delito conforme al artículo 510, apartado 1 "
    "del Código Penal. Devuelves SOLO JSON válido, sin texto extra."
)

_ART510_USER_TMPL = """Analiza el siguiente mensaje y determina si podría constituir un delito según el artículo 510.1 del Código Penal español.

ARTÍCULO 510.1 — Conductas tipificadas (SOLO evaluar estas):

a) Fomentar, promover o incitar directa o indirectamente al odio, hostilidad, discriminación o violencia contra un grupo o persona por razón de su pertenencia a un grupo protegido.

b) Producir, elaborar, poseer con finalidad de distribuir, facilitar el acceso, distribuir, difundir o vender escritos o material idóneo para fomentar, promover o incitar al odio, hostilidad, discriminación o violencia contra grupos protegidos.

c) Negar, trivializar gravemente o enaltecer los delitos de genocidio, de lesa humanidad o contra personas y bienes protegidos en caso de conflicto armado, o enaltecer a sus autores, cuando se promueva o favorezca un clima de violencia, hostilidad, odio o discriminación.

GRUPOS PROTEGIDOS (Art. 510): raza, antisemitismo, antigitanismo, ideología, religión, creencias, situación familiar, etnia, nación, origen nacional, sexo, orientación sexual, identidad sexual, género, aporofobia, enfermedad, discapacidad.

IMPORTANTE: NO evaluar bajo el apartado 2 del Art. 510 (lesiones a la dignidad por humillación, menosprecio o descrédito). Solo el apartado 1.

Devuelve SOLO un JSON válido con EXACTAMENTE estas claves:
- es_potencial_delito: true o false
- apartado_510: "1a", "1b" o "1c" (vacío si no es delito)
- grupo_protegido: el grupo protegido específico afectado (vacío si no es delito)
- conducta_detectada: descripción breve de la conducta tipificada (vacío si no es delito)
- justificacion: 1-2 frases breves explicando tu razonamiento
- confianza: "alta", "media" o "baja"

MENSAJE:
{txt}
"""

_ART510_APARTADOS_VALIDOS = {"1a", "1b", "1c"}
_ART510_CONFIANZA_VALIDOS = {"alta", "media", "baja"}

_MAX_FEEDBACK_EXAMPLES = 15


@st.cache_data(ttl=600)
def _art510_load_feedback_examples() -> str:
    """Carga correcciones y rechazos humanos como bloque few-shot para el prompt.

    Prioriza rechazos (falsos positivos) y correcciones (apartado/grupo incorrecto)
    porque son los errores más valiosos de los que el LLM puede aprender.
    Devuelve un string listo para inyectar en el prompt, o cadena vacía si no hay feedback.
    """
    import json as _json

    query = """
        SELECT pm.content_original,
               ea.es_potencial_delito  AS llm_delito,
               ea.apartado_510         AS llm_apartado,
               ea.grupo_protegido      AS llm_grupo,
               ea.conducta_detectada   AS llm_conducta,
               v.validacion_humana,
               v.apartado_510_final,
               v.grupo_protegido_final,
               v.conducta_final,
               v.comentario
        FROM processed.validacion_art510_humana v
        JOIN processed.evaluacion_art510 ea
             USING (message_uuid, label_source)
        JOIN processed.mensajes pm
             USING (message_uuid)
        WHERE v.validacion_humana IN ('rechazado', 'corregido')
        ORDER BY v.annotation_date DESC
        LIMIT %s
    """
    try:
        with get_conn() as conn:
            df = pd.read_sql(query, conn, params=[_MAX_FEEDBACK_EXAMPLES * 2])
    except Exception:
        return ""

    if df.empty:
        return ""

    rejected = df[df["validacion_humana"] == "rechazado"]
    corrected = df[df["validacion_humana"] == "corregido"]

    examples = []

    for _, row in rejected.head(_MAX_FEEDBACK_EXAMPLES // 2).iterrows():
        msg_preview = str(row["content_original"])[:200]
        examples.append(
            f"EJEMPLO (FALSO POSITIVO — el LLM clasificó como delito pero NO lo es):\n"
            f"Mensaje: \"{msg_preview}\"\n"
            f"LLM dijo: delito={row['llm_delito']}, apartado={row['llm_apartado']}, "
            f"grupo={row['llm_grupo']}\n"
            f"Corrección humana: NO es delito."
            + (f" Motivo: {row['comentario']}" if row.get("comentario") else "")
        )

    for _, row in corrected.head(_MAX_FEEDBACK_EXAMPLES - len(examples)).iterrows():
        msg_preview = str(row["content_original"])[:200]
        examples.append(
            f"EJEMPLO (CORRECCIÓN — el LLM clasificó incorrectamente):\n"
            f"Mensaje: \"{msg_preview}\"\n"
            f"LLM dijo: apartado={row['llm_apartado']}, grupo={row['llm_grupo']}, "
            f"conducta={row['llm_conducta']}\n"
            f"Corrección humana: apartado={row['apartado_510_final']}, "
            f"grupo={row['grupo_protegido_final']}, conducta={row['conducta_final']}"
            + (f" Nota: {row['comentario']}" if row.get("comentario") else "")
        )

    if not examples:
        return ""

    header = (
        "\n\n--- FEEDBACK DE VALIDACIONES HUMANAS ---\n"
        "Los siguientes son errores detectados por validadores humanos en evaluaciones "
        "anteriores. Úsalos para calibrar tu criterio y evitar errores similares:\n\n"
    )
    return header + "\n\n".join(examples) + "\n--- FIN FEEDBACK ---\n"


def _art510_extract_json(text: str) -> dict:
    """Extrae JSON del output del LLM de forma robusta."""
    import json as _json
    t = (text or "").strip()
    if t.startswith("```"):
        t = t.replace("```json", "").replace("```JSON", "").replace("```", "").strip()
    t = t.translate({
        ord("\u201C"): ord('"'), ord("\u201D"): ord('"'),
        ord("\u2018"): ord("'"), ord("\u2019"): ord("'"),
    })
    if not t.startswith("{"):
        a, b = t.find("{"), t.rfind("}")
        if a != -1 and b != -1 and b > a:
            t = t[a:b + 1]
    return _json.loads(t)


def _art510_eval_single(client, model: str, txt: str, feedback: str = "") -> dict:
    """Evalúa un mensaje bajo Art. 510.1 y devuelve dict normalizado.

    Args:
        feedback: bloque de ejemplos few-shot generado por _art510_load_feedback_examples().

    Raises:
        openai.AuthenticationError (re-raised to stop the batch).
    """
    _fallback = {
        "es_potencial_delito": False, "apartado_510": "",
        "grupo_protegido": "", "conducta_detectada": "",
        "justificacion": "Error en la evaluación", "confianza": "baja",
    }

    for attempt in range(2):
        user_content = _ART510_USER_TMPL.format(txt=txt)
        if feedback:
            user_content = user_content + feedback
        if attempt > 0:
            user_content = "IMPORTANTE: devolvé SOLO JSON válido. Sin texto extra.\n\n" + user_content

        try:
            resp = client.responses.create(
                model=model,
                input=[
                    {"role": "system", "content": _ART510_SYSTEM},
                    {"role": "user", "content": user_content},
                ],
            )
        except Exception as api_err:
            err_name = type(api_err).__name__
            if "AuthenticationError" in err_name or "PermissionDenied" in err_name:
                raise
            if attempt == 1:
                _fallback["justificacion"] = f"Error API: {err_name}"
                obj = _fallback
                break
            continue

        raw = getattr(resp, "output_text", "") or ""
        try:
            obj = _art510_extract_json(raw)
            break
        except Exception:
            if attempt == 1:
                obj = {
                    "es_potencial_delito": False,
                    "apartado_510": "", "grupo_protegido": "",
                    "conducta_detectada": "",
                    "justificacion": "Error de parseo JSON",
                    "confianza": "baja",
                }

    es_delito = str(obj.get("es_potencial_delito", False)).lower() in ("true", "1", "si", "sí", "yes")
    apartado = str(obj.get("apartado_510", "")).strip().lower()
    if apartado not in _ART510_APARTADOS_VALIDOS:
        apartado = ""
    confianza = str(obj.get("confianza", "baja")).strip().lower()
    if confianza not in _ART510_CONFIANZA_VALIDOS:
        confianza = "baja"

    return {
        "es_potencial_delito": es_delito,
        "apartado_510": apartado if es_delito else "",
        "grupo_protegido": str(obj.get("grupo_protegido", "")).strip() if es_delito else "",
        "conducta_detectada": str(obj.get("conducta_detectada", "")).strip() if es_delito else "",
        "justificacion": str(obj.get("justificacion", "")).strip(),
        "confianza": confianza,
    }


def _art510_get_already_evaluated() -> set:
    """Devuelve el set de claves 'uuid|label_source' ya evaluadas en BD."""
    try:
        with get_conn() as conn:
            df = pd.read_sql(
                "SELECT message_uuid, label_source FROM processed.evaluacion_art510",
                conn,
            )
        return set(df["message_uuid"].astype(str) + "|" + df["label_source"].astype(str))
    except Exception:
        return set()


def _art510_ensure_tables():
    """Crea las tablas Art. 510 si no existen."""
    ddl = """
    CREATE TABLE IF NOT EXISTS processed.evaluacion_art510 (
        message_uuid        UUID        NOT NULL,
        label_source        VARCHAR(20) NOT NULL,
        es_potencial_delito BOOLEAN     NOT NULL,
        apartado_510        VARCHAR(5),
        grupo_protegido     VARCHAR(100),
        conducta_detectada  VARCHAR(100),
        justificacion       TEXT,
        confianza           VARCHAR(10),
        evaluacion_date     TIMESTAMPTZ DEFAULT NOW(),
        llm_version         VARCHAR(50) DEFAULT 'v1',
        PRIMARY KEY (message_uuid, label_source)
    );
    CREATE TABLE IF NOT EXISTS processed.validacion_art510_humana (
        message_uuid            UUID        NOT NULL,
        label_source            VARCHAR(20) NOT NULL,
        validacion_humana       VARCHAR(20) NOT NULL,
        apartado_510_final      VARCHAR(5),
        grupo_protegido_final   VARCHAR(100),
        conducta_final          VARCHAR(100),
        comentario              TEXT,
        annotator_id            VARCHAR(50) NOT NULL,
        annotation_date         DATE        NOT NULL,
        PRIMARY KEY (message_uuid, label_source)
    );
    """
    alter_ddl = """
    DO $$ BEGIN
        ALTER TABLE processed.evaluacion_art510
            ALTER COLUMN grupo_protegido TYPE VARCHAR(500),
            ALTER COLUMN conducta_detectada TYPE VARCHAR(500);
    EXCEPTION WHEN others THEN NULL;
    END $$;
    DO $$ BEGIN
        ALTER TABLE processed.validacion_art510_humana
            ALTER COLUMN grupo_protegido_final TYPE VARCHAR(500),
            ALTER COLUMN conducta_final TYPE VARCHAR(500);
    EXCEPTION WHEN others THEN NULL;
    END $$;
    """
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(ddl)
            cur.execute(alter_ddl)
            cur.close()
    except Exception as e:
        st.error(f"Error creando tablas Art. 510: {e}")


def _art510_save_batch(results: list) -> int:
    """Guarda un lote de resultados en processed.evaluacion_art510.

    Returns:
        Número de filas guardadas con éxito, 0 si hubo error.
    """
    if not results:
        return 0

    def _trunc(val, maxlen):
        if val and len(str(val)) > maxlen:
            return str(val)[:maxlen]
        return val or None

    columns = [
        "message_uuid", "label_source", "es_potencial_delito", "apartado_510",
        "grupo_protegido", "conducta_detectada", "justificacion", "confianza",
        "llm_version",
    ]
    rows = []
    for r in results:
        rows.append((
            r["message_uuid"], _trunc(r["label_source"], 20),
            r["es_potencial_delito"],
            _trunc(r.get("apartado_510"), 5),
            _trunc(r.get("grupo_protegido"), 200),
            _trunc(r.get("conducta_detectada"), 200),
            r.get("justificacion") or None,
            _trunc(r.get("confianza"), 10),
            "v1",
        ))
    try:
        with get_conn() as conn:
            from db_utils import upsert_rows as _upsert
            _upsert(
                conn, "processed.evaluacion_art510", columns, rows,
                conflict_columns=["message_uuid", "label_source"],
                update_columns=[c for c in columns if c not in ("message_uuid", "label_source")],
            )
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM processed.evaluacion_art510")
            total_db = cur.fetchone()[0]
            cur.close()
        return total_db
    except Exception as e:
        st.warning(f"Error guardando lote: {e}")
        return 0


@st.cache_data(ttl=300)
def load_art510_data(
    platforms: Optional[Tuple] = None,
    label_sources: Optional[Tuple] = None,
    solo_delitos: bool = True,
) -> pd.DataFrame:
    """Carga datos de evaluación Art. 510 con filtros."""
    conditions = []
    params: list = []
    platforms = _expand_platforms(list(platforms) if platforms else None)

    if solo_delitos:
        conditions.append("ea.es_potencial_delito = TRUE")

    if platforms:
        conditions.append("pm.platform IN %s")
        params.append(tuple(platforms))

    if label_sources:
        conditions.append("ea.label_source IN %s")
        params.append(tuple(label_sources))

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT ea.message_uuid,
                   ea.label_source,
                   ea.es_potencial_delito,
                   ea.apartado_510,
                   ea.grupo_protegido,
                   ea.conducta_detectada,
                   ea.justificacion,
                   ea.confianza,
                   ea.evaluacion_date,
                   pm.platform,
                   pm.content_original,
                   pm.source_media
            FROM processed.evaluacion_art510 ea
            JOIN processed.mensajes pm USING (message_uuid)
            {where}
            ORDER BY ea.evaluacion_date DESC
        """, conn, params=params if params else None)

    if not df.empty:
        df["platform_label"] = df["platform"].map(platform_label)
        df["source_label"] = df["label_source"].map(
            lambda x: LABEL_SOURCE_LABELS.get(x, x)
        )
        df["apartado_label"] = df["apartado_510"].map(
            lambda x: APARTADO_LABELS.get(x, x) if pd.notna(x) and x else "Sin apartado"
        )

    return df


def _render_art510_preview(sel_platforms, sel_sources):
    """Vista previa de candidatos Art. 510 basada en datos existentes."""
    st.info(
        "**Modo vista previa** — Se muestran mensajes etiquetados como ODIO "
        "cuyas categorías corresponden a grupos protegidos del Art. 510.1. "
        "Usa el botón de abajo para ejecutar la evaluación jurídica con LLM."
    )

    df = load_art510_candidates(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        label_sources=tuple(sel_sources) if sel_sources else None,
    )

    if df.empty:
        st.warning("No hay candidatos Art. 510 con los filtros seleccionados.")
        return

    # ── KPIs ──
    st.markdown("---")
    st.markdown("### Candidatos a evaluación Art. 510")

    total = len(df)
    n_llm = (df["label_source"] == "llm").sum()
    n_human = (df["label_source"] == "humano").sum()
    n_int3 = (df["intensidad"].astype(str) == "3").sum()

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Total candidatos", f"{total:,}")
    k2.metric("Por LLM", f"{n_llm:,}")
    k3.metric("Por humanos", f"{n_human:,}")
    k4.metric("Intensidad 3 (hostil)", f"{n_int3:,}")

    # ── Gráficos ──
    st.markdown("---")
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        cat_counts = (
            df["grupo_protegido_estimado"]
            .value_counts()
            .reset_index()
        )
        cat_counts.columns = ["Grupo protegido (estimado)", "Cantidad"]
        fig_cat = px.pie(
            cat_counts, names="Grupo protegido (estimado)", values="Cantidad",
            title="Candidatos por grupo protegido Art. 510",
            hole=0.4,
            color_discrete_sequence=CAT_COLORS,
        )
        fig_cat.update_layout(height=400)
        st.plotly_chart(fig_cat, use_container_width=True)

    with col_g2:
        if len(df["platform_label"].unique()) > 0 and len(df["source_label"].unique()) > 0:
            grouped = (
                df.groupby(["platform_label", "source_label"])
                .size()
                .reset_index(name="Cantidad")
            )
            fig_gr = px.bar(
                grouped, x="platform_label", y="Cantidad",
                color="source_label",
                barmode="group",
                title="Candidatos por plataforma y fuente",
                labels={"platform_label": "Plataforma", "source_label": "Fuente"},
                color_discrete_map={
                    "Etiquetado LLM": COLORS["accent"],
                    "Etiquetado humano": COLORS["success"],
                },
            )
            fig_gr.update_layout(height=400)
            st.plotly_chart(fig_gr, use_container_width=True)

    # ── Tabla pivot ──
    st.markdown("---")
    st.markdown("### Vista agrupada")
    pivot = pd.crosstab(
        df["platform_label"],
        df["source_label"],
        margins=True,
        margins_name="Total",
    )
    st.dataframe(pivot, use_container_width=True)

    # ── Intensidad ──
    int_counts = (
        df["intensidad"]
        .astype(str)
        .value_counts()
        .reindex(["1", "2", "3"], fill_value=0)
        .reset_index()
    )
    int_counts.columns = ["Intensidad", "Cantidad"]
    int_labels = {"1": "Leve", "2": "Ofensivo", "3": "Hostil/Incitación"}
    int_counts["Nivel"] = int_counts["Intensidad"].map(int_labels)
    fig_int = px.bar(
        int_counts, x="Nivel", y="Cantidad",
        color="Nivel",
        color_discrete_map={
            "Leve": COLORS["muted"],
            "Ofensivo": COLORS["warning"],
            "Hostil/Incitación": COLORS["danger"],
        },
        title="Distribución por intensidad (los de intensidad 3 son los más relevantes para Art. 510)",
    )
    fig_int.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_int, use_container_width=True)

    # ── Tabla detalle ──
    st.markdown("---")
    st.markdown("### Detalle de candidatos")
    display_cols = [
        "content_original", "platform_label", "source_label",
        "categoria_label", "grupo_protegido_estimado", "intensidad",
        "motivo_etiquetado",
    ]
    rename_map = {
        "content_original": "Mensaje",
        "platform_label": "Plataforma",
        "source_label": "Fuente",
        "categoria_label": "Categoría de odio",
        "grupo_protegido_estimado": "Grupo protegido (Art. 510)",
        "intensidad": "Intensidad",
        "motivo_etiquetado": "Motivo",
    }
    df_display = df[display_cols].rename(columns=rename_map)
    st.dataframe(df_display, use_container_width=True, hide_index=True, height=500)

    render_section_exports(
        section_key="art510_preview",
        section_title="Art. 510 — Vista previa",
        csv_items=[
            ("candidatos", df),
            ("vista_agrupada", pivot.reset_index() if "pivot" in locals() else pd.DataFrame()),
            ("detalle", df_display if "df_display" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Candidatos por grupo protegido", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
            {"title": "Candidatos por plataforma y fuente", "fig": fig_gr if "fig_gr" in locals() else None, "kind": "plotly"},
            {"title": "Distribución de intensidad", "fig": fig_int if "fig_int" in locals() else None, "kind": "plotly"},
        ],
    )

    # ── Ejecutar evaluación LLM ──
    st.markdown("---")
    st.markdown("### Ejecutar evaluación Art. 510.1")

    already_done = _art510_get_already_evaluated()
    pending = []
    for _, r in df.iterrows():
        key = f"{r['message_uuid']}|{r['label_source']}"
        if key not in already_done:
            pending.append(r)

    total_pending = len(pending)
    total_already = len(already_done)

    if total_already > 0:
        st.caption(f"Ya evaluados previamente: {total_already:,} (en caché)")

    if total_pending == 0 and total_already > 0:
        st.success("Todos los candidatos ya fueron evaluados. Recarga la página para ver los resultados.")
        if st.button("Recargar datos", key="art510_reload"):
            st.cache_data.clear()
            st.rerun()
        return

    if total_pending == 0:
        st.warning("No hay candidatos para evaluar.")
        return

    st.markdown(f"**{total_pending:,}** mensajes pendientes de evaluación jurídica.")

    api_key = _get_openai_api_key()

    if api_key:
        st.caption("API key de OpenAI configurada ✓")
    else:
        st.warning(
            "No se encontró la API key en secrets. "
            "Configúrala en Streamlit Cloud: Settings > Secrets > `OPENAI_API_KEY = \"sk-...\"`"
        )
        api_key_input = st.text_input(
            "O introdúcela aquí:",
            type="password",
            placeholder="sk-...",
            key="art510_api_key",
        )
        api_key = _clean_api_key(api_key_input)

    import os as _os
    model = (_os.environ.get("OPENAI_MODEL") or "gpt-4o").strip()

    col_limit, col_model = st.columns(2)
    with col_limit:
        max_eval = st.number_input(
            "Máx. mensajes a evaluar",
            min_value=1,
            max_value=total_pending,
            value=min(50, total_pending),
            step=10,
            key="art510_max_eval",
            help="Limita la cantidad para controlar el coste de API.",
        )
    with col_model:
        st.text_input(
            "Modelo",
            value=model,
            disabled=True,
            key="art510_model_display",
        )

    if not api_key:
        st.warning("Introduce tu API key de OpenAI para continuar.")
        return

    if st.button(
        f"Evaluar {max_eval} mensajes bajo Art. 510.1",
        type="primary",
        key="art510_run_eval",
    ):
        _art510_ensure_tables()

        try:
            from openai import OpenAI as _OpenAI
        except ImportError:
            st.error(
                "El paquete `openai` no está instalado. "
                "Agrega `openai>=1.0` a `requirements.txt` y reinicia la app."
            )
            return
        client = _OpenAI(api_key=api_key)

        # Verificar API key antes de procesar todo el lote
        try:
            client.models.list()
        except Exception as e:
            st.error(f"Error de autenticación con OpenAI: {type(e).__name__}. Verifica tu API key.")
            return

        batch_to_process = pending[:max_eval]
        results = []
        unsaved_buffer = []
        n_delitos = 0
        total_in_db = total_already

        feedback = _art510_load_feedback_examples()
        if feedback:
            st.caption("Feedback humano cargado: el LLM usará correcciones anteriores para calibrar su criterio.")

        progress = st.progress(0, text="Iniciando evaluación...")
        status = st.empty()

        try:
            for i, r in enumerate(batch_to_process):
                txt = str(r.get("content_original", "")).strip()
                if txt:
                    evaluation = _art510_eval_single(client, model, txt, feedback=feedback)
                else:
                    evaluation = {
                        "es_potencial_delito": False, "apartado_510": "",
                        "grupo_protegido": "", "conducta_detectada": "",
                        "justificacion": "Texto vacío", "confianza": "baja",
                    }

                result = {
                    "message_uuid": str(r["message_uuid"]),
                    "label_source": str(r["label_source"]),
                    **evaluation,
                }
                results.append(result)
                unsaved_buffer.append(result)

                if evaluation["es_potencial_delito"]:
                    n_delitos += 1

                pct = (i + 1) / len(batch_to_process)
                progress.progress(pct, text=f"Evaluando {i+1}/{len(batch_to_process)}...")

                if len(unsaved_buffer) >= 10:
                    db_count = _art510_save_batch(unsaved_buffer)
                    if db_count > 0:
                        total_in_db = db_count
                        status.success(
                            f"Guardados en PostgreSQL: {len(results):,}/{len(batch_to_process)} "
                            f"(total en BD: {total_in_db:,}) | Pot. delitos: {n_delitos}"
                        )
                    else:
                        status.warning(
                            f"Procesados {len(results):,}/{len(batch_to_process)} — "
                            f"error al guardar lote en BD"
                        )
                    unsaved_buffer = []

        except Exception as e:
            st.error(f"Error durante la evaluación: {type(e).__name__} — {e}")
            if unsaved_buffer:
                db_count = _art510_save_batch(unsaved_buffer)
                if db_count > 0:
                    total_in_db = db_count
            if results:
                st.warning(
                    f"Se guardaron {len(results):,} evaluaciones antes del error. "
                    f"Total en BD: {total_in_db:,}"
                )
                st.cache_data.clear()
            return

        if unsaved_buffer:
            db_count = _art510_save_batch(unsaved_buffer)
            if db_count > 0:
                total_in_db = db_count

        progress.progress(1.0, text="Evaluación completada")
        st.success(
            f"Evaluación completada: {len(results):,} mensajes procesados, "
            f"{n_delitos:,} potenciales delitos detectados. "
            f"**Total acumulado en BD: {total_in_db:,}**"
        )
        st.cache_data.clear()
        st.balloons()

        if st.button("Ver resultados", key="art510_see_results"):
            st.rerun()


def _art510_escape(s) -> str:
    return html.escape(str(s) if s is not None and not (isinstance(s, float) and pd.isna(s)) else "")


def _render_art510_validacion_hscroll(cards_inner_html: str) -> None:
    """Carril horizontal con tarjetas (sin scroll vertical del listado principal)."""
    components.html(
        f"""
        <style>
        .art510-hscroll-wrap {{
            overflow-x: auto;
            overflow-y: hidden;
            -webkit-overflow-scrolling: touch;
            width: 100%;
            padding: 4px 2px 12px 2px;
        }}
        .art510-hscroll-inner {{
            display: flex;
            flex-direction: row;
            gap: 14px;
            align-items: stretch;
        }}
        .art510-card {{
            flex: 0 0 min(340px, 88vw);
            max-width: 360px;
            min-width: 260px;
            border: 1px solid #cbd5e0;
            border-radius: 10px;
            padding: 12px;
            background: #f8fafc;
            font-family: system-ui, -apple-system, sans-serif;
            font-size: 13px;
            color: #1a202c;
            line-height: 1.35;
        }}
        .art510-card blockquote {{
            margin: 0 0 10px 0;
            padding: 8px 10px;
            background: #fff;
            border-left: 3px solid #2b6cb0;
            font-size: 12px;
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 220px;
            overflow-y: auto;
        }}
        .art510-card .meta {{ margin: 4px 0; font-size: 12px; }}
        .art510-card details {{ margin-top: 8px; font-size: 11px; color: #4a5568; }}
        .art510-badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 600;
            margin-bottom: 6px;
            background: #e2e8f0;
        }}
        </style>
        <div class="art510-hscroll-wrap"><div class="art510-hscroll-inner">
        {cards_inner_html}
        </div></div>
        """,
        height=540,
        scrolling=False,
    )


def _render_art510_validacion_humana(summary: dict):
    """Sub-sección: resultado de validaciones humanas Art. 510."""
    total_val = summary.get("total_validados", 0)
    if total_val == 0:
        return

    st.markdown("---")
    st.markdown("### Validación humana")
    st.markdown(
        "Resultado de la revisión manual por expertos de los mensajes "
        "pre-seleccionados por el LLM como potenciales delitos Art. 510."
    )

    confirmados = summary.get("total_confirmados", 0)
    rechazados = summary.get("total_rechazados", 0)
    tasa_precision = (confirmados / total_val * 100) if total_val else 0

    v1, v2, v3, v4 = st.columns(4)
    v1.metric("Revisados por humano", f"{total_val}")
    v2.metric("Confirmados como delito", f"{confirmados}")
    v3.metric("Rechazados", f"{rechazados}")
    v4.metric("Precisión del LLM", f"{tasa_precision:.0f}%")

    df_vh = load_art510_validaciones_humanas()
    if df_vh.empty:
        return

    _dec_lab = {
        "confirmado": "Confirmado",
        "rechazado": "Rechazado",
        "corregido": "Corregido",
    }
    st.markdown("#### Resumen visual")
    g1, g2 = st.columns(2)
    with g1:
        vc = df_vh["validacion_humana"].value_counts().reset_index()
        vc.columns = ["decision", "Cantidad"]
        vc["Decisión"] = vc["decision"].map(lambda x: _dec_lab.get(str(x), str(x)))
        fig_d = px.pie(
            vc, names="Decisión", values="Cantidad",
            title="Distribución de decisiones humanas",
            hole=0.35,
            color="Decisión",
            color_discrete_map={
                "Confirmado": COLORS["danger"],
                "Rechazado": COLORS["muted"],
                "Corregido": COLORS["warning"],
            },
        )
        fig_d.update_layout(height=360)
        st.plotly_chart(fig_d, use_container_width=True, key="art510_vh_pie_decisiones")
    with g2:
        df_ap = df_vh[df_vh["validacion_humana"].isin(["confirmado", "corregido"])].copy()
        df_ap = df_ap[df_ap["apartado_510_final"].notna() & (df_ap["apartado_510_final"].astype(str) != "")]
        if df_ap.empty:
            st.info("Sin apartado humano registrado para confirmados/corregidos.")
        else:
            df_ap["Apartado"] = df_ap["apartado_510_final"].map(
                lambda x: APARTADO_LABELS.get(x, x) if pd.notna(x) else "—"
            )
            ap_c = df_ap["Apartado"].value_counts().reset_index()
            ap_c.columns = ["Apartado", "Cantidad"]
            fig_ap = px.bar(
                ap_c, x="Apartado", y="Cantidad",
                title="Apartado Art. 510.1 (decisión humana)",
                color="Apartado",
                color_discrete_map={
                    APARTADO_LABELS["1a"]: ART510_COLORS["1a"],
                    APARTADO_LABELS["1b"]: ART510_COLORS["1b"],
                    APARTADO_LABELS["1c"]: ART510_COLORS["1c"],
                },
            )
            fig_ap.update_layout(height=360, showlegend=False)
            st.plotly_chart(fig_ap, use_container_width=True, key="art510_vh_bar_apartado")

    st.markdown("---")
    tab_conf, tab_rech, tab_all = st.tabs([
        f"Confirmados ({confirmados})",
        f"Rechazados ({rechazados})",
        "Todos",
    ])

    with tab_conf:
        df_c = df_vh[df_vh["validacion_humana"] == "confirmado"]
        if df_c.empty:
            st.info("No hay mensajes confirmados como delito.")
        else:
            st.caption("Desplazá horizontalmente para ver todas las tarjetas.")
            parts_conf: List[str] = []
            for _, r in df_c.iterrows():
                cond = r.get("conducta_final")
                com = r.get("comentario")
                just = r.get("llm_justificacion")
                extra_cond = (
                    f'<div class="meta"><b>Conducta:</b> {_art510_escape(cond)}</div>'
                    if cond and str(cond).strip() else ""
                )
                extra_com = (
                    f'<div class="meta"><b>Comentario:</b> {_art510_escape(com)}</div>'
                    if com and str(com).strip() else ""
                )
                extra_just = (
                    f"<p>Justificación LLM: {_art510_escape(just)}</p>"
                    if just and str(just).strip() else ""
                )
                parts_conf.append(
                    "<div class=\"art510-card\">"
                    f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                    f"<div class=\"meta\"><b>Apartado:</b> {_art510_escape(r.get('apartado_510_final')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Grupo protegido:</b> {_art510_escape(r.get('grupo_protegido_final')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                    f"{extra_cond}{extra_com}"
                    "<details><summary>Comparar con evaluación LLM</summary>"
                    f"<p>Apartado LLM: <code>{_art510_escape(r.get('llm_apartado')) or '—'}</code></p>"
                    f"<p>Grupo LLM: <code>{_art510_escape(r.get('llm_grupo')) or '—'}</code></p>"
                    f"<p>Confianza LLM: <code>{_art510_escape(r.get('llm_confianza')) or '—'}</code></p>"
                    f"{extra_just}"
                    "</details></div>"
                )
            _render_art510_validacion_hscroll("".join(parts_conf))

    with tab_rech:
        df_r = df_vh[df_vh["validacion_humana"] == "rechazado"]
        if df_r.empty:
            st.info("No hay mensajes rechazados.")
        else:
            st.caption(
                "Estos mensajes fueron evaluados como potencial delito por el LLM "
                "pero descartados por el experto humano. Desplazá horizontalmente para ver todas las tarjetas."
            )
            parts_rech: List[str] = []
            for _, r in df_r.iterrows():
                parts_rech.append(
                    "<div class=\"art510-card\">"
                    f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                    f"<div class=\"meta\"><b>Confianza LLM:</b> {_art510_escape(r.get('llm_confianza')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Grupo (LLM):</b> {_art510_escape(r.get('llm_grupo')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                    "</div>"
                )
            _render_art510_validacion_hscroll("".join(parts_rech))

    with tab_all:
        st.caption(
            "Vista completa: desplazá horizontalmente. El texto largo de cada mensaje se puede "
            "revisar con scroll **dentro** de la tarjeta."
        )
        _dec_badge = {
            "confirmado": "Confirmado",
            "rechazado": "Rechazado",
            "corregido": "Corregido",
        }
        parts_all: List[str] = []
        for _, r in df_vh.iterrows():
            vh = str(r.get("validacion_humana") or "")
            badge = _dec_badge.get(vh, _art510_escape(vh) or "—")
            cond = r.get("conducta_final")
            com = r.get("comentario")
            extra_cond = (
                f'<div class="meta"><b>Conducta (humano):</b> {_art510_escape(cond)}</div>'
                if cond and str(cond).strip() else ""
            )
            extra_com = (
                f'<div class="meta"><b>Comentario:</b> {_art510_escape(com)}</div>'
                if com and str(com).strip() else ""
            )
            parts_all.append(
                "<div class=\"art510-card\">"
                f'<span class="art510-badge">{_art510_escape(badge)}</span>'
                f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                f"<div class=\"meta\"><b>Apartado (humano):</b> {_art510_escape(r.get('apartado_510_final')) or '—'}</div>"
                f"<div class=\"meta\"><b>Grupo (humano):</b> {_art510_escape(r.get('grupo_protegido_final')) or '—'}</div>"
                f"{extra_cond}{extra_com}"
                f"<div class=\"meta\"><b>Confianza LLM:</b> {_art510_escape(r.get('llm_confianza')) or '—'}</div>"
                f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                "</div>"
            )
        _render_art510_validacion_hscroll("".join(parts_all))


def _render_art510_full(summary, sel_platforms, sel_sources, solo_delitos):
    """Vista completa con evaluaciones LLM Art. 510 ya procesadas."""
    df = load_art510_data(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        label_sources=tuple(sel_sources) if sel_sources else None,
        solo_delitos=solo_delitos,
    )

    if df.empty:
        st.info("No hay datos con los filtros seleccionados.")
        return

    # ── KPIs ──
    st.markdown("---")
    st.markdown("### Indicadores clave")

    total_evaluados_db = summary["total_evaluados"]
    total_delitos_db = summary["total_delitos"]
    pct_delitos_db = (total_delitos_db / total_evaluados_db * 100) if total_evaluados_db else 0

    df_delitos_all = df[df["es_potencial_delito"]].copy() if not df.empty else df
    n_1a = (df_delitos_all["apartado_510"] == "1a").sum() if not df_delitos_all.empty else 0
    n_1b = (df_delitos_all["apartado_510"] == "1b").sum() if not df_delitos_all.empty else 0
    n_1c = (df_delitos_all["apartado_510"] == "1c").sum() if not df_delitos_all.empty else 0

    st.markdown(f"""
<style>
.metric-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}}
.metric-card {{
    background-color: #1B3A6B;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}}
.metric-card .label {{
    font-size: 13px;
    font-weight: 400;
    opacity: 0.85;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.metric-card .value {{
    font-size: 28px;
    font-weight: 700;
    line-height: 1;
}}
.metric-card .sub {{
    font-size: 12px;
    opacity: 0.7;
    margin-top: 6px;
}}
</style>

<div class="metric-grid">
  <div class="metric-card">
    <div class="label">Total evaluados</div>
    <div class="value">{total_evaluados_db:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Pot. delitos</div>
    <div class="value">{total_delitos_db:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">% Delitos</div>
    <div class="value">{pct_delitos_db:.1f}%</div>
  </div>
  <div class="metric-card">
    <div class="label">Art. 510.1a</div>
    <div class="value">{n_1a:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Art. 510.1b</div>
    <div class="value">{n_1b:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Art. 510.1c</div>
    <div class="value">{n_1c:,}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    if solo_delitos and len(df) < total_evaluados_db:
        st.caption(
            f"Mostrando {len(df):,} mensajes (filtro activo: solo potenciales delitos). "
            f"Desmarca el filtro para ver todos."
        )

    # ── Gráficos ──
    st.markdown("---")
    st.markdown("### Distribución por apartado y grupo protegido")

    df_delitos = df[df["es_potencial_delito"]].copy()

    if df_delitos.empty:
        st.info("No hay potenciales delitos con los filtros seleccionados.")
    else:
        col_g1, col_g2 = st.columns(2)

        with col_g1:
            ap_counts = (
                df_delitos["apartado_label"]
                .value_counts()
                .reset_index()
            )
            ap_counts.columns = ["Apartado", "Cantidad"]
            fig_ap = px.pie(
                ap_counts, names="Apartado", values="Cantidad",
                title="Por apartado del Art. 510.1",
                color="Apartado",
                color_discrete_map={
                    APARTADO_LABELS["1a"]: ART510_COLORS["1a"],
                    APARTADO_LABELS["1b"]: ART510_COLORS["1b"],
                    APARTADO_LABELS["1c"]: ART510_COLORS["1c"],
                },
                hole=0.4,
            )
            fig_ap.update_layout(height=400)
            st.plotly_chart(fig_ap, use_container_width=True)

        with col_g2:
            gp_counts = (
                df_delitos["grupo_protegido"]
                .dropna()
                .where(lambda s: s != "")
                .dropna()
                .value_counts()
                .head(12)
                .reset_index()
            )
            gp_counts.columns = ["Grupo protegido", "Cantidad"]
            fig_gp = px.bar(
                gp_counts, x="Cantidad", y="Grupo protegido",
                orientation="h",
                title="Por grupo protegido",
                color_discrete_sequence=[COLORS["accent"]],
            )
            fig_gp.update_layout(height=400, yaxis=dict(autorange="reversed"))
            _apply_horizontal_bar_labels(fig_gp)
            st.plotly_chart(fig_gp, use_container_width=True)

    # ── Vista agrupada: Plataforma x Fuente ──
    st.markdown("---")
    st.markdown("### Vista agrupada")

    if not df_delitos.empty:
        tab_pivot, tab_conf, tab_detail = st.tabs(
            ["Plataforma x Etiquetado", "Nivel de confianza", "Detalle mensajes"]
        )

        with tab_pivot:
            pivot = pd.crosstab(
                df_delitos["platform_label"],
                df_delitos["source_label"],
                margins=True,
                margins_name="Total",
            )
            st.dataframe(pivot, use_container_width=True)

            if len(df_delitos["platform_label"].unique()) > 0 and len(df_delitos["source_label"].unique()) > 0:
                grouped = (
                    df_delitos.groupby(["platform_label", "source_label"])
                    .size()
                    .reset_index(name="Cantidad")
                )
                fig_grouped = px.bar(
                    grouped, x="platform_label", y="Cantidad",
                    color="source_label",
                    barmode="group",
                    title="Potenciales delitos por plataforma y fuente de etiquetado",
                    labels={"platform_label": "Plataforma", "source_label": "Fuente"},
                    color_discrete_map={
                        "Etiquetado LLM": COLORS["accent"],
                        "Etiquetado humano": COLORS["success"],
                    },
                )
                fig_grouped.update_layout(height=400)
                st.plotly_chart(fig_grouped, use_container_width=True)

        with tab_conf:
            conf_order = ["alta", "media", "baja"]
            conf_counts = (
                df_delitos["confianza"]
                .value_counts()
                .reindex(conf_order, fill_value=0)
                .reset_index()
            )
            conf_counts.columns = ["Confianza", "Cantidad"]
            conf_colors = {"alta": COLORS["danger"], "media": COLORS["warning"], "baja": COLORS["muted"]}
            fig_conf = px.bar(
                conf_counts, x="Confianza", y="Cantidad",
                color="Confianza",
                color_discrete_map=conf_colors,
                title="Distribución por nivel de confianza del LLM",
            )
            fig_conf.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_conf, use_container_width=True)

        with tab_detail:
            display_cols = [
                "content_original", "platform_label", "source_label",
                "apartado_label", "grupo_protegido", "conducta_detectada",
                "justificacion", "confianza",
            ]
            rename_map = {
                "content_original": "Mensaje",
                "platform_label": "Plataforma",
                "source_label": "Fuente",
                "apartado_label": "Apartado",
                "grupo_protegido": "Grupo protegido",
                "conducta_detectada": "Conducta",
                "justificacion": "Justificación",
                "confianza": "Confianza",
            }
            df_display = df_delitos[display_cols].rename(columns=rename_map)
            st.dataframe(df_display, use_container_width=True, hide_index=True, height=500)

    render_section_exports(
        section_key="art510_full",
        section_title="Art. 510 — Evaluación completa",
        csv_items=[
            ("evaluaciones_filtradas", df),
            ("potenciales_delito", df_delitos if "df_delitos" in locals() else pd.DataFrame()),
            ("detalle_potenciales_delito", df_display if "df_display" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Distribución por apartado", "fig": fig_ap if "fig_ap" in locals() else None, "kind": "plotly"},
            {"title": "Distribución por grupo protegido", "fig": fig_gp if "fig_gp" in locals() else None, "kind": "plotly"},
            {"title": "Plataforma x fuente", "fig": fig_grouped if "fig_grouped" in locals() else None, "kind": "plotly"},
            {"title": "Distribución por confianza", "fig": fig_conf if "fig_conf" in locals() else None, "kind": "plotly"},
        ],
    )

    # ── Validación humana ──
    _render_art510_validacion_humana(summary)

    # ── Evaluar nuevos mensajes (expander discreto) ──
    already_done = _art510_get_already_evaluated()
    df_all_candidates = load_art510_candidates()
    new_pending = []
    if not df_all_candidates.empty:
        for _, r in df_all_candidates.iterrows():
            key = f"{r['message_uuid']}|{r['label_source']}"
            if key not in already_done:
                new_pending.append(r)

    if new_pending:
        st.markdown("---")
        with st.expander(f"Evaluar {len(new_pending):,} nuevos mensajes pendientes"):
            api_key = _get_openai_api_key()

            if not api_key:
                api_key_input = st.text_input(
                    "OpenAI API Key", type="password",
                    placeholder="sk-...", key="art510_full_api_key",
                )
                api_key = _clean_api_key(api_key_input)

            import os as _os
            model = (_os.environ.get("OPENAI_MODEL") or "gpt-4o").strip()
            max_eval = st.number_input(
                "Máx. mensajes", min_value=1,
                max_value=len(new_pending),
                value=min(50, len(new_pending)),
                step=10, key="art510_full_max",
            )

            if api_key and st.button(
                f"Evaluar {max_eval} nuevos mensajes",
                type="primary", key="art510_full_run",
            ):
                try:
                    from openai import OpenAI as _OpenAI
                except ImportError:
                    st.error(
                        "El paquete `openai` no está instalado. "
                        "Agrega `openai>=1.0` a `requirements.txt` y reinicia la app."
                    )
                    return
                client = _OpenAI(api_key=api_key)

                try:
                    client.models.list()
                except Exception as e:
                    st.error(f"Error de autenticación: {type(e).__name__}. Verifica tu API key.")
                    return

                batch = new_pending[:max_eval]
                results = []
                unsaved_buf = []
                n_delitos = 0
                total_in_db = len(already_done)

                feedback = _art510_load_feedback_examples()
                if feedback:
                    st.caption("Feedback humano cargado para calibrar las evaluaciones.")

                progress = st.progress(0, text="Evaluando...")
                status_full = st.empty()

                try:
                    for i, r in enumerate(batch):
                        txt = str(r.get("content_original", "")).strip()
                        if txt:
                            ev = _art510_eval_single(client, model, txt, feedback=feedback)
                        else:
                            ev = {
                                "es_potencial_delito": False, "apartado_510": "",
                                "grupo_protegido": "", "conducta_detectada": "",
                                "justificacion": "Texto vacío", "confianza": "baja",
                            }
                        results.append({"message_uuid": str(r["message_uuid"]),
                                        "label_source": str(r["label_source"]), **ev})
                        unsaved_buf.append(results[-1])
                        if ev["es_potencial_delito"]:
                            n_delitos += 1
                        progress.progress((i + 1) / len(batch),
                                          text=f"Evaluando {i+1}/{len(batch)}...")
                        if len(unsaved_buf) >= 10:
                            db_count = _art510_save_batch(unsaved_buf)
                            if db_count > 0:
                                total_in_db = db_count
                                status_full.success(
                                    f"Guardados en PostgreSQL: {len(results):,}/{len(batch)} "
                                    f"(total en BD: {total_in_db:,}) | Pot. delitos: {n_delitos}"
                                )
                            unsaved_buf = []
                except Exception as e:
                    st.error(f"Error: {type(e).__name__} — {e}")
                    if unsaved_buf:
                        db_count = _art510_save_batch(unsaved_buf)
                        if db_count > 0:
                            total_in_db = db_count
                    if results:
                        st.warning(
                            f"Guardados {len(results):,} antes del error. "
                            f"Total en BD: {total_in_db:,}"
                        )
                        st.cache_data.clear()
                    return

                if unsaved_buf:
                    db_count = _art510_save_batch(unsaved_buf)
                    if db_count > 0:
                        total_in_db = db_count

                progress.progress(1.0, text="Completado")
                st.success(
                    f"{len(results):,} evaluados, {n_delitos:,} potenciales delitos. "
                    f"**Total acumulado en BD: {total_in_db:,}**"
                )
                st.cache_data.clear()


def render_analisis_art510():
    """Sección 7: Análisis de mensajes bajo el Art. 510.1 del Código Penal."""
    if not _require_role("admin", "editor", section="Análisis Art. 510"):
        return
    # Asegurar que las tablas existan antes de cualquier consulta
    _art510_ensure_tables()

    _render_section_header(
        "Análisis Art. 510",
        "Potenciales delitos de odio según el art. 510.1 CP (conductas 1a–1c; sin 510.2).",
    )
    st.caption(
        "Evaluación de mensajes etiquetados como odio bajo el criterio del "
        "artículo 510.1 del Código Penal español (excluyendo apartado 2). "
        "Conductas: incitación (1a), distribución de material (1b), "
        "negación/trivialización de genocidio (1c)."
    )

    # ── Filtros (siempre visibles) ──
    st.markdown("### Filtros")
    opts = load_filter_options(_role_can_access_raw())
    platforms_display = {p: platform_label(p) for p in opts["platforms"]}

    summary = load_art510_summary()
    has_evaluations = summary["total_evaluados"] > 0

    if has_evaluations:
        col_f1, col_f2, col_f3 = st.columns(3)
    else:
        col_f1, col_f2 = st.columns(2)

    with col_f1:
        sel_platforms = st.multiselect(
            "Plataforma",
            options=list(platforms_display.keys()),
            format_func=lambda x: platforms_display[x],
            default=list(platforms_display.keys()),
            key="art510_plat",
            placeholder="Todas las plataformas",
        )

    with col_f2:
        sel_sources = st.multiselect(
            "Fuente de etiquetado",
            options=list(LABEL_SOURCE_LABELS.keys()),
            format_func=lambda x: LABEL_SOURCE_LABELS[x],
            default=list(LABEL_SOURCE_LABELS.keys()),
            key="art510_source",
            placeholder="Seleccionar…",
        )

    solo_delitos = False
    if has_evaluations:
        with col_f3:
            solo_delitos = st.checkbox(
                "Solo potenciales delitos",
                value=True,
                key="art510_solo_delitos",
            )

    if not sel_platforms or not sel_sources:
        st.warning("Selecciona al menos una plataforma y una fuente de etiquetado.")
        return

    # ── Renderizar vista según disponibilidad de datos ──
    if has_evaluations:
        _render_art510_full(summary, sel_platforms, sel_sources, solo_delitos)
    else:
        _render_art510_preview(sel_platforms, sel_sources)

    # ── Nota legal (siempre visible) ──
    st.markdown("---")
    with st.expander("Nota sobre el Art. 510.3 (agravante por difusión en internet)"):
        st.markdown(
            "Todos los mensajes analizados provienen de plataformas de internet "
            "(X, YouTube), lo que técnicamente aplica el **agravante del Art. 510.3**: "
            "\"*Las penas se impondrán en su mitad superior cuando los hechos se "
            "hubieran llevado a cabo a través de un medio de comunicación social, "
            "por medio de internet o mediante el uso de tecnologías de la información, "
            "de modo que, aquel se hiciera accesible a un elevado número de personas.*\""
        )


# ============================================================
# SECCIÓN: DELITOS DE ODIO (datos oficiales)
# ============================================================

# Mapeo de códigos de motivo a etiquetas legibles
BIAS_LABELS = {
    "ANTIGITANISMO": "Antigitanismo",
    "ANTISEMITISMO": "Antisemitismo",
    "APOROFOBIA": "Aporofobia",
    "DISCAPACIDAD": "Discapacidad",
    "DISCRIM_ENFERMEDAD": "Discriminación por enfermedad",
    "DISCRIM_GENERACIONAL": "Discriminación generacional",
    "DISCRIM_SEXO_GENERO": "Discriminación sexo/género",
    "IDEOLOGIA": "Ideología",
    "ORI_SEX_IDENT_GEN": "Orientación sexual / Identidad de género",
    "RACISMO_XENOFOBIA": "Racismo / Xenofobia",
    "RELIGION": "Religión",
    "ISLAMOFOBIA": "Islamofobia",
}

AGE_LABELS = {
    "MENORES": "Menores de edad",
    "18_25": "18-25 años",
    "26_40": "26-40 años",
    "41_50": "41-50 años",
    "51_65": "51-65 años",
    "65_MAS": "+65 años",
    "DESCONOCIDA": "Desconocida",
}

AGE_ORDER = ["MENORES", "18_25", "26_40", "41_50", "51_65", "65_MAS", "DESCONOCIDA"]


def _bias_label(code: str) -> str:
    return BIAS_LABELS.get(code, code)


def _age_label(code: str) -> str:
    return AGE_LABELS.get(code, code)


@st.cache_data(ttl=300)
def load_crime_totals() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_motive_code, crimes_total
            FROM delitos.fact_crime_totals_minint
            ORDER BY year, bias_motive_code
        """, conn)
    df["motivo"] = df["bias_motive_code"].map(_bias_label)
    return df


@st.cache_data(ttl=300)
def load_crime_solved() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_motive_code, crimes_solved
            FROM delitos.fact_crime_solved_minint
            ORDER BY year, bias_motive_code
        """, conn)
    df["motivo"] = df["bias_motive_code"].map(_bias_label)
    return df


@st.cache_data(ttl=300)
def load_authors_age() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, age_group_code, n_authors
            FROM delitos.fact_authors_by_age_minint
            ORDER BY year, age_group_code
        """, conn)
    df["grupo_edad"] = df["age_group_code"].map(_age_label)
    return df


@st.cache_data(ttl=300)
def load_investigations_sex() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_code, male, female
            FROM delitos.fact_investigaciones_sexo_minint
            ORDER BY year, bias_code
        """, conn)
    df["motivo"] = df["bias_code"].map(_bias_label)
    return df


@st.cache_data(ttl=300)
def load_suspects_bias() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, bias_code, n_detained_or_investigated
            FROM delitos.fact_suspects_by_bias_minint
            ORDER BY year, bias_code
        """, conn)
    df["motivo"] = df["bias_code"].map(_bias_label)
    return df


@st.cache_data(ttl=300)
def load_prosecution_motives() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT source_type, year, motive_code, motive_label, value
            FROM delitos.fact_prosecution_discrimination_motives
            WHERE motive_code != 'TOTAL'
            ORDER BY year, motive_code
        """, conn)
    df["tipo"] = df["source_type"].map({
        "investigation": "Diligencias",
        "complaint": "Denuncias",
    })
    return df


@st.cache_data(ttl=300)
def load_prosecution_articles() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, legal_article, article_label, accusations_count
            FROM delitos.fact_prosecution_legal_articles
            ORDER BY year, legal_article
        """, conn)
    return df


@st.cache_data(ttl=300)
def load_fiscalia_investigations() -> pd.DataFrame:
    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT year, legal_article, legal_description, investigations
            FROM delitos.fact_fiscalia_investigations_by_legal_article
            ORDER BY year, investigations DESC
        """, conn)
    return df


def render_delitos():
    """Sección de datos oficiales de delitos de odio en España."""
    _render_section_header(
        "Delitos de odio — Datos oficiales",
        "España: series publicadas por el Ministerio del Interior y la Fiscalía General del Estado.",
    )
    st.caption("Fuente: Ministerio del Interior y Fiscalía General del Estado (2018-2024)")

    # ── Cargar todos los datasets ──
    df_totals = load_crime_totals()
    df_solved = load_crime_solved()
    df_age = load_authors_age()
    df_sex = load_investigations_sex()
    df_suspects = load_suspects_bias()
    df_prosecution = load_prosecution_motives()
    df_articles = load_prosecution_articles()
    df_fiscalia = load_fiscalia_investigations()

    years = sorted(df_totals["year"].unique())
    last_year = max(years)
    prev_year = last_year - 1

    # ── Filtros con botón "Seleccionar todos" ──
    st.markdown("### Filtros")
    all_motives = sorted(df_totals["motivo"].unique())

    col_btn1, col_btn2 = st.columns(2)
    with col_btn1:
        if st.button("Todos los años", key="btn_all_years"):
            st.session_state["delitos_years"] = years
    with col_btn2:
        if st.button("Todos los motivos", key="btn_all_motives"):
            st.session_state["delitos_motives"] = all_motives

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        selected_years = st.multiselect(
            "Años", years, default=years, key="delitos_years",
            placeholder="Seleccionar…",
        )
    with col_f2:
        selected_motives = st.multiselect(
            "Motivos de odio", all_motives, default=all_motives, key="delitos_motives",
            placeholder="Seleccionar…",
        )

    if not selected_years or not selected_motives:
        st.warning("Selecciona al menos un año y un motivo.")
        return

    # Filtrar datasets
    df_totals_f = df_totals[
        df_totals["year"].isin(selected_years) & df_totals["motivo"].isin(selected_motives)
    ]
    df_solved_f = df_solved[
        df_solved["year"].isin(selected_years) & df_solved["motivo"].isin(selected_motives)
    ]

    # ── 1. KPIs (dinámicos según filtros) ──
    st.markdown("---")
    st.markdown("### Indicadores clave")

    kpi_year = max(selected_years)
    kpi_prev = kpi_year - 1

    df_kpi = df_totals[df_totals["motivo"].isin(selected_motives)]
    total_kpi = df_kpi[df_kpi["year"] == kpi_year]["crimes_total"].sum()
    total_kpi_prev = df_kpi[df_kpi["year"] == kpi_prev]["crimes_total"].sum()
    solved_kpi = df_solved[
        (df_solved["year"] == kpi_year) & df_solved["motivo"].isin(selected_motives)
    ]["crimes_solved"].sum()
    variation = ((total_kpi - total_kpi_prev) / total_kpi_prev * 100) if total_kpi_prev else 0
    solve_rate = (solved_kpi / total_kpi * 100) if total_kpi else 0
    df_kpi_yr = df_kpi[df_kpi["year"] == kpi_year]
    top_motive = (
        df_kpi_yr.sort_values("crimes_total", ascending=False).iloc[0]["motivo"]
        if not df_kpi_yr.empty else "N/A"
    )

    st.markdown(f"""
<style>
.metric-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
    gap: 16px;
    margin-bottom: 24px;
}}
.metric-card {{
    background-color: #1B3A6B;
    border-radius: 12px;
    padding: 20px;
    box-shadow: 0 4px 12px rgba(0,0,0,0.15);
    color: white;
    text-align: center;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
}}
.metric-card .label {{
    font-size: 13px;
    font-weight: 400;
    opacity: 0.85;
    margin-bottom: 8px;
    text-transform: uppercase;
    letter-spacing: 0.5px;
}}
.metric-card .value {{
    font-size: 28px;
    font-weight: 700;
    line-height: 1;
}}
.metric-card .sub {{
    font-size: 12px;
    opacity: 0.7;
    margin-top: 6px;
}}
</style>

<div class="metric-grid">
  <div class="metric-card">
    <div class="label">Total delitos ({kpi_year})</div>
    <div class="value">{total_kpi:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Var. vs {kpi_prev}</div>
    <div class="value">{variation:+.1f}%</div>
  </div>
  <div class="metric-card">
    <div class="label">Esclarecimiento ({kpi_year})</div>
    <div class="value">{solve_rate:.1f}%</div>
  </div>
  <div class="metric-card">
    <div class="label">Motivo principal</div>
    <div class="value">{top_motive}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── 2. Evolución temporal ──
    st.markdown("---")
    st.markdown("### Evolución de delitos de odio por año")

    agg_year = (
        df_totals_f.groupby(["year", "motivo"])["crimes_total"]
        .sum()
        .reset_index()
    )

    tab_line, tab_bar = st.tabs(["Líneas", "Barras apiladas"])

    with tab_line:
        fig_line = px.line(
            agg_year, x="year", y="crimes_total", color="motivo",
            markers=True,
            labels={"year": "Año", "crimes_total": "Nº delitos", "motivo": "Motivo"},
            color_discrete_sequence=DELITOS_COLORS,
        )
        fig_line.update_layout(
            xaxis=dict(dtick=1),
            legend=dict(orientation="h", yanchor="bottom", y=-0.35),
            height=500,
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with tab_bar:
        fig_bar = px.bar(
            agg_year, x="year", y="crimes_total", color="motivo",
            labels={"year": "Año", "crimes_total": "Nº delitos", "motivo": "Motivo"},
            color_discrete_sequence=DELITOS_COLORS,
        )
        fig_bar.update_layout(
            barmode="stack",
            xaxis=dict(dtick=1),
            legend=dict(orientation="h", yanchor="bottom", y=-0.35),
            height=500,
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # ── 3. Tasa de esclarecimiento ──
    st.markdown("---")
    st.markdown("### Tasa de esclarecimiento por motivo")

    col_yr = st.selectbox(
        "Año de referencia", sorted(selected_years, reverse=True),
        key="solve_year",
    )

    totals_yr = df_totals[df_totals["year"] == col_yr][["motivo", "crimes_total"]]
    solved_yr = df_solved[df_solved["year"] == col_yr][["motivo", "crimes_solved"]]
    merged = totals_yr.merge(solved_yr, on="motivo", how="left").fillna(0)
    merged["no_esclarecidos"] = merged["crimes_total"] - merged["crimes_solved"]
    merged = merged.sort_values("crimes_total", ascending=True)

    fig_solve = go.Figure()
    fig_solve.add_trace(go.Bar(
        y=merged["motivo"], x=merged["crimes_solved"],
        name="Esclarecidos", orientation="h",
        marker_color=COLORS["success"],
    ))
    fig_solve.add_trace(go.Bar(
        y=merged["motivo"], x=merged["no_esclarecidos"],
        name="No esclarecidos", orientation="h",
        marker_color=COLORS["muted"],
    ))
    fig_solve.update_layout(
        barmode="stack",
        xaxis_title="Nº delitos",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )
    _apply_horizontal_bar_labels(fig_solve)
    st.plotly_chart(fig_solve, use_container_width=True)

    # ── 4. Perfil de autores por edad ──
    st.markdown("---")
    st.markdown("### Perfil de autores por grupo de edad")

    df_age_f = df_age[df_age["year"].isin(selected_years)]
    df_age_f = df_age_f[df_age_f["age_group_code"] != "DESCONOCIDA"]

    # Ordenar por AGE_ORDER
    age_order_labels = [_age_label(a) for a in AGE_ORDER if a != "DESCONOCIDA"]
    df_age_f["grupo_edad"] = pd.Categorical(
        df_age_f["grupo_edad"], categories=age_order_labels, ordered=True
    )

    tab_age_bar, tab_age_line = st.tabs(["Por año", "Evolución"])

    with tab_age_bar:
        age_agg = df_age_f.groupby(["year", "grupo_edad"])["n_authors"].sum().reset_index()
        fig_age = px.bar(
            age_agg, x="grupo_edad", y="n_authors", color="year",
            barmode="group",
            labels={"grupo_edad": "Grupo de edad", "n_authors": "Nº autores", "year": "Año"},
            color_discrete_sequence=DELITOS_COLORS,
        )
        years_presentes = sorted(age_agg["year"].unique())
        fig_age.update_coloraxes(
            colorbar=dict(
                tickvals=years_presentes,
                ticktext=[str(y) for y in years_presentes],
            )
        )
        fig_age.update_layout(height=450)
        st.plotly_chart(fig_age, use_container_width=True)

    with tab_age_line:
        age_total_yr = df_age_f.groupby(["year", "grupo_edad"])["n_authors"].sum().reset_index()
        fig_age_l = px.line(
            age_total_yr, x="year", y="n_authors", color="grupo_edad",
            markers=True,
            labels={"year": "Año", "n_authors": "Nº autores", "grupo_edad": "Grupo de edad"},
            color_discrete_sequence=DELITOS_COLORS,
        )
        fig_age_l.update_layout(xaxis=dict(dtick=1), height=450)
        st.plotly_chart(fig_age_l, use_container_width=True)

    # ── 5. Investigados por sexo ──
    st.markdown("---")
    st.markdown("### Investigados/detenidos por sexo y motivo")

    df_sex_f = df_sex[
        df_sex["year"].isin(selected_years) & df_sex["motivo"].isin(selected_motives)
    ]
    sex_agg = df_sex_f.groupby("motivo")[["male", "female"]].sum().reset_index()
    sex_agg = sex_agg.sort_values("male", ascending=True)

    fig_sex = go.Figure()
    fig_sex.add_trace(go.Bar(
        y=sex_agg["motivo"], x=sex_agg["male"],
        name="Hombres", orientation="h",
        marker_color="#3498DB",
    ))
    fig_sex.add_trace(go.Bar(
        y=sex_agg["motivo"], x=sex_agg["female"],
        name="Mujeres", orientation="h",
        marker_color="#E74C3C",
    ))
    fig_sex.update_layout(
        barmode="stack",
        xaxis_title="Nº investigados/detenidos",
        height=450,
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )
    _apply_horizontal_bar_labels(fig_sex)
    st.plotly_chart(fig_sex, use_container_width=True)

    # Porcentaje de mujeres por motivo
    sex_agg["pct_mujeres"] = (
        sex_agg["female"] / (sex_agg["male"] + sex_agg["female"]) * 100
    ).round(1)
    with st.expander("Detalle: % mujeres por motivo"):
        st.dataframe(
            sex_agg[["motivo", "male", "female", "pct_mujeres"]]
            .rename(columns={
                "motivo": "Motivo",
                "male": "Hombres",
                "female": "Mujeres",
                "pct_mujeres": "% Mujeres",
            })
            .sort_values("% Mujeres", ascending=False),
            use_container_width=True, hide_index=True,
        )

    # ── 6. Fiscalía: denuncias vs diligencias por motivo ──
    st.markdown("---")
    st.markdown("### Fiscalía: denuncias vs diligencias por motivo")

    df_pros_f = df_prosecution[df_prosecution["year"].isin(selected_years)]

    pros_agg = (
        df_pros_f.groupby(["motive_label", "tipo"])["value"]
        .sum()
        .reset_index()
    )

    fig_pros = px.bar(
        pros_agg, x="value", y="motive_label", color="tipo",
        orientation="h", barmode="group",
        labels={"value": "Cantidad", "motive_label": "Motivo", "tipo": "Tipo"},
        color_discrete_map={"Diligencias": "#1F4E79", "Denuncias": "#F39C12"},
    )
    fig_pros.update_layout(
        height=500,
        yaxis=dict(categoryorder="total ascending"),
        legend=dict(orientation="h", yanchor="bottom", y=-0.2),
    )
    _apply_horizontal_bar_labels(fig_pros)
    st.plotly_chart(fig_pros, use_container_width=True)

    # ── 7. Artículos del Código Penal más aplicados ──
    st.markdown("---")
    st.markdown("### Artículos del Código Penal aplicados")

    # Usar fiscalía investigations si hay datos, sino prosecution_legal_articles
    if not df_fiscalia.empty:
        df_art_f = df_fiscalia[df_fiscalia["year"].isin(selected_years)]
        art_agg = (
            df_art_f.groupby(["legal_article", "legal_description"])["investigations"]
            .sum()
            .reset_index()
            .sort_values("investigations", ascending=True)
        )
        fig_art = px.bar(
            art_agg, x="investigations",
            y=art_agg["legal_article"] + " — " + art_agg["legal_description"],
            orientation="h",
            labels={"x": "Nº diligencias", "y": "Artículo"},
            color_discrete_sequence=[COLORS["primary"]],
        )
        fig_art.update_layout(height=450, yaxis_title="")
        _apply_horizontal_bar_labels(fig_art)
        st.plotly_chart(fig_art, use_container_width=True)
    elif not df_articles.empty:
        df_art_f = df_articles[df_articles["year"].isin(selected_years)]
        art_agg = (
            df_art_f.groupby(["legal_article", "article_label"])["accusations_count"]
            .sum()
            .reset_index()
            .dropna(subset=["accusations_count"])
            .sort_values("accusations_count", ascending=True)
        )
        if not art_agg.empty:
            fig_art = px.bar(
                art_agg, x="accusations_count",
                y=art_agg["legal_article"] + " — " + art_agg["article_label"],
                orientation="h",
                labels={"x": "Nº acusaciones", "y": "Artículo"},
                color_discrete_sequence=[COLORS["primary"]],
            )
            fig_art.update_layout(height=450, yaxis_title="")
            _apply_horizontal_bar_labels(fig_art)
            st.plotly_chart(fig_art, use_container_width=True)
        else:
            st.info("No hay datos de acusaciones por artículo para los años seleccionados.")
    else:
        st.info("No hay datos de artículos del Código Penal disponibles.")

    # ── Tabla resumen ──
    st.markdown("---")
    st.markdown("### Tabla resumen por año y motivo")

    summary = (
        df_totals_f.groupby(["year", "motivo"])["crimes_total"]
        .sum()
        .reset_index()
        .pivot_table(index="motivo", columns="year", values="crimes_total", fill_value=0)
    )
    summary["Total"] = summary.sum(axis=1)
    summary = summary.sort_values("Total", ascending=False)
    st.dataframe(summary, use_container_width=True)

    render_section_exports(
        section_key="delitos_oficiales",
        section_title="Delitos de odio (oficial)",
        csv_items=[
            ("totales_filtrados", df_totals_f if "df_totals_f" in locals() else pd.DataFrame()),
            ("esclarecidos_filtrados", df_solved_f if "df_solved_f" in locals() else pd.DataFrame()),
            ("evolucion_anual", agg_year if "agg_year" in locals() else pd.DataFrame()),
            ("esclarecimiento_motivo", merged if "merged" in locals() else pd.DataFrame()),
            ("autores_edad", age_agg if "age_agg" in locals() else pd.DataFrame()),
            ("investigados_sexo", sex_agg if "sex_agg" in locals() else pd.DataFrame()),
            ("fiscalia_motivos", pros_agg if "pros_agg" in locals() else pd.DataFrame()),
            ("articulos_penales", art_agg if "art_agg" in locals() else pd.DataFrame()),
            ("resumen_tabla", summary.reset_index() if "summary" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Evolución anual (líneas)", "fig": fig_line if "fig_line" in locals() else None, "kind": "plotly"},
            {"title": "Evolución anual (barras)", "fig": fig_bar if "fig_bar" in locals() else None, "kind": "plotly"},
            {"title": "Tasa de esclarecimiento", "fig": fig_solve if "fig_solve" in locals() else None, "kind": "plotly"},
            {"title": "Autores por edad (barras)", "fig": fig_age if "fig_age" in locals() else None, "kind": "plotly"},
            {"title": "Autores por edad (líneas)", "fig": fig_age_l if "fig_age_l" in locals() else None, "kind": "plotly"},
            {"title": "Investigados por sexo", "fig": fig_sex if "fig_sex" in locals() else None, "kind": "plotly"},
            {"title": "Fiscalía por motivo", "fig": fig_pros if "fig_pros" in locals() else None, "kind": "plotly"},
            {"title": "Artículos del código penal", "fig": fig_art if "fig_art" in locals() else None, "kind": "plotly"},
        ],
    )


# ============================================================
# ANOTACIÓN YOUTUBE
# ============================================================

CATEGORIA_NO_ODIO = "no_odio"
CATEGORIA_DUDOSO = "dudoso"


def _clasif_from_odio_flag(odio_flag: Optional[bool]) -> Optional[str]:
    if odio_flag is True:
        return "ODIO"
    if odio_flag is False:
        return "NO_ODIO"
    if odio_flag is None:
        return "DUDOSO"
    return None


def _categoria_odio_for_save(
    odio_flag: Optional[bool], categoria_odio: Optional[str]
) -> Optional[str]:
    """Valor explícito en validaciones_manuales según odio_flag."""
    if odio_flag is True:
        cat = (categoria_odio or "").strip()
        return cat if cat else None
    if odio_flag is False:
        return CATEGORIA_NO_ODIO
    if odio_flag is None:
        return CATEGORIA_DUDOSO
    return None


def _normalize_cat_for_coincide(cat: Optional[str], clasif: Optional[str]) -> str:
    c = (cat or "").strip().lower()
    clf = (clasif or "").strip().upper()
    if not c:
        if clf == "NO_ODIO":
            return CATEGORIA_NO_ODIO
        if clf == "DUDOSO":
            return CATEGORIA_DUDOSO
    return c


def _compute_coincide_con_llm(
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    llm_clasif: Optional[str],
    llm_categoria_pred: Optional[str],
    llm_intensidad_pred: Any,
) -> Optional[bool]:
    """
    Compara etiqueta humana (normalizada) con processed.etiquetas_llm.
    None si odio_flag no permite clasificación (no debería ocurrir tras normalizar).
    """
    human_clasif = _clasif_from_odio_flag(odio_flag)
    if human_clasif is None:
        return None

    llm_c = (llm_clasif or "").strip().upper()
    if llm_c != human_clasif:
        return False

    hum_cat = _normalize_cat_for_coincide(categoria_odio, human_clasif)
    llm_cat = _normalize_cat_for_coincide(llm_categoria_pred, llm_c)
    if hum_cat != llm_cat:
        return False

    if odio_flag is True:
        llm_i = str(llm_intensidad_pred or "").strip()
        return str(intensidad) == llm_i

    return True


def _fetch_llm_labels_for_uuid(message_uuid: str) -> tuple:
    """Lee predicción LLM para calcular coincide_con_llm al guardar."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT clasificacion_principal, categoria_odio_pred, intensidad_pred
            FROM processed.etiquetas_llm
            WHERE message_uuid = %s::uuid
            """,
            (message_uuid,),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def _period_to_sql_date(period: str) -> str:
    """
    period in ('day', 'week', 'month')
    Devuelve la fecha mínima como string ISO para usar en queries.
    """
    today = date.today()
    if period == "week":
        return (today - timedelta(days=7)).isoformat()
    if period == "month":
        return (today - timedelta(days=30)).isoformat()
    return today.isoformat()


def _load_admin_annotation_supervision(period: str) -> dict:
    """Carga conteos de anotación por subsección y anotador para el panel admin/editor."""
    fecha_desde = _period_to_sql_date(period)
    empty_df = pd.DataFrame(
        columns=["Anotador", "YT Odio", "Art.510", "LLM YT", "LLM X", "Total"]
    )

    queries = {
        "YT Odio": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND g.label_source = 'human_explicit'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "LLM YT": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND g.label_source = 'llm_validated'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "LLM X": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform IN ('x', 'twitter')
              AND g.label_source = 'llm_validated'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "Art.510": """
            SELECT annotator_id, COUNT(*) AS n
            FROM processed.validacion_art510_humana
            WHERE annotation_date >= %s
            GROUP BY annotator_id
        """,
    }

    summary: Dict[str, int] = {}
    frames: Dict[str, pd.DataFrame] = {}

    try:
        with get_conn() as conn:
            for subsection, sql in queries.items():
                df = pd.read_sql(sql, conn, params=(fecha_desde,))
                if df.empty:
                    summary[subsection] = 0
                    continue
                ann_col = "annotator_id" if "annotator_id" in df.columns else df.columns[0]
                df = df.rename(columns={ann_col: "Anotador", "n": subsection})
                summary[subsection] = int(df[subsection].sum())
                frames[subsection] = df[["Anotador", subsection]]

        by_annotator = empty_df.copy()
        for subsection, df_sub in frames.items():
            if df_sub.empty:
                continue
            if by_annotator.empty:
                by_annotator = df_sub.copy()
            else:
                by_annotator = by_annotator.merge(
                    df_sub, on="Anotador", how="outer"
                )

        if not by_annotator.empty:
            for col in ("YT Odio", "Art.510", "LLM YT", "LLM X"):
                if col not in by_annotator.columns:
                    by_annotator[col] = 0
            by_annotator = by_annotator.fillna(0)
            for col in ("YT Odio", "Art.510", "LLM YT", "LLM X"):
                by_annotator[col] = by_annotator[col].astype(int)
            by_annotator["Total"] = (
                by_annotator["YT Odio"]
                + by_annotator["Art.510"]
                + by_annotator["LLM YT"]
                + by_annotator["LLM X"]
            )
            by_annotator = by_annotator.sort_values("Total", ascending=False)
        else:
            by_annotator = empty_df

        return {"summary": summary, "by_annotator": by_annotator}
    except Exception:
        return {
            "summary": {k: 0 for k in queries},
            "by_annotator": empty_df,
        }


def _load_annotation_queue() -> pd.DataFrame:
    """Carga mensajes YouTube pendientes de anotación (sin cache)."""
    skipped = st.session_state.get("ann_skipped", set())

    with get_conn() as conn:
        df = pd.read_sql("""
            SELECT DISTINCT ON (pm.content_original)
                   pm.message_uuid, pm.content_original, pm.source_media,
                   pm.matched_terms, pm.relevante_score, pm.relevante_motivo,
                   pm.created_at, rm.tweet_id AS video_id
            FROM processed.mensajes pm
            LEFT JOIN raw.mensajes rm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND pm.relevante_llm = 'SI'
              AND pm.message_uuid NOT IN (
                  SELECT message_uuid FROM processed.validaciones_manuales
              )
            ORDER BY pm.content_original, pm.relevante_score DESC NULLS LAST
        """, conn)
        df = df.sort_values("relevante_score", ascending=False).head(100)

    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


def _load_annotation_kpis(annotator_id: str, period: str = "day") -> dict:
    """Carga KPIs de progreso de anotación YouTube."""
    fecha_desde = _period_to_sql_date(period)
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM processed.mensajes pm
            WHERE pm.platform = 'youtube'
              AND pm.relevante_llm = 'SI'
        """)
        total_relevantes = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
        """)
        total_anotados = cur.fetchone()[0]

        pendientes = total_relevantes - total_anotados

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND vm.annotation_date >= %s
        """, (fecha_desde,))
        anotados_periodo = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND vm.annotator_id = %s
        """, (annotator_id,))
        por_anotador = cur.fetchone()[0]

        cur.close()

    pct_avance = (total_anotados / total_relevantes * 100) if total_relevantes else 0

    return {
        "total_relevantes": total_relevantes,
        "pendientes": pendientes,
        "total_anotados": total_anotados,
        "anotados_periodo": anotados_periodo,
        "por_anotador": por_anotador,
        "pct_avance": pct_avance,
    }


def _stratified_split(target_ratio: float = 0.85) -> str:
    """Asigna split TRAIN/TEST de forma estratificada consultando el ratio actual en gold_dataset.

    Si el ratio actual de TRAIN < target_ratio → asigna TRAIN (para reequilibrar).
    Si ya está en target o más → asigna TEST.
    Con fallback a asignación aleatoria si la consulta falla.
    """
    import random
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    SUM(CASE WHEN split = 'TRAIN' THEN 1 ELSE 0 END) AS n_train,
                    COUNT(*) AS n_total
                FROM processed.gold_dataset
            """)
            row = cur.fetchone()
            cur.close()
        if row and row[1] and row[1] > 0:
            current_train_ratio = (row[0] or 0) / row[1]
            return "TRAIN" if current_train_ratio < target_ratio else "TEST"
    except Exception:
        pass
    # Fallback aleatorio si la consulta falla
    return "TRAIN" if random.random() < target_ratio else "TEST"


def _save_annotation(
    message_uuid: str,
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    humor_flag: bool,
    annotator_id: str,
) -> bool:
    """Guarda la anotación en validaciones_manuales y gold_dataset."""
    import random
    from datetime import date

    if odio_flag is True:
        y_odio_final = "Odio"
        y_odio_bin = 1
    elif odio_flag is False:
        y_odio_final = "No Odio"
        y_odio_bin = 0
    else:
        y_odio_final = "Dudoso"
        y_odio_bin = None

    categoria_save = _categoria_odio_for_save(odio_flag, categoria_odio)
    y_categoria = categoria_save
    y_intensidad = intensidad if odio_flag else None
    split_val = _stratified_split(target_ratio=0.85)

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                (message_uuid, odio_flag, categoria_odio, intensidad,
                 humor_flag, annotator_id, annotation_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    odio_flag = EXCLUDED.odio_flag,
                    categoria_odio = EXCLUDED.categoria_odio,
                    intensidad = EXCLUDED.intensidad,
                    humor_flag = EXCLUDED.humor_flag,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date
            """, (
                message_uuid, odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(),
            ))

            cur.execute("""
                INSERT INTO processed.gold_dataset
                (message_uuid, y_odio_final, y_odio_bin, y_categoria_final,
                 y_intensidad_final, label_source, split)
                VALUES (%s, %s, %s, %s, %s, 'human_explicit', %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    y_odio_final = EXCLUDED.y_odio_final,
                    y_odio_bin = EXCLUDED.y_odio_bin,
                    y_categoria_final = EXCLUDED.y_categoria_final,
                    y_intensidad_final = EXCLUDED.y_intensidad_final,
                    label_source = EXCLUDED.label_source
            """, (
                message_uuid, y_odio_final, y_odio_bin,
                y_categoria, y_intensidad, split_val,
            ))

            # Anotar también duplicados con mismo contenido
            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                    (message_uuid, odio_flag, categoria_odio, intensidad,
                     humor_flag, annotator_id, annotation_date)
                SELECT pm2.message_uuid, %s, %s, %s, %s, %s, %s
                FROM processed.mensajes pm2
                WHERE pm2.content_original = (
                    SELECT content_original FROM processed.mensajes
                    WHERE message_uuid = %s
                )
                  AND pm2.message_uuid != %s
                  AND pm2.message_uuid NOT IN (
                      SELECT message_uuid
                      FROM processed.validaciones_manuales
                  )
                ON CONFLICT (message_uuid) DO NOTHING
            """, (
                odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(),
                message_uuid, message_uuid,
            ))

            cur.close()

        # Invalidar cache para que las vistas reflejen la anotación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando anotación: {e}")
        return False


def _load_v510_queue() -> pd.DataFrame:
    """Carga mensajes con potencial delito Art. 510 pendientes de validación humana."""
    skipped = st.session_state.get("v510_skipped", set())

    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT ea.message_uuid,
                       ea.label_source,
                       ea.apartado_510,
                       ea.grupo_protegido,
                       ea.conducta_detectada,
                       ea.justificacion,
                       ea.confianza,
                       pm.platform,
                       pm.content_original,
                       pm.source_media,
                       rm.tweet_id AS video_id
                FROM processed.evaluacion_art510 ea
                JOIN processed.mensajes pm USING (message_uuid)
                LEFT JOIN raw.mensajes rm USING (message_uuid)
                WHERE ea.es_potencial_delito = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM processed.validacion_art510_humana vh
                      WHERE vh.message_uuid = ea.message_uuid
                        AND vh.label_source = ea.label_source
                  )
                ORDER BY
                    CASE ea.confianza
                        WHEN 'alta' THEN 1
                        WHEN 'media' THEN 2
                        ELSE 3
                    END,
                    ea.evaluacion_date DESC
                LIMIT 200
            """, conn)
    except Exception:
        return pd.DataFrame()

    if skipped and not df.empty:
        keys = df["message_uuid"].astype(str) + "|" + df["label_source"].astype(str)
        df = df[~keys.isin(skipped)]

    return df


def _load_v510_kpis(annotator_id: str, period: str = "day") -> dict:
    """KPIs de progreso de validación Art. 510."""
    fecha_desde = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT COUNT(*) FROM processed.evaluacion_art510
                WHERE es_potencial_delito = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM processed.validacion_art510_humana vh
                      WHERE vh.message_uuid = evaluacion_art510.message_uuid
                        AND vh.label_source = evaluacion_art510.label_source
                  )
            """)
            pendientes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM processed.validacion_art510_humana")
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validacion_art510_humana
                WHERE annotation_date >= %s
            """, (fecha_desde,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validacion_art510_humana
                WHERE annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        return {
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
        }
    except Exception:
        return {
            "pendientes": 0, "total_validados": 0,
            "validados_periodo": 0, "por_anotador": 0,
        }


def _save_v510_validation(
    message_uuid: str,
    label_source: str,
    validacion: str,
    apartado_final: Optional[str],
    grupo_final: Optional[str],
    conducta_final: Optional[str],
    comentario: Optional[str],
    annotator_id: str,
) -> bool:
    """Guarda la validación humana de Art. 510."""
    from datetime import date

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO processed.validacion_art510_humana
                (message_uuid, label_source, validacion_humana,
                 apartado_510_final, grupo_protegido_final, conducta_final,
                 comentario, annotator_id, annotation_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid, label_source) DO UPDATE SET
                    validacion_humana = EXCLUDED.validacion_humana,
                    apartado_510_final = EXCLUDED.apartado_510_final,
                    grupo_protegido_final = EXCLUDED.grupo_protegido_final,
                    conducta_final = EXCLUDED.conducta_final,
                    comentario = EXCLUDED.comentario,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date
            """, (
                message_uuid, label_source, validacion,
                apartado_final, grupo_final, conducta_final,
                comentario, annotator_id, date.today(),
            ))
            cur.close()
        # Invalidar cache para reflejar la validación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando validación Art. 510: {e}")
        return False


def _render_anotacion_youtube(annotator: str):
    """Contenido del tab de anotación YouTube (flujo original sin cambios)."""

    # === PASO 0: procesar guardado pendiente (antes de renderizar) ===
    pending_save = st.session_state.pop("_ann_pending_save", None)
    if pending_save is not None:
        ok = _save_annotation(**pending_save)
        if ok:
            st.session_state["ann_skipped"] = st.session_state.get(
                "ann_skipped", set()
            )
            st.session_state["ann_skipped"].discard(
                pending_save["message_uuid"]
            )
            st.session_state["_ann_last_status"] = (
                "ok", pending_save["message_uuid"][:8]
            )
        else:
            st.session_state["_ann_last_status"] = ("error", "")

    # Mostrar resultado de la última operación
    last_status = st.session_state.pop("_ann_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Anotación guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la anotación.")

    # --- KPIs de progreso ---
    _kpi_period = st.session_state.get("supervision_period", "day")
    kpis = _load_annotation_kpis(annotator, _kpi_period)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total relevantes (YT)", f"{kpis['total_relevantes']:,}")
    k2.metric("Anotados", f"{kpis['total_anotados']:,}")
    k3.metric("Pendientes", f"{kpis['pendientes']:,}")
    k4.metric("Anotados en el periodo", f"{kpis['anotados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(kpis["pct_avance"] / 100, text=f"Avance: {kpis['pct_avance']:.1f}%")

    st.divider()

    # --- Cola de mensajes ---
    if "ann_skipped" not in st.session_state:
        st.session_state["ann_skipped"] = set()

    queue = _ann_get_or_load_queue("_ann_yt_queue_cache", _load_annotation_queue)

    if queue.empty:
        st.success("No hay mensajes pendientes de anotación.")
        st.caption(
            "Si esperabas mensajes, verifica que se haya ejecutado "
            "`filtrar_relevancia_youtube.py` para generar la cola de "
            "anotación (marca `relevante_llm = 'SI'` en los candidatos)."
        )
        if st.button("Limpiar saltos y recargar"):
            st.session_state["ann_skipped"] = set()
            st.session_state.pop("_ann_yt_queue_cache", None)
            st.session_state.pop("_ann_yt_current_uuid", None)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "esos comentarios pueden volver a salir en la cola (muestra aleatoria)."
        )
        return

    # Mantener el mismo mensaje activo entre reruns hasta Guardar/Saltar.
    msg = _ann_pick_sticky_row(queue, state_key="_ann_yt_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a anotar  ({queue.shape[0]} en cola)")

    # --- Mostrar contenido y metadata ---
    col_msg, col_meta = st.columns([3, 1])
    with col_msg:
        st.markdown("**Texto del comentario:**")
        st.text_area(
            "contenido", value=str(msg["content_original"]),
            height=130, disabled=True, label_visibility="collapsed",
        )
    with col_meta:
        st.markdown(
            f"**Plataforma:** {platform_label(str(msg.get('platform') or ''))}"
        )
        _mp = _public_medio_label(msg.get("source_media"))
        if _mp:
            st.markdown(f"**Medio monitorizado:** {_mp}")
        video_id = msg.get("video_id")
        if video_id and pd.notna(video_id):
            yt_url = f"https://www.youtube.com/watch?v={video_id}"
            st.markdown(f"**Video:** [{video_id}]({yt_url})")
        terms = msg.get("matched_terms") or ""
        if terms and pd.notna(terms):
            st.markdown(f"**Términos:** `{terms}`")
        score = msg.get("relevante_score")
        if pd.notna(score):
            st.markdown(f"**Score relevancia:** {float(score):.2f}")
        motivo = msg.get("relevante_motivo")
        if motivo and pd.notna(motivo):
            st.markdown(f"**Motivo LLM:** _{motivo}_")

    st.divider()

    # --- Formulario (paso 1 fuera del st.form para poder deshabilitar 2–4 hasta elegir Odio) ---
    # fk incluye message_uuid: cada mensaje nuevo = claves nuevas en session_state (sin arrastre de la última selección).
    _inject_anotacion_form_css()
    fk = f"ann_yt_{msg_uuid}"

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Completa los siguientes 4 pasos y guarda para pasar al siguiente mensaje.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=None,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "Elegí **Odio**, **No Odio** o **Dudoso**; la pantalla se actualiza al cambiar la opción, "
            "pero **nada se guarda en la base** hasta que pulses **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al marcar No odio o Dudoso: solo se desbloquea la vista. "
            "Para registrar la clasificación y ver el **siguiente mensaje**, pulsá **Guardar y siguiente** abajo."
        )
    else:
        st.caption(
            "Completá los pasos 2 a 4 si corresponde y pulsá **Guardar y siguiente** para guardar."
        )

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=list(CATEGORIAS_LABELS.keys()),
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=None if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(2),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    # --- Procesar acciones del formulario ---
    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"

        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )

        st.session_state["_ann_pending_save"] = {
            "message_uuid": msg_uuid,
            "odio_flag": odio_flag,
            "categoria_odio": _categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            "intensidad": intensidad if es_odio else None,
            "humor_flag": humor if es_odio else False,
            "annotator_id": annotator,
        }
        st.session_state.pop("_ann_yt_current_uuid", None)
        st.session_state.pop("_ann_yt_queue_cache", None)
        st.rerun()

    if skipped:
        st.session_state["ann_skipped"].add(msg_uuid)
        st.session_state.pop("_ann_yt_current_uuid", None)
        st.session_state.pop("_ann_yt_queue_cache", None)
        st.rerun()


def _render_validacion_art510(annotator: str):
    """Contenido del tab de validación Art. 510 (X + YouTube)."""

    # === Procesar guardado pendiente ===
    pending = st.session_state.pop("_v510_pending_save", None)
    if pending is not None:
        ok = _save_v510_validation(**pending)
        if ok:
            skipped_set = st.session_state.get("v510_skipped", set())
            key = f"{pending['message_uuid']}|{pending['label_source']}"
            skipped_set.discard(key)
            st.session_state["v510_skipped"] = skipped_set
            st.session_state["_v510_last_status"] = (
                "ok", pending["message_uuid"][:8]
            )
        else:
            st.session_state["_v510_last_status"] = ("error", "")

    last_status = st.session_state.pop("_v510_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación Art. 510 guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación Art. 510.")

    # --- KPIs ---
    _kpi_period = st.session_state.get("supervision_period", "day")
    kpis = _load_v510_kpis(annotator, _kpi_period)
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pendientes Art. 510", f"{kpis['pendientes']:,}")
    k2.metric("Total validados", f"{kpis['total_validados']:,}")
    k3.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k4.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")

    st.divider()

    # --- Cola ---
    if "v510_skipped" not in st.session_state:
        st.session_state["v510_skipped"] = set()

    queue = _ann_get_or_load_queue("_v510_queue_cache", _load_v510_queue)

    if queue.empty:
        summary = load_art510_summary()
        if summary["total_evaluados"] == 0:
            st.info(
                "Aún no se ha ejecutado `evaluar_art510.py`. "
                "Una vez que se evalúen los mensajes de odio bajo el criterio del "
                "Art. 510.1, aparecerán aquí los que requieran validación humana."
            )
            # Mostrar preview de cuántos candidatos hay
            df_preview = load_art510_candidates()
            if not df_preview.empty:
                st.caption(
                    f"Hay **{len(df_preview):,}** mensajes candidatos a evaluar "
                    f"(visibles en la sección *Análisis Art. 510*)."
                )
        else:
            st.success("No hay mensajes Art. 510 pendientes de validación.")
            if summary.get("total_validados", 0) > 0:
                _render_art510_validacion_humana(summary)
        if st.button("Limpiar saltos Art. 510 y recargar", key="v510_clear"):
            st.session_state["v510_skipped"] = set()
            st.session_state.pop("_v510_queue_cache", None)
            st.session_state.pop("_v510_current_id", None)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra los pares (mensaje + fuente de etiqueta) que pasaste con **Saltar**; "
            "si siguen sin validación en la base, volverán a mostrarse como pendientes."
        )
        return

    queue = queue.copy()
    queue["_v510_id"] = (
        queue["message_uuid"].astype(str) + "|" + queue["label_source"].astype(str)
    )
    msg = _ann_pick_sticky_row(
        queue, state_key="_v510_current_id", id_col="_v510_id"
    )
    msg_uuid = str(msg["message_uuid"])
    msg_label_source = str(msg["label_source"])
    msg_key = f"{msg_uuid}|{msg_label_source}"

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    # --- Contenido y evaluación LLM ---
    col_msg, col_eval = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del mensaje:**")
        st.text_area(
            "contenido_510", value=str(msg["content_original"]),
            height=150, disabled=True, label_visibility="collapsed",
        )
        plat_raw = str(msg.get("platform", ""))
        plat = platform_label(plat_raw)
        _mp = _public_medio_label(msg.get("source_media"))
        if _mp:
            st.caption(f"Plataforma: **{plat}** · Medio monitorizado: **{_mp}**")
        else:
            st.caption(f"Plataforma: **{plat}**")

    with col_eval:
        st.markdown("**Evaluación del LLM:**")
        ap = msg.get("apartado_510") or "—"
        ap_label = APARTADO_LABELS.get(ap, ap)
        st.markdown(f"**Apartado:** {ap_label}")
        st.markdown(f"**Grupo protegido:** {msg.get('grupo_protegido') or '—'}")
        st.markdown(f"**Conducta:** {msg.get('conducta_detectada') or '—'}")
        st.markdown(f"**Confianza:** {msg.get('confianza') or '—'}")
        st.markdown(f"**Fuente etiquetado:** {LABEL_SOURCE_LABELS.get(msg_label_source, msg_label_source)}")
        just = msg.get("justificacion") or ""
        if just:
            st.markdown(f"**Justificación:** _{just}_")

    st.divider()

    # --- Formulario de validación (decisión principal fuera del form para habilitar/deshabilitar corrección) ---
    fk = "v510_" + re.sub(r"[^0-9a-zA-Z_-]", "_", msg_key)

    st.markdown("**Validación**")
    validacion = st.radio(
        "¿Es potencial delito Art. 510.1?",
        ["Confirmar", "Rechazar", "Corregir"],
        horizontal=True,
        index=None,
        key=f"{fk}_val",
        help="Confirmar: el LLM acertó. Rechazar: no es delito. Corregir: es delito pero con datos distintos.",
    )
    only_corregir = validacion == "Corregir"

    if validacion is None:
        st.caption(
            "Elegí **Confirmar**, **Rechazar** o **Corregir**; **nada se guarda** hasta pulsar "
            "**Guardar y siguiente** (o **Saltar**)."
        )
    else:
        st.caption(
            "La opción del radio **no guarda sola**: usá **Guardar y siguiente** abajo para registrar y pasar al siguiente."
        )

    with st.form(key=fk, clear_on_submit=False):
        st.markdown("---")
        if only_corregir:
            st.caption(
                "Completar apartado, grupo y conducta solo si mantienes **Corregir** "
                "(se usarán los valores del LLM si eliges **Confirmar**)."
            )
        else:
            st.caption(
                "Apartado, grupo protegido y conducta solo se editan si eliges **Corregir**; "
                "con **Confirmar** o **Rechazar** quedan deshabilitados."
            )

        apartado_opts = ["1a", "1b", "1c"]
        apartado_default = (
            apartado_opts.index(ap) if ap in apartado_opts else 0
        )
        apartado_sel = st.selectbox(
            "Apartado Art. 510.1",
            options=apartado_opts,
            format_func=lambda x: APARTADO_LABELS.get(x, x),
            index=apartado_default,
            key=f"{fk}_ap",
            disabled=not only_corregir,
        )

        grupo_sel = st.text_input(
            "Grupo protegido",
            value=msg.get("grupo_protegido") or "",
            key=f"{fk}_gp",
            help="Ej: raza, religión, orientación sexual, discapacidad...",
            disabled=not only_corregir,
        )

        conducta_sel = st.text_input(
            "Conducta detectada",
            value=msg.get("conducta_detectada") or "",
            key=f"{fk}_cond",
            disabled=not only_corregir,
        )

        comentario = st.text_area(
            "Comentario (opcional)",
            height=80,
            key=f"{fk}_comment",
        )

        st.markdown("---")
        col_save, col_skip = st.columns(2)
        submitted = col_save.form_submit_button(
            "Guardar y siguiente", type="primary", use_container_width=True,
        )
        skipped = col_skip.form_submit_button(
            "Saltar", use_container_width=True,
        )

    if submitted:
        if validacion is None:
            st.error("Selecciona una opción (Confirmar / Rechazar / Corregir).")
            return

        validacion_map = {
            "Confirmar": "confirmado",
            "Rechazar": "rechazado",
            "Corregir": "corregido",
        }

        if validacion == "Confirmar":
            ap_final = msg.get("apartado_510") or None
            gp_final = msg.get("grupo_protegido") or None
            cd_final = msg.get("conducta_detectada") or None
        elif validacion == "Corregir":
            ap_final = apartado_sel
            gp_final = grupo_sel.strip() or None
            cd_final = conducta_sel.strip() or None
        else:
            ap_final = None
            gp_final = None
            cd_final = None

        st.session_state["_v510_pending_save"] = {
            "message_uuid": msg_uuid,
            "label_source": msg_label_source,
            "validacion": validacion_map[validacion],
            "apartado_final": ap_final,
            "grupo_final": gp_final,
            "conducta_final": cd_final,
            "comentario": comentario.strip() or None,
            "annotator_id": annotator,
        }
        st.session_state.pop("_v510_current_id", None)
        st.session_state.pop("_v510_queue_cache", None)
        st.rerun()

    if skipped:
        st.session_state.setdefault("v510_skipped", set()).add(msg_key)
        st.session_state.pop("_v510_current_id", None)
        st.session_state.pop("_v510_queue_cache", None)
        st.rerun()


# ============================================================
# VALIDACIÓN ETIQUETADO LLM YOUTUBE
# ============================================================

def _load_vllm_yt_queue(clasif_filter: Optional[str] = None) -> pd.DataFrame:
    """Carga muestra aleatoria de mensajes YT con etiqueta LLM pendientes de validación humana."""
    try:
        with get_conn() as conn:
            clasif_cond = ""
            params: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params.append(clasif_filter)

            df = pd.read_sql(f"""
                SELECT DISTINCT ON (pm.content_original)
                       pm.message_uuid, pm.content_original, pm.source_media,
                       pm.created_at, rm.tweet_id AS video_id,
                       e.clasificacion_principal, e.categoria_odio_pred,
                       e.intensidad_pred, e.resumen_motivo
                FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                LEFT JOIN raw.mensajes rm USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
                ORDER BY pm.content_original, pm.created_at DESC
            """, conn, params=params)
    except Exception:
        return pd.DataFrame()

    df = df.sample(frac=1).head(100).reset_index(drop=True)

    skipped = st.session_state.get("vllm_yt_skipped", set())
    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


def _load_vllm_yt_kpis(
    annotator_id: str,
    clasif_filter: Optional[str] = None,
    period: str = "day",
) -> dict:
    """KPIs de validación de etiquetado LLM en YouTube."""
    fecha_desde = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            clasif_cond = ""
            params_pending: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params_pending.append(clasif_filter)

            cur.execute(f"""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
            """, params_pending)
            pendientes = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform = 'youtube'
            """)
            total_etiquetados_llm = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
            """)
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND vm.annotation_date >= %s
            """, (fecha_desde,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND vm.annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        pct = (total_validados / total_etiquetados_llm * 100) if total_etiquetados_llm else 0
        return {
            "total_etiquetados_llm": total_etiquetados_llm,
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
            "pct_avance": pct,
        }
    except Exception:
        return {
            "total_etiquetados_llm": 0, "pendientes": 0,
            "total_validados": 0, "validados_periodo": 0,
            "por_anotador": 0, "pct_avance": 0,
        }


def _load_vllm_x_queue(clasif_filter: Optional[str] = None) -> pd.DataFrame:
    """Cola de mensajes X/Twitter con etiqueta LLM pendientes de validación humana."""
    try:
        with get_conn() as conn:
            clasif_cond = ""
            params: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params.append(clasif_filter)

            df = pd.read_sql(f"""
                SELECT DISTINCT ON (pm.content_original)
                       pm.message_uuid, pm.content_original,
                       pm.created_at,
                       e.clasificacion_principal, e.categoria_odio_pred,
                       e.intensidad_pred, e.resumen_motivo
                FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
                ORDER BY pm.content_original, pm.created_at DESC
            """, conn, params=params)
    except Exception:
        return pd.DataFrame()

    df = df.sample(frac=1).head(100).reset_index(drop=True)

    skipped = st.session_state.get("vllm_x_skipped", set())
    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


def _load_vllm_x_kpis(
    annotator_id: str,
    clasif_filter: Optional[str] = None,
    period: str = "day",
) -> dict:
    """KPIs de validación de etiquetado LLM en X."""
    fecha_desde = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            clasif_cond = ""
            params_pending: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params_pending.append(clasif_filter)

            cur.execute(f"""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
            """, params_pending)
            pendientes = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
            """)
            total_etiquetados_llm = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
            """)
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND vm.annotation_date >= %s
            """, (fecha_desde,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND vm.annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        pct = (total_validados / total_etiquetados_llm * 100) if total_etiquetados_llm else 0
        return {
            "total_etiquetados_llm": total_etiquetados_llm,
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
            "pct_avance": pct,
        }
    except Exception:
        return {
            "total_etiquetados_llm": 0, "pendientes": 0,
            "total_validados": 0, "validados_periodo": 0,
            "por_anotador": 0, "pct_avance": 0,
        }


def _save_vllm_yt_validation(
    message_uuid: str,
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    humor_flag: bool,
    annotator_id: str,
    coincide_con_llm: Optional[bool] = None,
) -> bool:
    """Guarda validación de etiquetado LLM (YT/X) en validaciones_manuales y gold_dataset."""
    import random
    from datetime import date

    if odio_flag is True and not (categoria_odio or "").strip():
        st.error(
            "Registro anómalo: odio_flag=true sin categoría. "
            "Marcá de nuevo como Odio y elegí categoría, o revisá en BD."
        )
        return False

    categoria_save = _categoria_odio_for_save(odio_flag, categoria_odio)

    llm_clasif, llm_cat, llm_int = _fetch_llm_labels_for_uuid(message_uuid)
    coincide = _compute_coincide_con_llm(
        odio_flag,
        categoria_save,
        intensidad,
        llm_clasif,
        llm_cat,
        llm_int,
    )

    if odio_flag is True:
        y_odio_final = "Odio"
        y_odio_bin = 1
    elif odio_flag is False:
        y_odio_final = "No Odio"
        y_odio_bin = 0
    else:
        y_odio_final = "Dudoso"
        y_odio_bin = None

    y_categoria = categoria_save
    y_intensidad = intensidad if odio_flag else None
    split_val = _stratified_split(target_ratio=0.85)

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                (message_uuid, odio_flag, categoria_odio, intensidad,
                 humor_flag, annotator_id, annotation_date, coincide_con_llm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    odio_flag = EXCLUDED.odio_flag,
                    categoria_odio = EXCLUDED.categoria_odio,
                    intensidad = EXCLUDED.intensidad,
                    humor_flag = EXCLUDED.humor_flag,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date,
                    coincide_con_llm = EXCLUDED.coincide_con_llm
            """, (
                message_uuid, odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(), coincide,
            ))

            cur.execute("""
                INSERT INTO processed.gold_dataset
                (message_uuid, y_odio_final, y_odio_bin, y_categoria_final,
                 y_intensidad_final, label_source, split)
                VALUES (%s, %s, %s, %s, %s, 'llm_validated', %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    y_odio_final = EXCLUDED.y_odio_final,
                    y_odio_bin = EXCLUDED.y_odio_bin,
                    y_categoria_final = EXCLUDED.y_categoria_final,
                    y_intensidad_final = EXCLUDED.y_intensidad_final,
                    label_source = EXCLUDED.label_source
            """, (
                message_uuid, y_odio_final, y_odio_bin,
                y_categoria, y_intensidad, split_val,
            ))

            cur.close()

        # Invalidar cache para reflejar la validación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando validación LLM YT: {e}")
        return False


def _render_vllm_label_error_analysis(
    corrections_fn: Callable[[], pd.DataFrame],
    *,
    key_prefix: str,
    file_tag: str,
) -> None:
    """Panel de concordancia LLM vs Humano (YouTube o X); `corrections_fn` devuelve el DataFrame."""
    df = corrections_fn()

    if df.empty or len(df) < 3:
        st.info(
            f"Se necesitan al menos 3 validaciones para mostrar el análisis "
            f"(actualmente: {len(df)})."
        )
        return

    total = len(df)
    coincide_col = df["coincide_con_llm"]
    n_coincide = int(coincide_col.sum()) if coincide_col.notna().any() else 0
    n_corregidos = total - n_coincide
    accuracy = n_coincide / total * 100 if total else 0

    corrigio_clasif = (df["llm_clasif"] != df["humano_clasif"]).sum()
    pct_corr_clasif = corrigio_clasif / total * 100

    df_ambos_odio = df[
        (df["llm_clasif"] == "ODIO") & (df["humano_clasif"] == "ODIO")
    ]
    corrigio_cat = 0
    corrigio_int = 0
    if not df_ambos_odio.empty:
        corrigio_cat = (
            df_ambos_odio["llm_categoria"].fillna("") !=
            df_ambos_odio["humano_categoria"].fillna("")
        ).sum()
        corrigio_int = (
            df_ambos_odio["llm_intensidad"].astype(str).fillna("") !=
            df_ambos_odio["humano_intensidad"].astype(str).fillna("")
        ).sum()

    # ── KPIs ──
    st.markdown("#### Resumen de concordancia")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Validaciones", f"{total:,}")
    m2.metric("Concordancia total", f"{accuracy:.1f}%")
    m3.metric("Corrigió clasificación", f"{corrigio_clasif:,} ({pct_corr_clasif:.0f}%)")
    m4.metric("Corrigió categoría", f"{corrigio_cat:,}" if not df_ambos_odio.empty else "—")
    m5.metric("Corrigió intensidad", f"{corrigio_int:,}" if not df_ambos_odio.empty else "—")

    # ── Matriz de confusión: clasificación ──
    st.markdown("---")
    col_cm1, col_cm2 = st.columns(2)

    with col_cm1:
        st.markdown("##### Matriz de confusión — Clasificación")
        labels_order = ["ODIO", "NO_ODIO", "DUDOSO"]
        ct = pd.crosstab(
            df["llm_clasif"], df["humano_clasif"],
            rownames=["LLM"], colnames=["Humano"],
        ).reindex(index=labels_order, columns=labels_order, fill_value=0)

        fig_cm = go.Figure(data=go.Heatmap(
            z=ct.values,
            x=ct.columns.tolist(),
            y=ct.index.tolist(),
            text=ct.values,
            texttemplate="%{text}",
            colorscale="RdYlGn_r",
            showscale=False,
        ))
        fig_cm.update_layout(
            xaxis_title="Humano (gold)", yaxis_title="LLM (predicción)",
            height=340, margin=dict(t=10),
        )
        st.plotly_chart(fig_cm, use_container_width=True)

    with col_cm2:
        st.markdown("##### Tasa de corrección por dimensión")
        corr_data = pd.DataFrame({
            "Dimensión": ["Clasificación", "Categoría", "Intensidad"],
            "% Corregido": [
                pct_corr_clasif,
                (corrigio_cat / len(df_ambos_odio) * 100) if len(df_ambos_odio) else 0,
                (corrigio_int / len(df_ambos_odio) * 100) if len(df_ambos_odio) else 0,
            ],
        })
        corr_data["% Coincide"] = 100 - corr_data["% Corregido"]

        fig_corr = go.Figure()
        fig_corr.add_trace(go.Bar(
            x=corr_data["Dimensión"], y=corr_data["% Coincide"],
            name="Coincide", marker_color=COLORS["success"],
        ))
        fig_corr.add_trace(go.Bar(
            x=corr_data["Dimensión"], y=corr_data["% Corregido"],
            name="Corregido", marker_color=COLORS["danger"],
        ))
        fig_corr.update_layout(
            barmode="stack", yaxis_title="%", height=340,
            margin=dict(t=10),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    # ── Matriz de confusión: categoría (solo casos donde ambos = ODIO) ──
    if not df_ambos_odio.empty and corrigio_cat > 0:
        st.markdown("---")
        st.markdown("##### Confusión de categorías (donde LLM y humano = ODIO)")

        df_cat_cm = df_ambos_odio.dropna(subset=["llm_categoria", "humano_categoria"])
        if not df_cat_cm.empty:
            cat_order = list(CATEGORIAS_LABELS.keys())
            ct_cat = pd.crosstab(
                df_cat_cm["llm_categoria"], df_cat_cm["humano_categoria"],
                rownames=["LLM"], colnames=["Humano"],
            ).reindex(index=cat_order, columns=cat_order, fill_value=0)
            ct_cat = ct_cat.loc[
                ct_cat.sum(axis=1) > 0, ct_cat.sum(axis=0) > 0
            ]

            cat_display = {k: v.split("/")[0].strip() for k, v in CATEGORIAS_LABELS.items()}
            fig_cat = go.Figure(data=go.Heatmap(
                z=ct_cat.values,
                x=[cat_display.get(c, c) for c in ct_cat.columns],
                y=[cat_display.get(c, c) for c in ct_cat.index],
                text=ct_cat.values,
                texttemplate="%{text}",
                colorscale="RdYlGn_r",
                showscale=False,
            ))
            fig_cat.update_layout(
                xaxis_title="Humano", yaxis_title="LLM",
                height=380, margin=dict(t=10),
                xaxis_tickangle=-25,
            )
            st.plotly_chart(fig_cat, use_container_width=True)

    # ── Sesgo de intensidad ──
    if not df_ambos_odio.empty:
        df_int = df_ambos_odio.dropna(subset=["llm_intensidad", "humano_intensidad"]).copy()
        if not df_int.empty:
            st.markdown("---")
            st.markdown("##### Sesgo de intensidad")
            df_int["llm_int"] = pd.to_numeric(df_int["llm_intensidad"], errors="coerce")
            df_int["hum_int"] = pd.to_numeric(df_int["humano_intensidad"], errors="coerce")
            df_int["diff"] = df_int["llm_int"] - df_int["hum_int"]
            mean_diff = df_int["diff"].mean()

            bias_text = (
                "sin sesgo" if abs(mean_diff) < 0.1
                else f"**sobreestima** en {mean_diff:.2f} puntos" if mean_diff > 0
                else f"**subestima** en {abs(mean_diff):.2f} puntos"
            )
            st.markdown(f"Sesgo medio del LLM: {bias_text} (media diff = {mean_diff:+.2f})")

            int_ct = pd.crosstab(
                df_int["llm_int"].astype(int), df_int["hum_int"].astype(int),
                rownames=["LLM"], colnames=["Humano"],
            ).reindex(index=[1, 2, 3], columns=[1, 2, 3], fill_value=0)

            fig_int = go.Figure(data=go.Heatmap(
                z=int_ct.values,
                x=["1 Leve", "2 Ofensivo", "3 Hostil"],
                y=["1 Leve", "2 Ofensivo", "3 Hostil"],
                text=int_ct.values,
                texttemplate="%{text}",
                colorscale="YlOrRd",
                showscale=False,
            ))
            fig_int.update_layout(
                xaxis_title="Humano", yaxis_title="LLM",
                height=300, margin=dict(t=10),
            )
            st.plotly_chart(fig_int, use_container_width=True)

    # ── Tabla de correcciones ──
    st.markdown("---")
    st.markdown("##### Mensajes donde el humano corrigió al LLM")

    df_err = df[df["coincide_con_llm"] == False].copy()  # noqa: E712
    if df_err.empty:
        st.success("No hay correcciones: el LLM acertó en todos los casos validados.")
    else:
        st.caption(f"{len(df_err)} correcciones de {total} validaciones")
        display_df = df_err[[
            "content_original",
            "llm_clasif", "humano_clasif",
            "llm_categoria", "humano_categoria",
            "llm_intensidad", "humano_intensidad",
            "humano_humor", "annotator_id",
        ]].copy()
        display_df.columns = [
            "Texto", "LLM Clasif.", "Humano Clasif.",
            "LLM Categoría", "Humano Categoría",
            "LLM Intens.", "Humano Intens.",
            "Humor", "Anotador",
        ]
        display_df["LLM Categoría"] = display_df["LLM Categoría"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x) if pd.notna(x) else "—"
        )
        display_df["Humano Categoría"] = display_df["Humano Categoría"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x) if pd.notna(x) else "—"
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True,
                      key=f"{key_prefix}_errors_table")

    # ── Exportar correcciones como few-shot para el prompt ──
    st.markdown("---")
    st.markdown("##### Exportar para mejora del prompt")

    if df_err.empty:
        st.info("No hay correcciones para exportar.")
    else:
        few_shot_lines = []
        for _, row in df_err.iterrows():
            txt = str(row["content_original"])[:500]
            h_clasif = row["humano_clasif"]
            h_cat = row.get("humano_categoria") or ""
            h_int = row.get("humano_intensidad") or ""
            l_clasif = row["llm_clasif"]
            l_cat = row.get("llm_categoria") or ""

            entry = {
                "comentario": txt,
                "clasificacion_correcta": h_clasif,
                "error_del_llm": l_clasif,
            }
            if h_clasif == "ODIO":
                entry["categoria_correcta"] = h_cat
                entry["intensidad_correcta"] = str(h_int)
                if l_clasif == "ODIO" and l_cat != h_cat:
                    entry["categoria_erronea_llm"] = l_cat
            few_shot_lines.append(entry)

        few_shot_json = json.dumps(few_shot_lines, ensure_ascii=False, indent=2)

        st.download_button(
            "Descargar correcciones (JSON)",
            data=few_shot_json,
            file_name=f"correcciones_llm_{file_tag}_few_shot.json",
            mime="application/json",
            key=f"{key_prefix}_download_json",
        )

        prompt_block = "EJEMPLOS DE CORRECCIÓN (few-shot):\n"
        prompt_block += "Los siguientes son casos donde la clasificación correcta "
        prompt_block += "difiere de una predicción previa. Usalos como referencia:\n\n"
        for i, ex in enumerate(few_shot_lines[:15], 1):
            prompt_block += f"Ejemplo {i}:\n"
            prompt_block += f"  Comentario: {ex['comentario'][:200]}\n"
            prompt_block += f"  Clasificación correcta: {ex['clasificacion_correcta']}\n"
            if "categoria_correcta" in ex:
                prompt_block += f"  Categoría correcta: {ex['categoria_correcta']}\n"
                prompt_block += f"  Intensidad correcta: {ex['intensidad_correcta']}\n"
            prompt_block += f"  (El LLM había predicho: {ex['error_del_llm']}"
            if "categoria_erronea_llm" in ex:
                prompt_block += f", categoría: {ex['categoria_erronea_llm']}"
            prompt_block += ")\n\n"

        st.download_button(
            "Descargar bloque para prompt (TXT)",
            data=prompt_block,
            file_name=f"few_shot_block_prompt_{file_tag}.txt",
            mime="text/plain",
            key=f"{key_prefix}_download_prompt",
        )

        with st.expander("Vista previa del bloque few-shot"):
            st.code(prompt_block[:3000], language="text")


def _render_vllm_yt_error_analysis() -> None:
    _render_vllm_label_error_analysis(
        _load_vllm_yt_corrections, key_prefix="vllm_yt", file_tag="yt",
    )


@st.cache_data(ttl=120)
def _load_vllm_x_corrections() -> pd.DataFrame:
    """Validaciones humanas de etiquetado LLM en X (twitter / x)."""
    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT pm.message_uuid,
                       pm.content_original,
                       pm.source_media,
                       e.clasificacion_principal AS llm_clasif,
                       e.categoria_odio_pred     AS llm_categoria,
                       e.intensidad_pred         AS llm_intensidad,
                       e.resumen_motivo          AS llm_motivo,
                       CASE WHEN v.odio_flag = TRUE THEN 'ODIO'
                            WHEN v.odio_flag = FALSE THEN 'NO_ODIO'
                            ELSE 'DUDOSO' END    AS humano_clasif,
                       v.categoria_odio          AS humano_categoria,
                       v.intensidad              AS humano_intensidad,
                       v.humor_flag              AS humano_humor,
                       v.coincide_con_llm,
                       v.annotator_id,
                       v.annotation_date
                FROM processed.validaciones_manuales v
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                ORDER BY v.annotation_date DESC
            """, conn)
    except Exception:
        return pd.DataFrame()
    return df


def _render_vllm_x_error_analysis() -> None:
    _render_vllm_label_error_analysis(
        _load_vllm_x_corrections, key_prefix="vllm_x", file_tag="x",
    )


def _render_validacion_llm_youtube(annotator: str):
    """Pestaña de validación del etiquetado LLM en YouTube."""

    # === Procesar guardado pendiente ===
    pending = st.session_state.pop("_vllm_yt_pending_save", None)
    if pending is not None:
        ok = _save_vllm_yt_validation(**pending)
        if ok:
            _load_vllm_yt_corrections.clear()
            st.session_state.get("vllm_yt_skipped", set()).discard(
                pending["message_uuid"]
            )
            st.session_state["_vllm_yt_last_status"] = (
                "ok", pending["message_uuid"][:8]
            )
        else:
            st.session_state["_vllm_yt_last_status"] = ("error", "")

    last_status = st.session_state.pop("_vllm_yt_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación LLM guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación.")

    # --- Filtro por clasificación LLM ---
    clasif_options = ["Todos", "ODIO", "NO_ODIO", "DUDOSO"]
    clasif_sel = st.selectbox(
        "Filtrar por predicción LLM",
        options=clasif_options,
        index=0,
        key="vllm_yt_clasif_filter",
    )
    clasif_filter = clasif_sel if clasif_sel != "Todos" else None

    # --- KPIs ---
    _kpi_period = st.session_state.get("supervision_period", "day")
    kpis = _load_vllm_yt_kpis(annotator, clasif_filter, _kpi_period)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Etiquetados LLM (YT)", f"{kpis['total_etiquetados_llm']:,}")
    k2.metric("Validados", f"{kpis['total_validados']:,}")
    k3.metric("Pendientes" + (f" ({clasif_sel})" if clasif_filter else ""),
              f"{kpis['pendientes']:,}")
    k4.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(kpis["pct_avance"] / 100, text=f"Avance validación: {kpis['pct_avance']:.1f}%")

    # --- Panel de análisis de errores ---
    with st.expander("📊 Análisis de concordancia LLM vs Humano (YouTube)", expanded=False):
        _render_vllm_yt_error_analysis()

    st.divider()

    # --- Cola ---
    if "vllm_yt_skipped" not in st.session_state:
        st.session_state["vllm_yt_skipped"] = set()

    queue = _ann_get_or_load_queue(
        "_vllm_yt_queue_cache",
        _load_vllm_yt_queue,
        (clasif_filter,),
    )

    if queue.empty:
        if kpis["pendientes"] == 0 and kpis["total_etiquetados_llm"] > 0:
            st.success("Todos los mensajes con etiqueta LLM han sido validados.")
        elif kpis["total_etiquetados_llm"] == 0:
            st.info(
                "No hay mensajes YouTube etiquetados por el LLM. "
                "Ejecutá `etiquetar_completo_youtube_llm.py` primero."
            )
        else:
            st.info("No hay mensajes pendientes con el filtro seleccionado.")
        if st.button("Limpiar saltos y recargar", key="vllm_yt_clear"):
            st.session_state["vllm_yt_skipped"] = set()
            st.session_state.pop("_vllm_yt_queue_cache", None)
            st.session_state.pop("_vllm_yt_current_uuid", None)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "pueden volver a aparecer en la cola de validación LLM (YouTube)."
        )
        return

    msg = _ann_pick_sticky_row(queue, state_key="_vllm_yt_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    # --- Contenido y predicción LLM ---
    col_msg, col_llm = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del comentario:**")
        st.text_area(
            "contenido_vllm", value=str(msg["content_original"]),
            height=140, disabled=True, label_visibility="collapsed",
        )
        plat_v = platform_label(str(msg.get("platform") or ""))
        _mp = _public_medio_label(msg.get("source_media"))
        video_id = msg.get("video_id")
        vid_link = ""
        if video_id and pd.notna(video_id):
            yt_url = f"https://www.youtube.com/watch?v={video_id}"
            vid_link = f" · [Video]({yt_url})"
        if _mp:
            st.caption(f"Plataforma: **{plat_v}** · Medio monitorizado: **{_mp}**{vid_link}")
        else:
            st.caption(f"Plataforma: **{plat_v}**{vid_link}")

    with col_llm:
        st.markdown("**Predicción del LLM:**")
        llm_clasif = msg.get("clasificacion_principal") or "—"
        llm_cat_raw = msg.get("categoria_odio_pred") or ""
        llm_cat = CATEGORIAS_LABELS.get(llm_cat_raw, llm_cat_raw) if llm_cat_raw else "—"
        llm_int = msg.get("intensidad_pred") or "—"
        llm_motivo = msg.get("resumen_motivo") or ""

        clasif_colors = {"ODIO": "🔴", "NO_ODIO": "🟢", "DUDOSO": "🟡"}
        st.markdown(f"**Clasificación:** {clasif_colors.get(llm_clasif, '')} {llm_clasif}")
        st.markdown(f"**Categoría:** {llm_cat}")
        int_labels = {"1": "1 — Leve", "2": "2 — Ofensivo", "3": "3 — Hostil"}
        st.markdown(f"**Intensidad:** {int_labels.get(str(llm_int), str(llm_int))}")
        if llm_motivo:
            st.markdown(f"**Motivo:** _{llm_motivo}_")

    st.divider()

    # --- Formulario (paso 1 fuera del st.form para deshabilitar 2–4 si no es Odio) ---
    _inject_anotacion_form_css()
    fk = f"vllm_yt_{msg_uuid}"

    llm_odio_idx = (
        {"ODIO": 0, "NO_ODIO": 1, "DUDOSO": 2}.get(llm_clasif)
    )
    llm_cat_idx = None
    cat_keys = list(CATEGORIAS_LABELS.keys())
    if llm_cat_raw in cat_keys:
        llm_cat_idx = cat_keys.index(llm_cat_raw)
    llm_int_val = int(llm_int) if str(llm_int) in {"1", "2", "3"} else 2

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Los campos vienen precargados con la predicción del LLM. Confirma o corrige y guarda.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=llm_odio_idx,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "La predicción del LLM viene precargada; podés cambiarla. "
            "**Nada se guarda** hasta pulsar **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al cambiar a No odio o Dudoso. "
            "Pulsá **Guardar y siguiente** abajo para registrar y pasar al siguiente mensaje."
        )
    else:
        st.caption("Ajustá los pasos 2 a 4 si hace falta y pulsá **Guardar y siguiente**.")

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=cat_keys,
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=llm_cat_idx if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(llm_int_val),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    # --- Procesar acciones ---
    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"

        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )

        st.session_state["_vllm_yt_pending_save"] = {
            "message_uuid": msg_uuid,
            "odio_flag": odio_flag,
            "categoria_odio": _categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            "intensidad": intensidad if es_odio else None,
            "humor_flag": humor if es_odio else False,
            "annotator_id": annotator,
        }
        st.session_state.pop("_vllm_yt_current_uuid", None)
        st.session_state.pop("_vllm_yt_queue_cache", None)
        st.rerun()

    if skipped:
        st.session_state.setdefault("vllm_yt_skipped", set()).add(msg_uuid)
        st.session_state.pop("_vllm_yt_current_uuid", None)
        st.session_state.pop("_vllm_yt_queue_cache", None)
        st.rerun()


def _render_validacion_llm_x(annotator: str):
    """Pestaña de validación del etiquetado LLM en X (Twitter)."""

    pending = st.session_state.pop("_vllm_x_pending_save", None)
    if pending is not None:
        ok = _save_vllm_yt_validation(**pending)
        if ok:
            _load_vllm_x_corrections.clear()
            st.session_state.get("vllm_x_skipped", set()).discard(
                pending["message_uuid"]
            )
            st.session_state["_vllm_x_last_status"] = (
                "ok", pending["message_uuid"][:8]
            )
        else:
            st.session_state["_vllm_x_last_status"] = ("error", "")

    last_status = st.session_state.pop("_vllm_x_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación LLM guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación.")

    clasif_options = ["Todos", "ODIO", "NO_ODIO", "DUDOSO"]
    clasif_sel = st.selectbox(
        "Filtrar por predicción LLM",
        options=clasif_options,
        index=0,
        key="vllm_x_clasif_filter",
    )
    clasif_filter = clasif_sel if clasif_sel != "Todos" else None

    _kpi_period = st.session_state.get("supervision_period", "day")
    kpis = _load_vllm_x_kpis(annotator, clasif_filter, _kpi_period)
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Etiquetados LLM (X)", f"{kpis['total_etiquetados_llm']:,}")
    k2.metric("Validados", f"{kpis['total_validados']:,}")
    k3.metric("Pendientes" + (f" ({clasif_sel})" if clasif_filter else ""),
              f"{kpis['pendientes']:,}")
    k4.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(kpis["pct_avance"] / 100, text=f"Avance validación: {kpis['pct_avance']:.1f}%")

    with st.expander("📊 Análisis de concordancia LLM vs Humano (X / Twitter)", expanded=False):
        _render_vllm_x_error_analysis()

    st.divider()

    if "vllm_x_skipped" not in st.session_state:
        st.session_state["vllm_x_skipped"] = set()

    queue = _ann_get_or_load_queue(
        "_vllm_x_queue_cache",
        _load_vllm_x_queue,
        (clasif_filter,),
    )

    if queue.empty:
        if kpis["pendientes"] == 0 and kpis["total_etiquetados_llm"] > 0:
            st.success("Todos los mensajes con etiqueta LLM han sido validados.")
        elif kpis["total_etiquetados_llm"] == 0:
            st.info(
                "No hay mensajes X/Twitter etiquetados por el LLM. "
                "Ejecutá el pipeline de etiquetado LLM para X primero."
            )
        else:
            st.info("No hay mensajes pendientes con el filtro seleccionado.")
        if st.button("Limpiar saltos y recargar", key="vllm_x_clear"):
            st.session_state["vllm_x_skipped"] = set()
            st.session_state.pop("_vllm_x_queue_cache", None)
            st.session_state.pop("_vllm_x_current_uuid", None)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "pueden volver a aparecer en la cola de validación LLM (X/Twitter)."
        )
        return

    msg = _ann_pick_sticky_row(queue, state_key="_vllm_x_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    col_msg, col_llm = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del mensaje:**")
        st.text_area(
            "contenido_vllm_x", value=str(msg["content_original"]),
            height=140, disabled=True, label_visibility="collapsed",
        )

    with col_llm:
        st.markdown("**Predicción del LLM:**")
        llm_clasif = msg.get("clasificacion_principal") or "—"
        llm_cat_raw = msg.get("categoria_odio_pred") or ""
        llm_cat = CATEGORIAS_LABELS.get(llm_cat_raw, llm_cat_raw) if llm_cat_raw else "—"
        llm_int = msg.get("intensidad_pred") or "—"
        llm_motivo = msg.get("resumen_motivo") or ""

        clasif_colors = {"ODIO": "🔴", "NO_ODIO": "🟢", "DUDOSO": "🟡"}
        st.markdown(f"**Clasificación:** {clasif_colors.get(llm_clasif, '')} {llm_clasif}")
        st.markdown(f"**Categoría:** {llm_cat}")
        int_labels = {"1": "1 — Leve", "2": "2 — Ofensivo", "3": "3 — Hostil"}
        st.markdown(f"**Intensidad:** {int_labels.get(str(llm_int), str(llm_int))}")
        if llm_motivo:
            st.markdown(f"**Motivo:** _{llm_motivo}_")

    st.divider()

    _inject_anotacion_form_css()
    fk = f"vllm_x_{msg_uuid}"

    llm_odio_idx = (
        {"ODIO": 0, "NO_ODIO": 1, "DUDOSO": 2}.get(llm_clasif)
    )
    llm_cat_idx = None
    cat_keys = list(CATEGORIAS_LABELS.keys())
    if llm_cat_raw in cat_keys:
        llm_cat_idx = cat_keys.index(llm_cat_raw)
    llm_int_val = int(llm_int) if str(llm_int) in {"1", "2", "3"} else 2

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Los campos vienen precargados con la predicción del LLM. Confirma o corrige y guarda.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=llm_odio_idx,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "La predicción del LLM viene precargada; podés cambiarla. "
            "**Nada se guarda** hasta pulsar **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al cambiar a No odio o Dudoso. "
            "Pulsá **Guardar y siguiente** abajo para registrar y pasar al siguiente mensaje."
        )
    else:
        st.caption("Ajustá los pasos 2 a 4 si hace falta y pulsá **Guardar y siguiente**.")

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=cat_keys,
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=llm_cat_idx if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(llm_int_val),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"

        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )

        st.session_state["_vllm_x_pending_save"] = {
            "message_uuid": msg_uuid,
            "odio_flag": odio_flag,
            "categoria_odio": _categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            "intensidad": intensidad if es_odio else None,
            "humor_flag": humor if es_odio else False,
            "annotator_id": annotator,
        }
        st.session_state.pop("_vllm_x_current_uuid", None)
        st.session_state.pop("_vllm_x_queue_cache", None)
        st.rerun()

    if skipped:
        st.session_state.setdefault("vllm_x_skipped", set()).add(msg_uuid)
        st.session_state.pop("_vllm_x_current_uuid", None)
        st.session_state.pop("_vllm_x_queue_cache", None)
        st.rerun()


def _render_supervision_panel(period: str) -> None:
    """Panel de supervisión: conteos por subsección y tabla anotador × subsección."""
    data = _load_admin_annotation_supervision(period)
    summary = data.get("summary", {})
    by_annotator = data.get("by_annotator", pd.DataFrame())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("YT Odio", f"{summary.get('YT Odio', 0):,}")
    c2.metric("Art. 510", f"{summary.get('Art.510', 0):,}")
    c3.metric("LLM YouTube", f"{summary.get('LLM YT', 0):,}")
    c4.metric("LLM X", f"{summary.get('LLM X', 0):,}")

    st.markdown("**Detalle por anotador**")
    if by_annotator is not None and not by_annotator.empty:
        st.dataframe(by_annotator, use_container_width=True, hide_index=True)
    else:
        st.info("No hay anotaciones registradas en el periodo seleccionado.")
    st.caption(
        "Los conteos reflejan mensajes etiquetados o re-etiquetados en el periodo "
        "(no necesariamente primera anotación)."
    )


def render_anotacion():
    """Sección de anotación humana: YouTube, Art. 510 y validación LLM (YT + X)."""
    if not _require_role("admin", "editor", section="Anotación y validación"):
        return
    _render_section_header(
        "Anotación y validación",
        "Anotación en YouTube, validación Art. 510 (X + YouTube) y control de calidad del etiquetado LLM.",
    )

    user_role = st.session_state.get("user_role")
    if user_role in ("admin", "editor"):
        st.subheader("Supervisión de anotaciones")
        period = st.radio(
            "Periodo",
            options=["day", "week", "month"],
            format_func=lambda x: {
                "day": "Último día",
                "week": "Última semana",
                "month": "Último mes",
            }[x],
            horizontal=True,
            key="supervision_period",
        )
        _render_supervision_panel(period)
        st.divider()

    # --- Identificación del anotador (derivada del usuario autenticado) ---
    # El annotator_id se fija a partir de la sesión autenticada para garantizar
    # la integridad de la autoría en el gold dataset.
    # Los admin pueden sobreescribirlo (para anotar en nombre de otro usuario).
    session_user = st.session_state.get("user_name", "")
    user_role = st.session_state.get("user_role")

    if user_role == "admin":
        annotator = st.text_input(
            "Nombre / ID de anotador",
            value=st.session_state.get("annotator_id", session_user),
            placeholder="Ej: CIEDES, Anotador1...",
            key="ann_id_input",
            help="Admin: podés cambiar el ID para anotar en nombre de otro anotador.",
        )
        if annotator:
            st.session_state["annotator_id"] = annotator.strip()
    else:
        # Editor: annotator_id fijo al usuario de sesión (no editable)
        annotator = session_user
        st.session_state["annotator_id"] = session_user
        st.caption(f"Anotando como: **{session_user}**")

    if not annotator.strip():
        st.info("No se pudo determinar tu ID de anotador. Iniciá sesión nuevamente.")
        return

    # --- Tabs ---
    tab_yt, tab_510, tab_vllm_yt, tab_vllm_x = st.tabs([
        "Anotación odio YouTube",
        "Validación Art. 510 (X + YouTube)",
        "Validación etiquetado LLM (YT)",
        "Validación Etiquetado LLM X",
    ])

    with tab_yt:
        _render_anotacion_youtube(annotator.strip())

    with tab_510:
        _render_validacion_art510(annotator.strip())

    with tab_vllm_yt:
        _render_validacion_llm_youtube(annotator.strip())

    with tab_vllm_x:
        _render_validacion_llm_x(annotator.strip())


# ============================================================
# PROYECTO ReTo – Sección institucional
# ============================================================
_CARD_CSS = """
<style>
.reto-hero {
    background: linear-gradient(135deg, #1a3a5c 0%, #2b6cb0 100%);
    color: white;
    padding: 2.5rem 2rem;
    border-radius: 12px;
    margin-bottom: 1.5rem;
}
.reto-hero h1 { color: white; margin: 0 0 0.3rem 0; font-size: 2.2rem; }
.reto-hero h3 { color: #bee3f8; margin: 0 0 1.2rem 0; font-weight: 400; }
.reto-hero p  { color: #e2e8f0; font-size: 1.05rem; line-height: 1.6; margin: 0; }

.reto-card {
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1.4rem 1.5rem;
    height: 100%;
}
.reto-card h4 {
    color: #1F4E79;
    margin: 0 0 0.8rem 0;
    font-size: 1.05rem;
    border-bottom: 2px solid #bee3f8;
    padding-bottom: 0.5rem;
}
.reto-card ul { padding-left: 1.2rem; margin: 0; }
.reto-card li { color: #4a5568; margin-bottom: 0.3rem; font-size: 0.95rem; }
.reto-card .card-note {
    color: #718096;
    font-style: italic;
    font-size: 0.85rem;
    margin-top: 0.8rem;
}

.reto-flow {
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    padding: 1.5rem 2rem;
    margin-bottom: 1rem;
}
.reto-flow-step {
    display: flex;
    align-items: flex-start;
}
.reto-flow-left {
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 44px;
}
.reto-flow-num {
    width: 38px; height: 38px; border-radius: 50%;
    background: linear-gradient(135deg, #2b6cb0, #3182ce);
    color: white; font-weight: 700; font-size: 15px;
    display: flex; align-items: center; justify-content: center;
    box-shadow: 0 2px 6px rgba(43,108,176,0.3);
    flex-shrink: 0;
}
.reto-flow-line {
    width: 2px; height: 22px;
    background: linear-gradient(180deg, #3182ce, #bee3f8);
    margin: 0;
}
.reto-flow-text {
    margin-left: 14px;
    padding-top: 4px;
}
.reto-flow-text strong { color: #2d3748; font-size: 0.98rem; }
.reto-flow-text span  { color: #718096; font-size: 0.88rem; }

.reto-principle {
    text-align: center;
    padding: 1rem 0.8rem;
    background: #f7fafc;
    border: 1px solid #e2e8f0;
    border-radius: 10px;
    height: 100%;
}
.reto-principle .p-icon {
    font-size: 1.6rem;
    margin-bottom: 0.4rem;
}
.reto-principle strong { color: #2b6cb0; font-size: 0.95rem; }
.reto-principle p { color: #718096; font-size: 0.82rem; margin: 0.3rem 0 0 0; }

.reto-alert {
    background: #ebf8ff;
    border-left: 4px solid #3182ce;
    padding: 0.8rem 1.2rem;
    border-radius: 0 8px 8px 0;
    color: #2c5282;
    font-size: 0.95rem;
    margin-top: 0.5rem;
}

/* Proyecto ReTo: misma altura imagen y texto (grid + fondo cover; evita fallos de img height:100% en Streamlit) */
.reto-proyecto-top {
    display: grid;
    grid-template-columns: minmax(0, 2fr) minmax(0, 3fr);
    align-items: stretch;
    gap: 1.25rem;
    margin-bottom: 1rem;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
}
.reto-proyecto-top__img-wrap {
    position: relative;
    min-width: 0;
    min-height: 0;
    align-self: stretch;
    border-radius: 10px;
    overflow: hidden;
    box-shadow: 0 2px 10px rgba(0,0,0,0.08);
    background-color: #edf2f7;
}
.reto-proyecto-top__img-wrap img {
    position: absolute;
    inset: 0;
    width: 100%;
    height: 100%;
    object-fit: cover;
    object-position: center;
    display: block;
    margin: 0;
    padding: 0;
}
.reto-proyecto-top__text {
    min-width: 0;
    font-size: 0.95rem;
    line-height: 1.55;
    color: #2d3748;
}
.reto-proyecto-top__text p { margin: 0 0 0.65rem 0; }
.reto-proyecto-top__text ul { margin: 0.35rem 0 0 1.1rem; padding: 0; }
.reto-proyecto-top__text li { margin-bottom: 0.35rem; }
.reto-proyecto-top--solo-texto {
    grid-template-columns: minmax(0, 1fr);
}
.reto-proyecto-consorcio {
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    clear: both;
}
.reto-proyecto-consorcio table {
    width: 100% !important;
    border-collapse: collapse;
}
</style>
"""


def _proyecto_consorcio_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Organización": "CIFAL Málaga", "Rol": "Coordinación"},
            {"Organización": "Fundación CIEDES", "Rol": "Investigación y datos"},
            {"Organización": "Movimiento Contra la Intolerancia (MCI)", "Rol": "Concienciación y apoyo a víctimas"},
            {"Organización": "Colegio Profesional de Periodistas de Andalucía (CPPA)", "Rol": "Ética en medios"},
            {"Organización": "Comité Olímpico Español (COE)", "Rol": "Deporte e inclusión"},
            {"Organización": "Asociación La Guajira", "Rol": "Cultura y arte"},
        ]
    )


def _render_proyecto_consorcio_y_actividades() -> None:
    consorcio_df = _proyecto_consorcio_df()
    if _is_viewer():
        st.markdown(
            "<h3 style='color:#1F4E79; margin:1.25rem 0 0.75rem 0;'>Consorcio y paquetes de trabajo</h3>",
            unsafe_allow_html=True,
        )
        st.dataframe(consorcio_df, use_container_width=True, hide_index=True)
        st.caption(
            "Otros socios: Universidad de Almería, Almería Acoge, Yo Soy El Otro."
        )
    else:
        st.markdown(
            '<div class="reto-proyecto-consorcio">'
            "<strong>Consorcio y paquetes de trabajo</strong>"
            f"{consorcio_df.to_html(index=False, justify='center', classes=['dataframe'])}"
            "<p>Otros socios: Universidad de Almería, Almería Acoge, Yo Soy El Otro.</p>"
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("**Alcance y actividades destacadas**")
    col_formacion, col_otras = st.columns(2)
    with col_formacion:
        st.markdown("**Formación y sensibilización**")
        st.markdown(
            "- **Fuerzas de seguridad:** capacitación para investigación y asesoramiento a víctimas "
            "(reducción de la infra denuncia).\n"
            "- **Periodistas y medios:** tratamiento ético, identificación de discursos de odio y lucha contra la desinformación.\n"
            "- **Deporte:** formación de entrenadores y voluntarios en valores olímpicos, igualdad y espacios seguros.\n"
            "- **Comunidad y jóvenes:** talleres culturales para desafiar estereotipos."
        )
    with col_otras:
        st.markdown("**Otras líneas de trabajo**")
        st.markdown(
            "- **Tecnología e IA:** base de datos y herramientas para monitorizar el odio en redes "
            "(incl. análisis predictivo y OSINT).\n"
            "- **Apoyo a víctimas:** puntos de atención con asistencia legal.\n"
            "- Eventos deportivos y culturales para cohesión social.\n"
            "- Producción audiovisual y estudios abiertos al público."
        )

    st.caption(
        "Los paneles siguientes de este dashboard describen el análisis digital y la metodología del componente "
        "de monitorización; no sustituyen la información institucional completa del consorcio."
    )


def _render_proyecto_intro_viewer() -> None:
    """Introducción institucional sin portada (solo perfil visualizador)."""
    col_quien, col_obj = st.columns(2)
    with col_quien:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Red de Tolerancia (ReTo)</h4>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0;">
                    Iniciativa estratégica europea financiada por el programa
                    <strong>CERV-2024-CHAR-LITI</strong> de la Unión Europea, orientada a combatir
                    el discurso y los delitos de odio en España (24 meses: junio 2025 – mayo 2027).
                </p>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0.75rem 0 0 0;">
                    Andalucía actúa como <strong>modelo regional</strong> para su posterior
                    replicación a nivel nacional.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col_obj:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Objetivos</h4>
                <p style="color:#2d3748; font-size:0.93rem; margin:0 0 0.5rem 0;"><strong>General</strong></p>
                <p style="color:#4a5568; font-size:0.95rem; line-height:1.55; margin:0 0 0.85rem 0;">
                    Crear un marco integral de colaboración entre sociedad civil, autoridades públicas
                    y agentes comunitarios para prevenir y responder al odio.
                </p>
                <p style="color:#2d3748; font-size:0.93rem; margin:0 0 0.5rem 0;"><strong>Específicos</strong></p>
                <ul style="margin:0; padding-left:1.15rem; color:#4a5568; font-size:0.95rem; line-height:1.5;">
                    <li>Coordinación entre fuerzas del orden y organizaciones civiles.</li>
                    <li>Recopilación de datos con herramientas avanzadas (incl. IA).</li>
                    <li>Trabajo desde cultura, deporte y medios de comunicación.</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _render_proyecto_intro_con_imagen() -> None:
    """Bloque superior con portada + texto (admin y editor)."""
    _portada_path = _reto_asset_file("ppt_assets", "Portada_manual_Reto.jpg") or _reto_asset_file(
        "ppt_assets", "Portada_manual_Reto.png"
    )
    _portada_b64 = ""
    if _portada_path is not None:
        try:
            _portada_b64 = base64.b64encode(_portada_path.read_bytes()).decode("ascii")
        except OSError:
            _portada_b64 = ""

    _proyecto_texto_html = """
<div class="reto-proyecto-top__text">
    <p><strong>Red de Tolerancia</strong> — El proyecto Red de Tolerancia (ReTo) es una iniciativa estratégica europea,
    financiada por el programa CERV-2024-CHAR-LITI de la Unión Europea, orientada a combatir el discurso
    y los delitos de odio en España. Con una duración de 24 meses (junio 2025 – mayo 2027), toma a Andalucía
    como modelo regional para su posterior replicación a nivel nacional.</p>
    <p><strong>Objetivo general</strong> — Crear un marco integral de colaboración entre la sociedad civil, autoridades públicas
    y agentes comunitarios para fortalecer la capacidad de prevenir y responder al odio.</p>
    <p><strong>Objetivos específicos</strong></p>
    <ul>
        <li>Mejorar la coordinación entre fuerzas del orden y organizaciones civiles para facilitar la denuncia.</li>
        <li>Fomentar la recopilación de datos con herramientas avanzadas (incl. IA) para entender tendencias del odio.</li>
        <li>Trabajar el tema desde la cultura, el deporte y los medios de comunicación.</li>
    </ul>
</div>
"""

    if _portada_b64:
        _data_uri = f"data:image/png;base64,{_portada_b64}"
        st.markdown(
            f'<div class="reto-proyecto-top">'
            f'<div class="reto-proyecto-top__img-wrap">'
            f'<img src="{_data_uri}" alt="Portada Manual ReTo" loading="lazy" />'
            f"</div>{_proyecto_texto_html}</div>",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            f'<div class="reto-proyecto-top reto-proyecto-top--solo-texto">{_proyecto_texto_html}</div>',
            unsafe_allow_html=True,
        )


def render_proyecto():
    st.markdown(_CARD_CSS, unsafe_allow_html=True)

    _render_section_header(
        "Proyecto ReTo",
        "Marco europeo CERV, consorcio y alcance del análisis digital en medios andaluces.",
    )

    if _is_viewer():
        _render_proyecto_intro_viewer()
    else:
        st.markdown("<br>", unsafe_allow_html=True)
        _render_proyecto_intro_con_imagen()

    if _is_viewer():
        with st.expander("ℹ️ Sobre esta plataforma"):
            st.markdown(
                """
                **Alcance del análisis**
                Esta plataforma monitoriza comentarios públicos en perfiles oficiales de medios
                de comunicación andaluces en **X (Twitter)** y **YouTube**. No se accede a
                información privada, mensajes directos ni perfiles personales.

                **Metodología**
                Los comentarios se clasifican mediante un sistema de inteligencia artificial (IA)
                en 6 categorías de discurso de odio, validado con revisión humana experta.
                Los resultados reflejan tendencias observadas, no un censo exhaustivo de todo
                el contenido publicado en las plataformas.

                **Limitaciones**
                - La clasificación automática puede contener errores; los datos se revisan
                  periódicamente por el equipo del proyecto.
                - El volumen recogido depende de las cuotas de las APIs de cada plataforma.
                - Los datos de X se actualizan los lunes y jueves; YouTube, con menor frecuencia.
                - Esta herramienta es de uso investigador y no tiene valor probatorio legal.

                **Proyecto**
                ReTo es una iniciativa financiada por el programa CERV-2024-CHAR-LITI
                de la Unión Europea. Más información en la sección *Proyecto ReTo*.
                """
            )

    _render_proyecto_consorcio_y_actividades()

    # --- Hero (sin repetir el título de página; ya está en la cabecera de sección) ---
    st.markdown(
        """
        <div class="reto-hero">
            <h2 style="color:white; margin:0 0 0.45rem 0; font-size:1.65rem; font-weight:700;">
                Componente de monitorización digital
            </h2>
            <h3>Red de Tolerancia contra los delitos de odio</h3>
            <p>
                ReTo es una iniciativa orientada al análisis, comprensión y prevención
                del discurso y los delitos de odio en Andalucía. Integra análisis
                estructurado de interacciones digitales, etiquetado humano experto,
                integración con estadísticas oficiales y desarrollo metodológico documentado.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # --- Alcance y Objetivos lado a lado ---
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Alcance del Análisis Digital</h4>
                <p style="color:#4a5568; font-size:0.95rem; margin:0 0 0.6rem 0;">
                    Comentarios públicos de usuarios en contenidos de medios de
                    comunicación andaluces previamente definidos.
                </p>
                <ul>
                    <li>Perfiles oficiales de medios andaluces en <strong>YouTube</strong></li>
                    <li>Perfiles oficiales de medios andaluces en <strong>X</strong> (Twitter)</li>
                </ul>
                <div class="card-note">
                    No se accede a información privada ni perfiles cerrados.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Objetivos del Análisis</h4>
                <ul>
                    <li>Identificar patrones de hostilidad en el debate digital</li>
                    <li>Clasificar tipologías de discurso</li>
                    <li>Analizar intensidad y target predominante</li>
                    <li>Detectar dinámicas recurrentes</li>
                    <li>Generar evidencia complementaria a datos oficiales</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown(
        '<div class="reto-alert">'
        "Este proyecto <strong>no</strong> constituye un sistema de vigilancia "
        "de usuarios ni un mecanismo automatizado de denuncia."
        "</div>",
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Metodología en 3 cards ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Enfoque Metodológico</h3>",
        unsafe_allow_html=True,
    )
    m1, m2, m3 = st.columns(3)
    with m1:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Herramientas Automatizadas</h4>
                <ul>
                    <li>Normalización lingüística</li>
                    <li>Diccionario optimizado</li>
                    <li>Detección preliminar de términos</li>
                    <li>Filtrado de volumen</li>
                </ul>
                <div class="card-note">
                    Las herramientas automatizadas no determinan la etiqueta final.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m2:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Etiquetado Humano Experto</h4>
                <p style="color:#4a5568; font-size:0.93rem; margin:0 0 0.5rem 0;">
                    Clasificación final por anotadores formados (Manual ReTo):
                </p>
                <ul>
                    <li>ODIO / NO ODIO / DUDOSO</li>
                    <li>Categoría</li>
                    <li>Intensidad</li>
                    <li>Humor</li>
                </ul>
                <div class="card-note">
                    La evaluación humana es el elemento central del proceso.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            """
            <div class="reto-card">
                <h4>Registro y Trazabilidad</h4>
                <ul>
                    <li>Auditoría del etiquetado</li>
                    <li>Registro de lotes de procesamiento</li>
                    <li>Anonimización irreversible (hashing)</li>
                    <li>Documentación completa del flujo técnico</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Flujo visual ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Flujo Metodológico</h3>",
        unsafe_allow_html=True,
    )
    flow_steps = [
        ("1", "Captura de Comentarios", "Recolección de datos públicos de YouTube y X"),
        ("2", "Preprocesamiento Automatizado", "Normalización + Diccionario + Filtrado"),
        ("3", "Pre-etiquetado Técnico", "Selección de candidatos"),
        ("4", "Etiquetado Humano Experto", "ODIO / NO ODIO / DUDOSO + Categoría + Intensidad"),
        ("5", "Integración en Base de Datos", "PostgreSQL + Audit Log"),
        ("6", "Análisis y Visualización", "Dashboards + Cruce con datos oficiales"),
    ]
    flow_html = '<div class="reto-flow">'
    for i, (num, title, desc) in enumerate(flow_steps):
        flow_html += (
            '<div class="reto-flow-step">'
            '<div class="reto-flow-left">'
            f'<div class="reto-flow-num">{num}</div>'
        )
        if i < len(flow_steps) - 1:
            flow_html += '<div class="reto-flow-line">&nbsp;</div>'
        flow_html += (
            "</div>"
            '<div class="reto-flow-text">'
            f"<strong>{title}</strong><br>"
            f"<span>{desc}</span>"
            "</div></div>"
        )
    flow_html += "</div>"
    st.markdown(flow_html, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Principios ---
    st.markdown(
        "<h3 style='color:#1F4E79; margin-bottom:0.8rem;'>Principios del Proyecto</h3>",
        unsafe_allow_html=True,
    )
    principles = [
        ("Rigor metodológico", "Procesos documentados y replicables"),
        ("Transparencia", "Flujos abiertos y auditables"),
        ("Protección de datos", "Cumplimiento normativo estricto"),
        ("Anonimización estricta", "Hashing irreversible de identidades"),
        ("Complementariedad", "Integración con estadísticas institucionales"),
        ("Mejora continua", "Iteración permanente del marco analítico"),
    ]
    p_cols = st.columns(3)
    for idx, (title, desc) in enumerate(principles):
        with p_cols[idx % 3]:
            st.markdown(
                f"""
                <div class="reto-principle">
                    <strong>{title}</strong>
                    <p>{desc}</p>
                </div>
                """,
                unsafe_allow_html=True,
            )


# ============================================================
# BUSCADOR Y ANÁLISIS — búsqueda por término y análisis agregado
# ============================================================

_INTENSIDAD_NUMERICA = {"1": 1, "2": 2, "3": 3}

_MEDIOS_AUDITADOS = {
    # X
    "DiarioDAlmeria", "101tvMalaga", "diariosevilla", "DiarioJAENes",
    "granadahoy", "europapress", "ideal_granada", "canalsur",
    "EFEnoticias", "AhoraGranada", "granadadigital", "OndaCero_es",
    "DiarioSUR", "HoraJaen", "malaga_ee", "opiniondemalaga",
    "JaenHoyDiario", "CadizDirecto", "ElSoldAntequera", "IndeGranada",
    "MijasCom", "8Directo_", "noticiasmira", "DiarioAvanza",
    "101tvAntequera", "lacontradejaen", "EstrechoDigital", "andaluciainf",
    "9laLoma", "Sur_de_Cordoba", "motrildigital", "NoticiasGr",
    "RTVMarbella", "LosBarriosHoy", "CastilloSanFdo", "DIARIOBC",
    "CordobaBN", "lasemana", "AljarafeDigital", "7TVAndalucia",
    "fuengirolatv", "IInfoLinares",
    # YouTube
    "El País – Edición Andalucía", "Onda Cero Andalucía",
    "Europa Press Andalucía", "COPE Andalucía", "Agencia EFE",
    "Betis TV", "Cádiz Directo",
}


@st.cache_data(ttl=300)
def _load_buscador_resultados(termino: str) -> Tuple[pd.DataFrame, bool]:
    """
    Busca mensajes cuyo content_original contenga el término (ILIKE).
    Devuelve (df, truncado). Si hay >5000 filas, corta a 5000 y truncado=True.
    """
    if not termino:
        return pd.DataFrame(), False

    pattern = f"%{termino}%"
    with get_conn() as conn:
        df = pd.read_sql(
            """
            SELECT
                m.message_uuid,
                m.content_original,
                m.source_media,
                m.platform,
                m.created_at,
                e.clasificacion_principal,
                e.categoria_odio_pred,
                e.intensidad_pred,
                e.resumen_motivo
            FROM processed.mensajes m
            LEFT JOIN processed.etiquetas_llm e ON m.message_uuid = e.message_uuid
            WHERE m.content_original ILIKE %(termino)s
              AND m.source_media = ANY(%(medios)s)
            ORDER BY m.created_at DESC
            LIMIT 5001
            """,
            conn,
            params={
                "termino": pattern,
                "medios": list(_MEDIOS_AUDITADOS),
            },
        )

    truncado = len(df) > 5000
    if truncado:
        df = df.head(5000).copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df, truncado


def render_buscador_terminos() -> None:
    _render_section_header(
        "Buscador y análisis",
        "Búsqueda de términos o frases en el corpus monitorizado (reacciones agregadas, sin identificar usuarios).",
    )

    # 2) Input + 3) Toggle LLM
    col_input, col_toggle = st.columns([3, 2])
    with col_input:
        termino = st.text_input(
            "Término o frase a buscar",
            key="buscador_terminos_input",
            placeholder="Ej.: inmigración, okupas, vivienda, etc.",
        ).strip()
    with col_toggle:
        solo_llm = st.checkbox(
            _ui_label("Analizar solo mensajes clasificados por LLM"),
            value=False,
            key="buscador_terminos_only_llm",
        )
    plataforma = st.radio(
        "Plataforma",
        options=["Todas", "X", "YouTube"],
        horizontal=True,
        key="buscador_plataforma",
    )

    if not termino:
        st.info("Ingresá un término para comenzar.")
        return

    df_all, truncado = _load_buscador_resultados(termino)

    # Reset paginación si cambió el término
    if st.session_state.get("buscador_ultimo_termino") != termino:
        st.session_state["buscador_page_idx"] = 0
        st.session_state["buscador_ultimo_termino"] = termino

    if truncado:
        st.warning("Resultado amplio — mostrando primeros 5000 mensajes.")

    if df_all.empty:
        st.info("No se encontraron mensajes para ese término.")
        return

    # Filtro toggle LLM
    if solo_llm:
        df = df_all[df_all["clasificacion_principal"].notna()].copy()
    else:
        df = df_all.copy()
    if plataforma == "X":
        df = df[df["platform"] == "x"].copy()
    elif plataforma == "YouTube":
        df = df[df["platform"] == "youtube"].copy()

    if df.empty:
        st.info("No se encontraron mensajes para los filtros seleccionados.")
        return

    # Texto anonimizado reusable
    df["content_clean"] = (
        df["content_original"].fillna("").astype(str).map(_anonimizar_texto_mensaje)
    )

    # 4) KPIs
    total = len(df)
    medios_distintos = int(df["source_media"].nunique())
    total_clasificados = int(df["clasificacion_principal"].notna().sum())
    pct_clasif = (total_clasificados / total * 100.0) if total else 0.0
    fmin = df["created_at"].min()
    fmax = df["created_at"].max()

    if solo_llm:
        k1, k2, k3, k4, k5 = st.columns(5)
    else:
        k1, k2, k3, k4 = st.columns(4)
    k1.metric("Menciones encontradas", f"{total:,}")
    k2.metric("Medios distintos", f"{medios_distintos:,}")
    k3.metric(_ui_label("% clasificado por LLM"), f"{pct_clasif:.1f}%")
    periodo_txt = (
        f"{fmin.strftime('%d/%m/%y')} → {fmax.strftime('%d/%m/%y')}"
        if pd.notna(fmin) and pd.notna(fmax)
        else "—"
    )
    k4.markdown(
        f"**Período cubierto**<br><span style='font-size:0.8rem'>{periodo_txt}</span>",
        unsafe_allow_html=True,
    )

    if solo_llm:
        df_odio = df[df["clasificacion_principal"] == "ODIO"].copy()
        df_odio["intens_num"] = df_odio["intensidad_pred"].map(_INTENSIDAD_NUMERICA)
        intens_media = df_odio["intens_num"].mean()
        intens_txt = f"{intens_media:.2f} / 3" if pd.notna(intens_media) else "—"
        k5.metric("Intensidad media (odio)", intens_txt)

    # 5) Evolución temporal
    st.markdown("### Evolución de menciones en el tiempo")
    df_sem = df.copy()
    df_sem["semana"] = (
        pd.to_datetime(df_sem["created_at"]).dt.to_period("W-MON").dt.start_time
    )
    semanal = (
        df_sem.dropna(subset=["semana"])
        .groupby("semana")
        .size()
        .reset_index(name="menciones")
    )

    fig_evol = go.Figure()
    fig_evol.add_trace(
        go.Scatter(
            x=semanal["semana"],
            y=semanal["menciones"],
            name="Menciones",
            mode="lines+markers",
        )
    )

    if solo_llm:
        df_odio = df_sem[df_sem["clasificacion_principal"] == "ODIO"]
        sem_odio = (
            df_odio.dropna(subset=["semana"])
            .groupby("semana")
            .size()
            .reset_index(name="odio")
        )
        sem_join = semanal.merge(sem_odio, on="semana", how="left").fillna({"odio": 0})
        sem_join["pct_odio"] = (
            sem_join["odio"] / sem_join["menciones"].replace(0, pd.NA)
        ) * 100.0
        fig_evol.add_trace(
            go.Scatter(
                x=sem_join["semana"],
                y=sem_join["pct_odio"],
                name="% ODIO",
                mode="lines+markers",
                yaxis="y2",
            )
        )
        fig_evol.update_layout(
            yaxis2=dict(title="% ODIO", overlaying="y", side="right", rangemode="tozero"),
        )

    fig_evol.update_layout(
        xaxis_title="Semana",
        yaxis_title="Menciones",
        height=400,
        legend=dict(orientation="h", y=-0.2),
        margin=dict(t=30, b=40),
    )
    st.plotly_chart(fig_evol, use_container_width=True)

    # 6) Comparativa por medio
    st.markdown("### Reacción de audiencia por medio")

    def _intens_media(s):
        nums = s.map(_INTENSIDAD_NUMERICA)
        return round(nums.mean(), 2) if nums.notna().any() else None

    por_medio = (
        df.groupby("source_media")
        .agg(
            total_menciones=("message_uuid", "count"),
            total_odio=(
                "clasificacion_principal",
                lambda s: int((s == "ODIO").sum()),
            ),
            intens_media=("intensidad_pred", _intens_media),
        )
        .reset_index()
        .sort_values("total_menciones", ascending=False)
        .head(20)
    )

    if solo_llm:
        por_medio["pct_odio"] = (
            por_medio["total_odio"] / por_medio["total_menciones"].replace(0, pd.NA)
        ) * 100.0
        fig_medios = px.bar(
            por_medio.sort_values("total_menciones"),
            x="total_menciones",
            y="source_media",
            orientation="h",
            color="pct_odio",
            color_continuous_scale="Reds",
            hover_data=["intens_media"],
            labels={
                "total_menciones": "Menciones",
                "pct_odio": "% ODIO",
                "source_media": "Medio",
                "intens_media": "Intensidad media",
            },
        )
    else:
        fig_medios = px.bar(
            por_medio.sort_values("total_menciones"),
            x="total_menciones",
            y="source_media",
            orientation="h",
            labels={
                "total_menciones": "Menciones",
                "source_media": "Medio",
            },
        )
    fig_medios.update_layout(height=520, margin=dict(l=160, t=20))
    _apply_horizontal_bar_labels(fig_medios)
    st.plotly_chart(fig_medios, use_container_width=True)

    # 7) Distribución de categorías por medio (solo si filtro IA/LLM activo)
    if solo_llm:
        st.markdown(_ui_label("### Categorías de odio por medio"))
        df_odio_cat = df[df["clasificacion_principal"] == "ODIO"].copy()
        if df_odio_cat.empty:
            st.info("No hay mensajes ODIO para este término.")
        else:
            cat_pivot = (
                df_odio_cat.groupby(["source_media", "categoria_odio_pred"])
                .size()
                .reset_index(name="total")
            )
            cat_pivot["categoria_label"] = cat_pivot["categoria_odio_pred"].map(
                lambda x: CATEGORIAS_LABELS.get(x, x or "—")
            )
            top_medios = (
                cat_pivot.groupby("source_media")["total"]
                .sum()
                .sort_values(ascending=False)
                .head(20)
                .index.tolist()
            )
            cat_pivot = cat_pivot[cat_pivot["source_media"].isin(top_medios)]
            fig_cat = px.bar(
                cat_pivot,
                x="source_media",
                y="total",
                color="categoria_label",
                barmode="stack",
                labels={
                    "total": "Mensajes ODIO",
                    "source_media": "Medio",
                    "categoria_label": "Categoría",
                },
            )
            fig_cat.update_layout(height=500, xaxis_tickangle=-30, margin=dict(t=20))
            st.plotly_chart(fig_cat, use_container_width=True)

    # 8) Muestra de mensajes
    st.markdown("### Muestra de mensajes")
    df_sample = (
        df.sort_values("created_at", ascending=False).head(50).reset_index(drop=True)
    )
    page_size = 10
    total_pages = max(1, (len(df_sample) + page_size - 1) // page_size)
    if "buscador_page_idx" not in st.session_state:
        st.session_state["buscador_page_idx"] = 0
    st.session_state["buscador_page_idx"] = max(
        0, min(int(st.session_state["buscador_page_idx"]), total_pages - 1)
    )
    page_idx = int(st.session_state["buscador_page_idx"])

    c_prev, c_info, c_next = st.columns([1, 3, 1])
    with c_prev:
        if st.button(
            "◀ Anterior",
            key="buscador_prev",
            disabled=page_idx <= 0,
        ):
            st.session_state["buscador_page_idx"] = max(0, page_idx - 1)
            st.rerun()
    with c_info:
        st.markdown(
            f"<div style='text-align:center;padding:0.35rem 0;color:#5c6b7a;'>"
            f"Página <b>{page_idx + 1}</b> / {total_pages} · "
            f"mostrando hasta {page_size} mensajes</div>",
            unsafe_allow_html=True,
        )
    with c_next:
        if st.button(
            "Siguiente ▶",
            key="buscador_next",
            disabled=page_idx >= total_pages - 1,
        ):
            st.session_state["buscador_page_idx"] = min(total_pages - 1, page_idx + 1)
            st.rerun()

    start = page_idx * page_size
    end = start + page_size
    for _, row in df_sample.iloc[start:end].iterrows():
        with st.container(border=True):
            plat = platform_label(str(row["platform"] or ""))
            medio_pub = _public_medio_label(row.get("source_media"))
            fecha = (
                pd.to_datetime(row["created_at"]).strftime("%d/%m/%Y %H:%M")
                if pd.notna(row["created_at"])
                else "—"
            )
            if medio_pub:
                st.markdown(
                    f"**{plat}** · Medio monitorizado: **{medio_pub}** · {fecha}"
                )
            else:
                st.markdown(f"**{plat}** · {fecha}")
            if solo_llm:
                clasif = (row.get("clasificacion_principal") or "—")
                raw_cat = row.get("categoria_odio_pred") or ""
                cat_label = CATEGORIAS_LABELS.get(raw_cat, raw_cat or "—")
                intens = (row.get("intensidad_pred") or "—")
                st.markdown(
                    f"**Clasificación:** `{clasif}` · **Categoría:** {cat_label} · "
                    f"**Intensidad:** `{intens}`"
                )
            st.text(row["content_clean"])

    # 9) Exportación CSV (todos los resultados filtrados)
    st.markdown("---")
    export_cols = ["platform", "source_media", "created_at", "content_clean"]
    if solo_llm:
        export_cols += [
            "clasificacion_principal",
            "categoria_odio_pred",
            "intensidad_pred",
        ]
    df_export = df[export_cols].copy()
    if "source_media" in df_export.columns:
        df_export["source_media"] = df_export["source_media"].apply(
            lambda x: _public_medio_label(x) or ""
        )
    df_export = df_export.rename(
        columns={"content_clean": "content_original_anon"}
    )
    safe_tag = re.sub(r"[^a-zA-Z0-9_-]+", "_", termino)[:40] or "busqueda"
    st.download_button(
        label=f"Descargar CSV — resultados de '{termino}'",
        data=df_to_csv_bytes(df_export),
        file_name=f"reto_buscador_{safe_tag}.csv",
        mime="text/csv",
        key="dl_csv_buscador_terminos",
    )


# ============================================================
# FOOTER – Logos institucionales
# ============================================================
_LOGOS_ORDER = [
    ("01_ciedes.png", "CIEDES"),
    ("02_cifal.png", "CIFAL Málaga"),
    ("03_laguajira.png", "La Guajira"),
    ("04_cppa.png", "Colegio Profesional de Periodistas de Andalucía"),
    ("05_coe.png", "Comité Olímpico Español"),
    ("06_mci.png", "Movimiento Contra la Intolerancia"),
]


def _img_to_base64(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode()


def render_footer():
    """Muestra los logos institucionales en la parte inferior de la app."""
    logos_dir = _reto_logos_directory()
    if logos_dir is None:
        return

    items = []
    for filename, alt in _LOGOS_ORDER:
        p = logos_dir / filename
        if p.exists():
            b64 = _img_to_base64(p)
            items.append((b64, alt))

    if not items:
        return

    st.markdown("---")

    st.markdown(
        """
        <div class="reto-footer-copy" style="
            text-align:center;
            color:#4A5568;
            font-size:0.9rem;
            line-height:1.55;
            max-width:46rem;
            margin:0 auto 1.1rem auto;
            padding:0 12px;
        ">
            <p style="margin:0 0 0.45rem 0;">
                <strong style="color:#1F4E79;">ReTo</strong>
                — Red de Tolerancia contra los delitos de odio.
                Proyecto <strong>cofinanciado por la Unión Europea</strong>
                (Programa <strong>CERV</strong> — derechos, igualdad y ciudadanía).
            </p>
            <p style="margin:0;font-size:0.85rem;color:#718096;">
                Consorcio: CIFAL Málaga, Fundación CIEDES, Movimiento Contra la Intolerancia,
                Colegio Profesional de Periodistas de Andalucía, Comité Olímpico Español,
                Asociación La Guajira; con colaboración de Universidad de Almería, Almería Acoge y Yo Soy El Otro.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    imgs_html = ""
    for b64, alt in items:
        imgs_html += (
            f'<img src="data:image/png;base64,{b64}" '
            f'alt="{alt}" title="{alt}" '
            f'style="height:36px; margin:5px 8px; object-fit:contain;">'
        )

    st.markdown(
        f"""
        <div style="
            display:flex;
            flex-wrap:wrap;
            justify-content:center;
            align-items:center;
            padding:6px 8px 18px 8px;
            gap:4px;
        ">
            {imgs_html}
        </div>
        """,
        unsafe_allow_html=True,
    )


# ============================================================
# MAIN
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


def _scroll_main_to_top() -> None:
    """Scroll al inicio del área principal al cambiar de sección (Streamlit suele conservar scroll)."""
    components.html(
        """
        <script>
        (function () {
            function goTop() {
                const doc = window.parent.document;
                const tryScroll = (el) => {
                    if (!el) return;
                    el.scrollTop = 0;
                    if (typeof el.scrollTo === "function") {
                        el.scrollTo({ top: 0, left: 0, behavior: "auto" });
                    }
                };
                doc.querySelectorAll(
                    '[data-testid="stAppViewContainer"], '
                    + 'section[data-testid="stMain"], section.main'
                ).forEach(tryScroll);
                tryScroll(doc.documentElement);
                tryScroll(doc.body);
                window.parent.scrollTo(0, 0);
            }
            goTop();
            setTimeout(goTop, 50);
            setTimeout(goTop, 150);
        })();
        </script>
        """,
        height=0,
        width=0,
    )


def _ensure_db_connection() -> bool:
    """Comprueba PostgreSQL una vez por sesión; evita cuelgues sin connect_timeout."""
    if st.session_state.get("_db_ok") is True:
        return True
    if st.session_state.get("_db_ok") is False:
        return False

    is_admin = st.session_state.get("user_role") == "admin"

    if not postgres_configured():
        st.session_state["_db_ok"] = False
        st.error("No se pudo establecer conexión con la base de datos.")
        if is_admin:
            st.markdown(
                """
**[Admin]** No se detectaron credenciales PostgreSQL.

En **Hugging Face Docker** configurá los secrets como variables de entorno:
- Opción A (recomendada): secret `DATABASE_URL` con la URL completa de Neon.
- Opción B: secrets `POSTGRES_HOST`, `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DBNAME`, `POSTGRES_SSLMODE=require`.
                """
            )
        return False
    try:
        access_raw = _role_can_access_raw()
        probe_table = "raw.mensajes" if access_raw else "processed.mensajes"
        schema_ok = False
        perm_denied = False
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {probe_table}")
                    cur.fetchone()
                    schema_ok = True
                except Exception as probe_exc:
                    err_name = type(probe_exc).__name__
                    if err_name == "InsufficientPrivilege":
                        perm_denied = True
                    elif err_name == "UndefinedTable":
                        schema_ok = False
                    else:
                        raise
        if perm_denied:
            st.session_state["_db_ok"] = False
            st.error("El usuario de base de datos no tiene permisos suficientes para este perfil.")
            if is_admin:
                st.markdown(
                    """
**[Admin]** El usuario de BD configurado no tiene permisos de lectura sobre el esquema requerido.
Revisá los GRANTs en Neon para el usuario de visualización.
Script de referencia: `automatizacion_diaria/migrations/grant_analista_01_viewer.sql`
                    """
                )
            return False
        if not schema_ok:
            st.session_state["_db_ok"] = False
            st.error("La base de datos no está configurada correctamente.")
            if is_admin:
                st.caption(
                    "[Admin] Verificá que DATABASE_URL apunte al proyecto y base de datos correctos."
                )
            return False
        st.session_state["_db_ok"] = True
        return True
    except Exception as exc:
        st.session_state["_db_ok"] = False
        st.error("No se pudo conectar a la base de datos. Intentá recargar la página.")
        if is_admin:
            st.caption(f"[Admin] Detalle técnico: {type(exc).__name__}")
        return False


def main():
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
