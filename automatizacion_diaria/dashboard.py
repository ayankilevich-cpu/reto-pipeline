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
from collections import Counter
from contextlib import contextmanager
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components
from wordcloud import WordCloud
import matplotlib.pyplot as plt

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


def _reto_logos_directory() -> Optional[Path]:
    for base in (_AUTO_DIR, _RETO_ROOT):
        d = base / "logos"
        if d.is_dir():
            return d
    return None


sys.path.insert(0, str(_AUTO_DIR))
from db_utils import get_conn, postgres_configured

# ── Módulos refactorizados (Fase 1) ────────────────
from secciones.comparativa_modelos import render_comparativa
from secciones.panel_general import load_last_pipeline_run_legacy, render_panel_general
from secciones.buscador_analisis import render_buscador_terminos
from secciones.delitos_odio import render_delitos
from secciones.terminos_frecuentes import render_terminos
from secciones.categorias_odio import render_categorias
from secciones.proyecto_reto import render_proyecto
from components.ui import _anonimizar_texto_mensaje
from components.validacion_shared import (
    _render_vllm_label_error_analysis,
    _render_vllm_yt_error_analysis,
)
from components.art510_shared import _render_art510_validacion_humana
from components.constants import APARTADO_LABELS, ART510_COLORS
from components.constants import (
    _expand_platforms,
    LABEL_SOURCE_LABELS,
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
from secciones.ranking_medios import render_ranking_medios
from components.ui import (
    _apply_horizontal_bar_labels,
    _is_viewer,
    _render_section_header,
    _reto_asset_file,
    _require_role,
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


@st.cache_data(ttl=60)
def load_last_pipeline_run(pipeline_name: str = "reto_x_diario") -> dict:
    """
    Compatibilidad temporal: mantiene el nombre histórico de la función.
    """
    return load_last_pipeline_run_legacy(pipeline_name=pipeline_name)


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


TERMINOS_EXCLUSION_JSON = Path(__file__).resolve().parent / "terminos_excluidos_visualizacion.json"


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
# BUSCADOR Y ANÁLISIS — búsqueda por término y análisis agregado
# ============================================================


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
