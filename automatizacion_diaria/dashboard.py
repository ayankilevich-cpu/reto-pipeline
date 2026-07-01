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
from secciones.analisis_510 import render_analisis_art510
from secciones.anotacion_validacion import render_anotacion
from secciones.calidad_llm import render_calidad_llm
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


# ── Prompt y lógica de evaluación LLM Art. 510 ──


# ============================================================
# SECCIÓN: DELITOS DE ODIO (datos oficiales)
# ============================================================


# ============================================================
# ANOTACIÓN YOUTUBE
# ============================================================


# ============================================================
# VALIDACIÓN ETIQUETADO LLM YOUTUBE
# ============================================================


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
