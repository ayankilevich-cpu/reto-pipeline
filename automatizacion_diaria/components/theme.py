"""Tema visual, CSS global y footer del dashboard RETO."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional
import base64
import plotly.graph_objects as go
import streamlit as st
import streamlit.components.v1 as components

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

try:
    import plotly.io as pio
except Exception:  # pragma: no cover
    pio = None

from components.constants import CAT_COLOR_MAP

_AUTO_DIR = _HERE
_RETO_ROOT = _HERE.parent


def _reto_logos_directory() -> Optional[Path]:
    for base in (_AUTO_DIR, _RETO_ROOT):
        d = base / "logos"
        if d.is_dir():
            return d
    return None


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
    grid-template-columns: repeat(auto-fill, minmax(220px, 1fr));
    justify-content: start;
    gap: 0.85rem;
    margin: 0.35rem 0 1rem 0;
    width: 100%;
}
/* Forzar Inter en KPI cards y headers de sección (Streamlit resolvía
   "Source Sans" por defecto en estos contenedores de markdown) */
.pg-kpi-grid,
.pg-kpi-card,
.pg-kpi-label,
.pg-kpi-value,
.reto-section-header {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif !important;
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

/* --- Selectbox / multiselect: mismo radio 8px que botones/headers --- */
[data-baseweb="select"] > div {
    border-radius: 8px !important;
}
[data-baseweb="select"] > div:first-child {
    border-color: #CBD5E0 !important;
    border-radius: 8px !important;
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
    color: #5A6B82 !important; /* antes #718096: 3.71:1 sobre #F4F6F8, bajo WCAG AA. Ahora ~5:1 */
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


def _register_plotly_theme() -> None:
    """Registra el template Plotly 'reto' con paleta y estilo unificados."""
    if pio is None:
        return
    template = go.layout.Template()
    template.layout = go.Layout(
        font=dict(family="Inter, -apple-system, sans-serif", size=12, color="#4A5568"),
        title=dict(
            font=dict(size=15, color="#1A202C", family="Inter, -apple-system, sans-serif"),
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
            font=dict(size=12, color="#4A5568", family="Inter, -apple-system, sans-serif"),
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
