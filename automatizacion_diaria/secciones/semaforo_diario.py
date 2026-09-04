"""Sección «Semáforo diario» del dashboard RETO — alerta temprana (Fase 1).

Rediseño (2026-09): chip de texto reemplazado por un semáforo real
(3 luces) y el gráfico de línea por una franja de los últimos 30 días,
en lenguaje llano. El detalle técnico (señal suavizada, referencia,
AUC, etc.) queda en un expander aparte, no en la vista principal.

Diseño validado por backtest v1→v2 + verificación manual de 9 eventos reales
— ver analitica/plan-alerta-temprana-anticipacion.md en el proyecto de
Cowork para el detalle completo.
"""
from __future__ import annotations

import html
import sys
from datetime import date
from pathlib import Path
from typing import List, Optional

import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import PLATFORM_DISPLAY
from components.ui import _render_section_header
from components.db_helpers import _pooled_conn

VENTANA_DIAS_DASHBOARD = 60   # histórico que trae la query (más que los 30 que se muestran)
DIAS_FRANJA = 30              # ventana de la franja de días
_ORDEN_PLATAFORMAS = ("x", "youtube")


@st.cache_data(ttl=300)
def load_semaforo_diario(dias: int = VENTANA_DIAS_DASHBOARD) -> pd.DataFrame:
    """Últimos `dias` de processed.semaforo_diario, ambas plataformas."""
    query = """
        SELECT platform, fecha, semaforo_rojo, volumen_total,
               senal_valor, senal_suavizada_3d, media_referencia_21d
        FROM processed.semaforo_diario
        WHERE fecha >= CURRENT_DATE - (%s::text || ' days')::interval
        ORDER BY platform, fecha
    """
    with _pooled_conn() as conn:
        df = pd.read_sql(query, conn, params=(dias,))
    if not df.empty:
        df["fecha"] = pd.to_datetime(df["fecha"])
    return df


def _plataformas_presentes(df: pd.DataFrame) -> List[str]:
    presentes = set(df["platform"].unique())
    return [p for p in _ORDEN_PLATAFORMAS if p in presentes]


# ------------------------------------------------------------------ #
# Semáforo (3 luces) — estado actual por plataforma
# ------------------------------------------------------------------ #

def _norm_semaforo(rojo) -> Optional[bool]:
    """Normaliza `semaforo_rojo` a True/False/None nativos de Python.

    numpy.bool_(False) is False da False en Python: nunca comparar por
    identidad valores que vienen de pandas. Siempre pd.isna() + bool().
    """
    if pd.isna(rojo):
        return None
    return bool(rojo)


def _semaphore_html(rojo) -> str:
    rojo = _norm_semaforo(rojo)
    if rojo is True:
        cls = ("reto-light red lit", "reto-light amber", "reto-light green")
    elif rojo is False:
        cls = ("reto-light red", "reto-light amber", "reto-light green lit")
    else:
        cls = ("reto-light red", "reto-light amber lit", "reto-light green")
    lights = "".join(f'<div class="{c}"></div>' for c in cls)
    return f'<div class="reto-semaphore">{lights}</div>'


def _status_text(rojo) -> tuple[str, str]:
    rojo = _norm_semaforo(rojo)
    if rojo is True:
        return "Actividad fuera de lo común", "alert"
    if rojo is False:
        return "Todo normal", "ok"
    return "Todavía no hay datos suficientes", "unknown"


def _plat_card_html(plat_label: str, rojo: Optional[bool], fecha, volumen) -> str:
    status_text, status_cls = _status_text(rojo)
    semaphore = _semaphore_html(rojo)
    fecha_s = fecha.strftime("%d/%m/%Y") if pd.notna(fecha) else "—"
    vol_s = f"{int(volumen)} mensajes ese día" if pd.notna(volumen) else "sin volumen registrado"
    return (
        '<div class="reto-plat-card">'
        f"{semaphore}"
        "<div>"
        f'<p class="reto-plat-name">{html.escape(plat_label)}</p>'
        f'<p class="reto-plat-status {status_cls}">{html.escape(status_text)}</p>'
        f'<p class="reto-plat-caption">Última actualización: {fecha_s} · {vol_s}</p>'
        "</div>"
        "</div>"
    )


# ------------------------------------------------------------------ #
# Franja de los últimos 30 días
# ------------------------------------------------------------------ #

def _day_strip_html(df_p: pd.DataFrame, dias: int = DIAS_FRANJA) -> str:
    if df_p.empty:
        return ""
    fin = df_p["fecha"].max()
    inicio = fin - pd.Timedelta(days=dias - 1)
    idx = pd.date_range(inicio, fin, freq="D")
    serie = df_p.set_index("fecha")["semaforo_rojo"].reindex(idx)
    hoy = date.today()

    cells = []
    for d, rojo in serie.items():
        rojo = _norm_semaforo(rojo)          # ← nuevo
        if rojo is True:
            cls, label = "alert", "Alerta"
        elif rojo is False:
            cls, label = "ok", "Normal"
        else:
            cls, label = "unknown", "Sin datos suficientes"
        today_cls = " today" if d.date() == hoy else ""
        cells.append(
            f'<div class="reto-day {cls}{today_cls}" '
            f'title="{d.strftime("%d/%m")} — {label}"></div>'
        )
    return '<div class="reto-day-strip">' + "".join(cells) + "</div>"


_LEGEND_HTML = """
<div class="reto-strip-legend">
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot ok"></span>Normal</div>
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot alert"></span>Alerta</div>
  <div class="reto-strip-legend-item"><span class="reto-strip-legend-dot unknown"></span>Sin datos suficientes</div>
</div>
"""


# ------------------------------------------------------------------ #
# Render
# ------------------------------------------------------------------ #

def render_semaforo() -> None:
    _render_section_header(
        "Semáforo diario",
        "Detecta picos inusuales de actividad el mismo día, antes de "
        "clasificarlos.",
    )

    df = load_semaforo_diario()
    if df.empty:
        st.info(
            "Todavía no hay datos en `processed.semaforo_diario`. Este panel se "
            "llena automáticamente con la corrida diaria del pipeline — no hace "
            "falta ninguna acción manual."
        )
        return

    plataformas = _plataformas_presentes(df)

    # --- Estado actual: el semáforo, arriba de todo ---
    cols = st.columns(len(plataformas)) if plataformas else []
    for col, plat in zip(cols, plataformas):
        df_p = df[df["platform"] == plat].sort_values("fecha")
        ultima = df_p.iloc[-1]
        with col:
            st.markdown(
                _plat_card_html(
                    PLATFORM_DISPLAY.get(plat, plat),
                    ultima["semaforo_rojo"],
                    ultima["fecha"],
                    ultima["volumen_total"],
                ),
                unsafe_allow_html=True,
            )

    if "youtube" in plataformas:
        st.caption(
            "En YouTube esta alerta todavía es una versión preliminar — tomá sus "
            "resultados con más cautela que los de X."
        )

    # --- Últimos 30 días, de un vistazo ---
    st.subheader("Últimos 30 días, de un vistazo")
    for plat in plataformas:
        df_p = df[df["platform"] == plat].sort_values("fecha")
        n_alertas = int((df_p["semaforo_rojo"] == True).sum())  # noqa: E712
        etiqueta = "día en alerta" if n_alertas == 1 else "días en alerta"
        st.markdown(
            f'<div class="reto-strip-label">{html.escape(PLATFORM_DISPLAY.get(plat, plat))} '
            f'<span class="reto-strip-label-n">· {n_alertas} {etiqueta} en el último mes</span></div>',
            unsafe_allow_html=True,
        )
        st.markdown(_day_strip_html(df_p), unsafe_allow_html=True)

    st.markdown(_LEGEND_HTML, unsafe_allow_html=True)

    # --- Detalle técnico, aparte ---
    with st.expander("Ver el detalle técnico"):
        st.markdown(
            "El semáforo se enciende en rojo cuando la actividad de los últimos "
            "3 días supera en un 50% o más el promedio habitual de las tres "
            "semanas anteriores. Es el mismo criterio que usamos para detectar "
            "picos semanales, pero calculado día a día en vez de esperar al "
            "cierre de la semana.\n\n"
            "En X, la alerta se basa en qué proporción de los mensajes el "
            "sistema marca como prioridad alta — un método ya probado contra "
            "datos reales. En YouTube, por ahora, se basa en el volumen total "
            "de mensajes sin ese filtro de prioridad, así que sus alertas son "
            "más preliminares y conviene tomarlas con más cautela."
        )
        _tbl = df.sort_values(["platform", "fecha"], ascending=[True, False]).copy()
        _tbl["Plataforma"] = _tbl["platform"].map(lambda p: PLATFORM_DISPLAY.get(p, p))
        _tbl["Fecha"] = _tbl["fecha"].dt.strftime("%d/%m/%Y")
        _tbl["Estado"] = _tbl["semaforo_rojo"].map(
            lambda v: "Alerta" if v is True else ("Normal" if v is False else "Sin datos")
        )
        st.dataframe(
            _tbl[[
                "Plataforma", "Fecha", "Estado", "volumen_total",
                "senal_suavizada_3d", "media_referencia_21d",
            ]].rename(columns={
                "volumen_total": "Volumen",
                "senal_suavizada_3d": "Señal 3d",
                "media_referencia_21d": "Referencia 21d",
            }),
            use_container_width=True,
            hide_index=True,
        )
