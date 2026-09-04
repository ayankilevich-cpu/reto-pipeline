"""Sección «Semáforo diario» del dashboard RETO — alerta temprana (Fase 1).

Muestra el estado del semáforo binario calculado por `automatizacion_diaria/
semaforo_diario.py` (processed.semaforo_diario): por plataforma, si el
volumen de mensajes "calientes" del día se desvía de lo normal, sin esperar
clasificación LLM/humana.

Diseño validado por backtest v1→v2 + verificación manual de 9 eventos reales.
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import COLORS, PLATFORM_DISPLAY
from components.ui import _render_section_header
from components.db_helpers import _pooled_conn

VENTANA_DIAS_DASHBOARD = 60
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


def _chip_html(rojo: Optional[bool]) -> str:
    """Chip reutilizando las clases .reto-chip ya definidas en theme.py."""
    if rojo is True:
        return '<span class="reto-chip danger">🔴 Alerta</span>'
    if rojo is False:
        return '<span class="reto-chip success">🟢 Normal</span>'
    return '<span class="reto-chip warning">⚪ Sin datos suficientes</span>'


def _plataformas_presentes(df: pd.DataFrame) -> List[str]:
    presentes = set(df["platform"].unique())
    return [p for p in _ORDEN_PLATAFORMAS if p in presentes]


def render_semaforo() -> None:
    _render_section_header(
        "Semáforo diario",
        "Alerta temprana (Fase 1) — detecta desviaciones de volumen \"caliente\" "
        "día a día, antes de que el mensaje esté clasificado. Verde = normal, "
        "rojo = la señal suavizada de 3 días alcanzó 1,5× el promedio de "
        "referencia de los 21 días anteriores (mismo criterio que <code>es_spike</code>, "
        "pero calculado diariamente en vez de al cierre de la semana).",
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

    st.subheader("Estado actual")
    cols = st.columns(len(plataformas)) if plataformas else []
    for col, plat in zip(cols, plataformas):
        df_p = df[df["platform"] == plat].sort_values("fecha")
        ultima = df_p.iloc[-1]
        with col:
            st.markdown(f"**{PLATFORM_DISPLAY.get(plat, plat)}** — {ultima['fecha'].strftime('%d/%m/%Y')}")
            st.markdown(_chip_html(ultima["semaforo_rojo"]), unsafe_allow_html=True)
            if pd.notna(ultima["media_referencia_21d"]):
                st.caption(
                    f"Volumen: {int(ultima['volumen_total'])} · "
                    f"Señal 3d: {ultima['senal_suavizada_3d']:.2f} · "
                    f"Referencia 21d: {ultima['media_referencia_21d']:.2f}"
                )
            else:
                st.caption(
                    f"Volumen: {int(ultima['volumen_total'])} · referencia todavía en formación "
                    "(hacen falta al menos 7 días de histórico)"
                )

    if "youtube" in plataformas:
        st.caption(
            "⚠️ YouTube corre con volumen crudo como proxy provisional — sin `score_baseline` "
            "en producción todavía (PR #6 sin mergear). Sin backtest propio: tratar sus "
            "alertas con más cautela que las de X."
        )

    st.subheader("Señal suavizada (3 días) vs. referencia (21 días)")
    if plataformas:
        tabs = st.tabs([PLATFORM_DISPLAY.get(p, p) for p in plataformas])
        for tab, plat in zip(tabs, plataformas):
            with tab:
                df_p = df[df["platform"] == plat].sort_values("fecha")
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df_p["fecha"], y=df_p["senal_suavizada_3d"],
                    mode="lines", name="Señal suavizada (3d)",
                    line=dict(color=COLORS["primary"], width=2),
                ))
                fig.add_trace(go.Scatter(
                    x=df_p["fecha"], y=df_p["media_referencia_21d"],
                    mode="lines", name="Referencia (21d)",
                    line=dict(color=COLORS["muted"], width=1.5, dash="dot"),
                ))
                df_rojo = df_p[df_p["semaforo_rojo"] == True]  # noqa: E712
                if not df_rojo.empty:
                    fig.add_trace(go.Scatter(
                        x=df_rojo["fecha"], y=df_rojo["senal_suavizada_3d"],
                        mode="markers", name="Semáforo rojo",
                        marker=dict(color=COLORS["danger"], size=8),
                    ))
                fig.update_layout(
                    height=380,
                    xaxis_title="", yaxis_title="Señal",
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                    margin=dict(b=60),
                    xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
                )
                st.plotly_chart(fig, use_container_width=True, theme=None, key=f"semaforo_chart_{plat}")

    with st.expander("ℹ️ Cómo leer este panel"):
        st.markdown(
            """
- 🟢 **Verde**: el volumen de mensajes "calientes" del día está dentro de lo normal.
- 🔴 **Rojo**: la señal suavizada de 3 días alcanzó o superó 1,5× el promedio de referencia de los 21 días anteriores — mismo criterio que `es_spike` en el análisis semanal, pero calculado día a día en vez de al cierre de la semana.
- ⚪ **Sin datos suficientes**: todavía no hay al menos 7 días de referencia, o el volumen del día es demasiado bajo para ser confiable.
- En **X**, la señal es el % de mensajes con `score_baseline` en prioridad alta (proxy validado por backtest, AUC test 0,716). En **YouTube** todavía es volumen crudo — proxy provisional, sin backtest propio.
- Este semáforo **no reemplaza** la clasificación de odio: es una alerta temprana de volumen, para saber más rápido cuándo hay que mirar más de cerca.
            """
        )

    st.subheader("Histórico reciente")
    _tbl = df.sort_values(["platform", "fecha"], ascending=[True, False]).copy()
    _tbl["Plataforma"] = _tbl["platform"].map(lambda p: PLATFORM_DISPLAY.get(p, p))
    _tbl["Fecha"] = _tbl["fecha"].dt.strftime("%d/%m/%Y")
    _tbl["Estado"] = _tbl["semaforo_rojo"].map(
        lambda v: "🔴 Rojo" if v is True else ("🟢 Verde" if v is False else "⚪ Sin datos")
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
