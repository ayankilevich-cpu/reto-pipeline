"""Sección "Ranking de medios" del dashboard RETO."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Tuple

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.anonimizacion import enmascarar_texto_ui
from components.constants import EXCLUDED_SOURCE_MEDIA, PLATFORM_DISPLAY
from components.db_helpers import (
    _public_medio_label, _pooled_conn,
    load_youtube_recorrido, fecha_fin_clasificacion_yt,
)
from components.ui import _render_section_header
from components.exports import render_descargas_menu
from components.aviso_criterio import render_aviso_cambio_criterio
from components.chart_style import (
    ACCENT, INK, MUTED, NAVY, NEUTRAL,
    colores_destacados, encabezado_grafico, etiqueta_doble,
    fmt_int, fmt_pct, kpi_tarjetas, nota_fuente,
)

_MIN_MSGS = 100


def _nombre_corto(nombre: str, maxlen: int = 28) -> str:
    idx = nombre.find(" (")
    if idx != -1:
        nombre = nombre[:idx]
    if len(nombre) > maxlen:
        nombre = nombre[:maxlen - 1] + "…"
    return nombre


def _ordinal(n: int) -> str:
    return f"{n}.º"


@st.cache_data(ttl=86400, show_spinner="Cargando datos…", max_entries=64)
def _load_ranking_medios_raw(
    min_msgs: int = 100,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> pd.DataFrame:
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

    with _pooled_conn() as conn:
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
                COUNT(DISTINCT CASE WHEN pm.relevante_llm = 'SI'
                    THEN pm.message_uuid END) AS relevante_filtro,
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


@st.cache_data(ttl=86400, max_entries=64)
def load_ranking_medios(
    platforms: Optional[Tuple] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> pd.DataFrame:
    df = _load_ranking_medios_raw(min_msgs=_MIN_MSGS, fecha_desde=fecha_desde, fecha_hasta=fecha_hasta)
    df = df[df["source_media"].apply(lambda sm: _public_medio_label(sm) is not None)]
    if platforms:
        df = df[df["platform"].isin(list(platforms))]
    return df


def _prepare_ranking_df(df: pd.DataFrame) -> pd.DataFrame:
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
    df["medio"] = df["source_media"].map(_public_medio_label).map(enmascarar_texto_ui)
    return df


def _render_ranking_simple(
    df: pd.DataFrame, top_n: int, key_suffix: str, plataforma_label: str = "la plataforma"
) -> Optional[go.Figure]:
    if df.empty:
        st.info("No hay datos de medios para esta vista.")
        return None

    df_vol = df.sort_values("total_mensajes", ascending=False).head(top_n).copy()
    df_vol["medio_corto"] = df_vol["medio"].map(_nombre_corto)

    _tot = df_vol["total_mensajes"].sum()
    _media = (df_vol["odio_cualquiera"].sum() / _tot * 100) if _tot else 0.0

    df_plot = df_vol.sort_values("pct_odio_any", ascending=False).reset_index(drop=True)
    _ids = [str(i) for i in df_plot.index]
    _top3 = set(_ids[:3])
    _lider = df_plot.iloc[0]
    _xmax = max(float(df_plot["pct_odio_any"].max()), _media) * 1.25 or 1

    fig = go.Figure(go.Bar(
        orientation="h",
        y=_ids,
        x=df_plot["pct_odio_any"],
        marker_color=colores_destacados(_ids, _top3),
        text=[fmt_pct(v) for v in df_plot["pct_odio_any"]],
        textposition="outside",
        cliponaxis=False,
        textfont=dict(size=13, color=[ACCENT if i in _top3 else MUTED for i in _ids]),
        customdata=[[m, fmt_int(t), fmt_int(o), fmt_pct(p)] for m, t, o, p in zip(
            df_plot["medio"], df_plot["total_mensajes"],
            df_plot["odio_cualquiera"], df_plot["pct_odio_any"])],
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>%{customdata[1]} mensajes analizados<br>"
            "%{customdata[2]} clasificados como odio<br><b>%{customdata[3]} de odio</b>"
            "<extra></extra>"
        ),
    ))
    fig.add_shape(
        type="line", x0=_media, x1=_media, xref="x",
        y0=0, y1=1, yref="paper",
        line=dict(color=NAVY, width=1, dash="dot"),
    )
    fig.add_annotation(
        x=_media, xref="x", y=1.0, yref="paper",
        text=f"Media del grupo: {fmt_pct(_media)}", showarrow=False,
        yanchor="bottom", xanchor="left", xshift=4,
        font=dict(size=11, color=MUTED),
    )
    fig.update_layout(
        height=max(380, len(_ids) * 46),
        showlegend=False,
        bargap=0.42,
        margin=dict(l=8, r=64, t=36, b=8),
        xaxis=dict(visible=False, range=[0, _xmax], fixedrange=True),
        yaxis=dict(
            type="category", autorange="reversed", showgrid=False, fixedrange=True, automargin=True,
            tickvals=_ids,
            ticktext=[etiqueta_doble(mc, f"{fmt_int(t)} mensajes")
                      for mc, t in zip(df_plot["medio_corto"], df_plot["total_mensajes"])],
            ticksuffix="   ",
            tickfont=dict(size=13, color="#1A202C"),
        ),
    )

    encabezado_grafico(
        f"{_nombre_corto(_lider['medio'])} registra el mayor porcentaje de odio entre los "
        f"{len(_ids)} medios con más mensajes ({fmt_pct(_lider['pct_odio_any'])})",
        "Porcentaje de mensajes clasificados como odio sobre el total de cada medio. "
        "Debajo de cada nombre, mensajes analizados. Línea punteada: media del grupo.",
    )
    st.plotly_chart(fig, use_container_width=True, theme=None, key=f"rm_pct_{key_suffix}")
    nota_fuente("% de odio: mensajes marcados como odio por al menos una fuente (modelo automático, clasificación IA o validación humana).")
    st.caption(f"Se comparan los {len(_ids)} medios con más mensajes de {plataforma_label} en el período.")
    return fig


def _render_explorar_medio(df_plat: pd.DataFrame, plat_key: str) -> None:
    if df_plat.empty:
        return

    st.markdown("---")
    st.markdown(f"<p style='font-size:17px;font-weight:600;color:{INK};margin:4px 0 12px'>Explorar medio</p>", unsafe_allow_html=True)

    df_e = df_plat.sort_values("total_mensajes", ascending=False).reset_index(drop=True)
    medio_idx = st.selectbox(
        "Medio", df_e.index, format_func=lambda i: df_e.at[i, "medio"],
        key=f"explorar_medio_{plat_key}",
    )
    if medio_idx is None:
        return
    r = df_e.loc[medio_idx]

    total = int(r["total_mensajes"])
    odio = int(r["odio_cualquiera"])
    pct = round(odio / max(total, 1) * 100, 2)

    df_ranked = df_e.sort_values("pct_odio_any", ascending=False)
    pos_num = int(df_ranked.index.get_loc(medio_idx)) + 1
    pos_str = _ordinal(pos_num) + f" de {len(df_e)}"

    kpi_tarjetas([
        ("Mensajes", fmt_int(total), ""),
        ("Mensajes de odio", fmt_int(odio), ""),
        ("% de odio", fmt_pct(pct, dec=2), ""),
        ("Posición en el ranking", pos_str, ""),
    ])

    top15 = df_e.head(15).copy()
    top15 = top15.sort_values("total_mensajes", ascending=True)
    _ids15 = [str(i) for i in top15.index]
    colors15 = [NAVY if i == medio_idx else NEUTRAL for i in top15.index]
    _xmax15 = float(top15["total_mensajes"].max()) * 1.2 or 1

    fig_e = go.Figure(go.Bar(
        orientation="h",
        y=_ids15,
        x=top15["total_mensajes"],
        marker_color=colors15,
        text=[fmt_int(t) for t in top15["total_mensajes"]],
        textposition="outside",
        cliponaxis=False,
        customdata=[[m, fmt_int(t), fmt_pct(p)] for m, t, p in zip(
            top15["medio"], top15["total_mensajes"], top15["pct_odio_any"])],
        hovertemplate=(
            "<b>%{customdata[0]}</b><br>%{customdata[1]} mensajes<br>"
            "%{customdata[2]} de odio<extra></extra>"
        ),
    ))
    fig_e.update_layout(
        height=max(420, len(_ids15) * 42),
        showlegend=False,
        bargap=0.4,
        margin=dict(l=8, r=64, t=16, b=8),
        xaxis=dict(visible=False, range=[0, _xmax15], fixedrange=True),
        yaxis=dict(
            type="category", showgrid=False, fixedrange=True, automargin=True,
            tickvals=_ids15,
            ticktext=[etiqueta_doble(_nombre_corto(m), f"{fmt_int(t)} mensajes · {fmt_pct(p)}")
                      for m, t, p in zip(top15["medio"], top15["total_mensajes"], top15["pct_odio_any"])],
            ticksuffix="   ",
            tickfont=dict(size=13),
        ),
    )
    st.plotly_chart(fig_e, use_container_width=True, theme=None, key=f"explorar_top15_{plat_key}")

    with st.expander("Ver tabla completa"):
        cols_tabla = ["medio", "total_mensajes", "odio_cualquiera", "pct_odio_any"]
        if plat_key == "youtube" and "relevante_filtro" in df_e.columns:
            cols_tabla.append("relevante_filtro")
        df_tabla = df_e[cols_tabla].copy().sort_values("total_mensajes", ascending=False)
        rename_map = {
            "medio": "Medio",
            "total_mensajes": "Mensajes",
            "odio_cualquiera": "Mensajes de odio",
            "pct_odio_any": "% de odio",
            "relevante_filtro": "Señalados por el filtro",
        }
        df_tabla = df_tabla.rename(columns={k: v for k, v in rename_map.items() if k in df_tabla.columns})
        for col in ["Mensajes", "Mensajes de odio"]:
            if col in df_tabla.columns:
                df_tabla[col] = df_tabla[col].apply(fmt_int)
        if "% de odio" in df_tabla.columns:
            df_tabla["% de odio"] = df_tabla["% de odio"].apply(fmt_pct)
        if "Señalados por el filtro" in df_tabla.columns:
            df_tabla["Señalados por el filtro"] = df_tabla["Señalados por el filtro"].apply(fmt_int)
        st.dataframe(df_tabla, use_container_width=True, hide_index=True, key=f"tabla_completa_{plat_key}")


def render_ranking_medios():
    col_titulo, col_dl = st.columns([6, 1])
    with col_titulo:
        _render_section_header(
            "Ranking de medios",
            "Top medios de comunicación por volumen de mensajes y porcentaje de odio.",
        )
    render_aviso_cambio_criterio()

    col_fd, col_fh = st.columns(2)
    with col_fd:
        fecha_desde = st.date_input(
            "Desde", value=None, key="ranking_fecha_desde",
            help="Opcional: deja vacío para ver todo el período.",
            format="DD/MM/YYYY",
        )
    with col_fh:
        fecha_hasta = st.date_input(
            "Hasta", value=None, key="ranking_fecha_hasta",
            help="Opcional: deja vacío para ver todo el período.",
            format="DD/MM/YYYY",
        )

    fd_str = fecha_desde.isoformat() if fecha_desde else None
    fh_str = fecha_hasta.isoformat() if fecha_hasta else None

    df_all = load_ranking_medios(fecha_desde=fd_str, fecha_hasta=fh_str)
    if df_all.empty:
        st.warning("No hay datos de medios.")
        return
    df_all = _prepare_ranking_df(df_all)
    df_x = df_all[df_all["platform"] == "x"].copy()
    df_yt = df_all[df_all["platform"] == "youtube"].copy()

    try:
        plataforma = st.segmented_control(
            "Plataforma", ["X", "YouTube"], default="X", key="ranking_plataforma",
        )
    except AttributeError:
        plataforma = st.radio(
            "Plataforma", ["X", "YouTube"], index=0, horizontal=True, key="ranking_plataforma",
        )
    plataforma = plataforma or "X"

    plat_key = "x" if plataforma == "X" else "youtube"
    df_plat = df_x if plat_key == "x" else df_yt

    if plataforma == "YouTube":
        yt_data = load_youtube_recorrido()
        st.info(
            f"En YouTube la clasificación IA completa terminó el {fecha_fin_clasificacion_yt(yt_data)}; "
            "desde entonces solo se aplica el filtro de relevancia. "
            "Los porcentajes de YouTube no son comparables con los de X."
        )

    fig = _render_ranking_simple(df_plat, top_n=10, key_suffix=plat_key, plataforma_label=plataforma)
    _render_explorar_medio(df_plat, plat_key)

    def _prep_csv(df: pd.DataFrame) -> pd.DataFrame:
        out = df.drop(columns=["source_media", "medio"])
        out.insert(0, "Medio", df["medio"])
        return out

    csv_items = []
    if not df_x.empty:
        csv_items.append(("x", _prep_csv(df_x)))
    if not df_yt.empty:
        csv_items.append(("youtube", _prep_csv(df_yt)))

    with col_dl:
        render_descargas_menu(
            "ranking_medios",
            "Ranking de medios",
            csv_items=csv_items,
            fig_items=[{"title": f"Ranking {plataforma}", "fig": fig, "kind": "plotly"}] if fig else [],
        )
