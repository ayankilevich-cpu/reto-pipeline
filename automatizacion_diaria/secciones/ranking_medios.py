"""Sección "Ranking de medios" del dashboard RETO."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import pandas as pd
import plotly.express as px
import streamlit as st

from db_utils import get_conn
from components.constants import COLORS, EXCLUDED_SOURCE_MEDIA, PLATFORM_DISPLAY
from components.db_helpers import _load_valid_media_map, _public_medio_label
from components.ui import (
    _render_section_header,
    _is_viewer,
    _apply_horizontal_bar_labels,
)
from components.exports import render_section_exports


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
        st.plotly_chart(
            fig,
            use_container_width=True,
            key="explore_plat_chart",
            config={
                "displayModeBar": "hover",
                "displaylogo": False,
                "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
            },
        )


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
