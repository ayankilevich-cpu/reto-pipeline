"""Sección «Comparativa modelos» del dashboard (editor + admin)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import get_conn
from components.constants import CATEGORIAS_LABELS, _expand_platforms, platform_label
from components.ui import (
    _apply_horizontal_bar_labels,
    _render_section_header,
    _require_role,
    _role_can_access_raw,
)
from components.exports import render_section_exports
from components.db_helpers import load_filter_options


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
