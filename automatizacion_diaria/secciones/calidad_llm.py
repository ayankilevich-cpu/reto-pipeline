"""Sección «Calidad LLM» del dashboard (editor + admin)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import plotly.express as px
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import get_conn
from components.constants import (
    CATEGORIAS_LABELS,
    COLORS,
    PLATFORM_DISPLAY,
    _expand_platforms,
)
from components.ui import (
    _apply_horizontal_bar_labels,
    _render_section_header,
    _require_role,
)
from components.exports import render_section_exports
from components.validacion_shared import _render_vllm_yt_error_analysis


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
            st.plotly_chart(fig, use_container_width=True, theme=None)
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
            st.plotly_chart(fig, use_container_width=True, theme=None)

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
