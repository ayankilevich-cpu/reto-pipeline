"""Sección "Dataset Gold" del dashboard RETO."""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from db_utils import get_conn
from components.constants import (
    CATEGORIAS_LABELS,
    CAT_COLOR_MAP,
    COLORS,
    DELITOS_COLORS,
    PLATFORM_COLORS,
    SEMANTIC_COLORS,
)
from components.ui import _render_section_header, _apply_horizontal_bar_labels
from components.exports import render_section_exports
from components.db_helpers import load_gold_full


def render_gold_dataset():
    """Sección de análisis del dataset gold (LLM + validación humana)."""
    _render_section_header(
        "Dataset Gold",
        "Evaluación del etiquetado: muestras validadas manualmente frente al LLM y métricas de concordancia.",
    )
    df = load_gold_full()

    if df.empty:
        st.warning("No hay datos en el gold dataset.")
        return

    total_samples = len(df)
    plat_counts = df["platform_label"].value_counts().to_dict()
    plat_summary = ", ".join(f"{v:,} {k}" for k, v in plat_counts.items())
    st.caption(f"{total_samples:,} mensajes validados manualmente por anotadores humanos ({plat_summary})")

    # ── Filtros ──
    st.markdown("### Filtros")
    role = st.session_state.get("user_role", "admin")
    platforms = sorted(df["platform_label"].dropna().unique())
    splits = sorted(df["split"].dropna().unique())
    annotators = sorted(df["annotator_id"].dropna().unique())
    labels = sorted(df["y_odio_final"].dropna().unique())

    if role == "admin":
        col_f0, col_f1, col_f2, col_f3 = st.columns(4)
        with col_f0:
            sel_platforms = st.multiselect("Plataforma", platforms, default=platforms, key="gold_plat", placeholder="Todas las plataformas")
        with col_f1:
            sel_splits = st.multiselect("Split", splits, default=splits, key="gold_split", placeholder="Seleccionar…")
        with col_f2:
            sel_annotators = st.multiselect("Anotador", annotators, default=annotators, key="gold_annot", placeholder="Seleccionar…")
        with col_f3:
            sel_labels = st.multiselect("Label final", labels, default=labels, key="gold_label", placeholder="Seleccionar…")
    elif role == "editor":
        col_f0, col_f1, col_f2 = st.columns(3)
        with col_f0:
            sel_platforms = st.multiselect("Plataforma", platforms, default=platforms, key="gold_plat", placeholder="Todas las plataformas")
        with col_f1:
            sel_annotators = st.multiselect("Anotador", annotators, default=annotators, key="gold_annot", placeholder="Seleccionar…")
        with col_f2:
            sel_labels = st.multiselect("Label final", labels, default=labels, key="gold_label", placeholder="Seleccionar…")
        sel_splits = splits
    else:
        col_f0, col_f1 = st.columns(2)
        with col_f0:
            sel_platforms = st.multiselect("Plataforma", platforms, default=platforms, key="gold_plat", placeholder="Todas las plataformas")
        with col_f1:
            sel_labels = st.multiselect("Label final", labels, default=labels, key="gold_label", placeholder="Seleccionar…")
        sel_splits = splits
        sel_annotators = annotators

    if not sel_splits or not sel_annotators or not sel_labels or not sel_platforms:
        st.warning("Selecciona al menos un valor en cada filtro.")
        return

    df_f = df[
        df["platform_label"].isin(sel_platforms)
        & df["split"].isin(sel_splits)
        & df["annotator_id"].isin(sel_annotators)
        & df["y_odio_final"].isin(sel_labels)
    ]

    # ── 1. KPIs ──
    st.markdown("---")
    st.markdown("### Indicadores clave")

    total = len(df_f)
    n_odio = (df_f["y_odio_bin"] == 1).sum()
    n_no_odio = (df_f["y_odio_final"] == "No Odio").sum()
    n_dudoso = (df_f["y_odio_final"] == "Dudoso").sum()
    concordancia = df_f["coincide_con_llm"].mean() * 100 if df_f["coincide_con_llm"].notna().any() else 0
    pct_corr_odio = pd.to_numeric(df_f["corrigio_odio"], errors="coerce").mean() * 100
    pct_corr_cat = pd.to_numeric(df_f["corrigio_categoria"], errors="coerce").mean() * 100

    odio_display = f"{n_odio:,} ({n_odio/total*100:.0f}%)" if total else "0"
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
    <div class="label">Total muestras</div>
    <div class="value">{total:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Odio</div>
    <div class="value">{odio_display}</div>
  </div>
  <div class="metric-card">
    <div class="label">Concordancia LLM</div>
    <div class="value">{concordancia:.1f}%</div>
  </div>
  <div class="metric-card">
    <div class="label">Corrección odio</div>
    <div class="value">{pct_corr_odio:.1f}%</div>
  </div>
  <div class="metric-card">
    <div class="label">Corrección categoría</div>
    <div class="value">{pct_corr_cat:.1f}%</div>
  </div>
</div>
""", unsafe_allow_html=True)

    # ── 1b. Comparativa por plataforma ──
    if len(sel_platforms) > 1:
        plat_summary_df = (
            df_f.groupby("platform_label")
            .agg(
                total=("message_uuid", "count"),
                odio=("y_odio_bin", "sum"),
                corr_odio=("corrigio_odio", "mean"),
            )
            .reset_index()
        )
        plat_summary_df["% Odio"] = (pd.to_numeric(plat_summary_df["odio"], errors="coerce").fillna(0) / plat_summary_df["total"] * 100).round(1)
        plat_summary_df["% Corrección"] = (pd.to_numeric(plat_summary_df["corr_odio"], errors="coerce").fillna(0) * 100).round(1)

        col_p1, col_p2 = st.columns(2)
        with col_p1:
            fig_plat = px.bar(
                plat_summary_df, x="platform_label", y="total",
                color="platform_label",
                color_discrete_map={
                    "X": PLATFORM_COLORS["X"],
                    "YouTube": PLATFORM_COLORS["YouTube"],
                },
                title="Muestras por plataforma",
                text="total",
            )
            fig_plat.update_layout(height=300, showlegend=False, xaxis_title="")
            st.plotly_chart(fig_plat, use_container_width=True)

        with col_p2:
            fig_plat_odio = px.bar(
                plat_summary_df, x="platform_label", y="% Odio",
                color="platform_label",
                color_discrete_map={
                    "X": PLATFORM_COLORS["X"],
                    "YouTube": PLATFORM_COLORS["YouTube"],
                },
                title="% Odio por plataforma",
                text="% Odio",
            )
            fig_plat_odio.update_layout(height=300, showlegend=False, xaxis_title="")
            st.plotly_chart(fig_plat_odio, use_container_width=True)

    # ── 2. Distribución del label final ──
    st.markdown("---")
    st.markdown("### Distribución del label final")

    col_pie1, col_pie2 = st.columns(2)

    with col_pie1:
        odio_counts = df_f["y_odio_final"].value_counts().reset_index()
        odio_counts.columns = ["Label", "Cantidad"]
        fig_odio = px.pie(
            odio_counts, names="Label", values="Cantidad",
            color="Label",
            color_discrete_map=SEMANTIC_COLORS,
            title="Distribución de Odio / No Odio / Dudusos sobre LLM",
        )
        fig_odio.update_layout(height=350)
        st.plotly_chart(fig_odio, use_container_width=True)

    with col_pie2:
        cat_counts = df_f["y_categoria_final"].dropna().value_counts().reset_index()
        cat_counts.columns = ["Categoría", "Cantidad"]
        cat_counts["Categoría"] = cat_counts["Categoría"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x)
        )
        cat_counts = cat_counts.sort_values("Cantidad", ascending=True)
        fig_cat = px.bar(
            cat_counts, x="Cantidad", y="Categoría", orientation="h",
            color="Categoría", color_discrete_map=CAT_COLOR_MAP,
            labels={"Cantidad": "Mensajes", "Categoría": ""},
            title="Categorías de odio (label final)",
            text_auto=True,
        )
        fig_cat.update_layout(
            showlegend=False, height=400, yaxis=dict(autorange="reversed"),
        )
        _apply_horizontal_bar_labels(fig_cat)
        st.plotly_chart(fig_cat, use_container_width=True)

    # ── 3. Distribución de intensidad ──
    st.markdown("---")
    st.markdown("### Distribución de intensidad (solo casos de odio)")

    df_odio = df_f[df_f["y_odio_bin"] == 1].copy()

    if not df_odio.empty:
        col_int1, col_int2 = st.columns(2)

        with col_int1:
            int_counts = df_odio["y_intensidad_final"].dropna().value_counts().sort_index().reset_index()
            int_counts.columns = ["Intensidad", "Cantidad"]
            int_counts["Intensidad"] = int_counts["Intensidad"].astype(int).map(
                {1: "1 — Leve", 2: "2 — Ofensivo", 3: "3 — Hostil"}
            )
            fig_int = px.bar(
                int_counts, x="Intensidad", y="Cantidad",
                color="Intensidad",
                color_discrete_map={
                    "1 — Leve": "#F39C12",
                    "2 — Ofensivo": "#E67E22",
                    "3 — Hostil": "#E74C3C",
                },
                title="Intensidad del odio",
            )
            fig_int.update_layout(height=350, showlegend=False)
            st.plotly_chart(fig_int, use_container_width=True)

        with col_int2:
            # Intensidad por categoría
            int_cat = (
                df_odio.dropna(subset=["y_categoria_final", "y_intensidad_final"])
                .groupby(["y_categoria_final", "y_intensidad_final"])
                .size()
                .reset_index(name="Cantidad")
            )
            int_cat["Categoría"] = int_cat["y_categoria_final"].map(
                lambda x: CATEGORIAS_LABELS.get(x, x)
            )
            int_cat["Intensidad"] = int_cat["y_intensidad_final"].astype(int).map(
                {1: "1 — Leve", 2: "2 — Ofensivo", 3: "3 — Hostil"}
            )
            fig_int_cat = px.bar(
                int_cat, x="Categoría", y="Cantidad", color="Intensidad",
                barmode="stack",
                color_discrete_map={
                    "1 — Leve": "#F39C12",
                    "2 — Ofensivo": "#E67E22",
                    "3 — Hostil": "#E74C3C",
                },
                title="Intensidad por categoría",
            )
            fig_int_cat.update_layout(height=350, xaxis_tickangle=-30)
            st.plotly_chart(fig_int_cat, use_container_width=True)

        df_cat_int = df_odio.dropna(
            subset=["y_categoria_final", "y_intensidad_final"]
        ).copy()
        if not df_cat_int.empty:
            df_cat_int["y_intensidad_final"] = df_cat_int["y_intensidad_final"].astype(float)
            df_cat_int["categoria_label"] = df_cat_int["y_categoria_final"].map(
                CATEGORIAS_LABELS
            ).fillna(df_cat_int["y_categoria_final"])
            avg_int_gold = (
                df_cat_int.groupby("categoria_label")["y_intensidad_final"]
                .mean().round(2).sort_values(ascending=False)
                .reset_index()
            )
            avg_int_gold.columns = ["Categoría", "Intensidad promedio"]
            fig_avg_gold = px.bar(
                avg_int_gold, x="Intensidad promedio", y="Categoría", orientation="h",
                color="Intensidad promedio",
                color_continuous_scale=[[0, "#FBD38D"], [0.5, "#F59E0B"], [1, "#C0392B"]],
                title="Intensidad promedio por categoría de odio",
                text_auto=".2f",
            )
            fig_avg_gold.update_layout(
                height=380, yaxis=dict(autorange="reversed"),
                coloraxis_colorbar=dict(title="Intensidad"),
            )
            _apply_horizontal_bar_labels(fig_avg_gold)
            st.plotly_chart(fig_avg_gold, use_container_width=True)
    else:
        st.info("No hay casos de odio en la selección actual.")

    # ── 4. Concordancia LLM vs Humano ──
    st.markdown("---")
    st.markdown("### Concordancia LLM vs Humano")

    col_c1, col_c2 = st.columns(2)

    with col_c1:
        # Tasa de corrección por tipo
        correction_data = pd.DataFrame({
            "Aspecto": ["Clasificación (odio/no)", "Categoría", "Intensidad"],
            "% Corregido": [
                pd.to_numeric(df_f["corrigio_odio"], errors="coerce").mean() * 100,
                pd.to_numeric(df_f["corrigio_categoria"], errors="coerce").mean() * 100,
                pd.to_numeric(df_f["corrigio_intensidad"], errors="coerce").mean() * 100,
            ],
        })
        correction_data["% Coincide"] = 100 - correction_data["% Corregido"]

        fig_corr = go.Figure()
        fig_corr.add_trace(go.Bar(
            x=correction_data["Aspecto"], y=correction_data["% Coincide"],
            name="Coincide", marker_color=COLORS["success"],
        ))
        fig_corr.add_trace(go.Bar(
            x=correction_data["Aspecto"], y=correction_data["% Corregido"],
            name="Corregido", marker_color=COLORS["danger"],
        ))
        fig_corr.update_layout(
            barmode="stack", title="Tasa de corrección humana",
            yaxis_title="%", height=380,
            legend=dict(orientation="h", yanchor="bottom", y=-0.25),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    with col_c2:
        # Matriz de confusión: LLM vs Humano (clasificación principal)
        df_conf = df_f.dropna(subset=["llm_clasif", "y_odio_final"]).copy()
        if not df_conf.empty:
            # Normalizar LLM labels para comparar
            llm_map = {"ODIO": "Odio", "NO_ODIO": "No Odio", "DUDOSO": "Dudoso"}
            df_conf["llm_label"] = df_conf["llm_clasif"].map(llm_map).fillna(df_conf["llm_clasif"])

            labels_order = ["Odio", "No Odio", "Dudoso"]
            ct = pd.crosstab(
                df_conf["llm_label"], df_conf["y_odio_final"],
                rownames=["LLM"], colnames=["Humano"],
            ).reindex(index=labels_order, columns=labels_order, fill_value=0)

            fig_cm = go.Figure(data=go.Heatmap(
                z=ct.values,
                x=ct.columns.tolist(),
                y=ct.index.tolist(),
                text=ct.values,
                texttemplate="%{text}",
                colorscale="RdYlGn_r",
                showscale=True,
            ))
            fig_cm.update_layout(
                title="Matriz de confusión (LLM vs Humano)",
                xaxis_title="Humano (gold)",
                yaxis_title="LLM (predicción)",
                height=380,
            )
            st.plotly_chart(fig_cm, use_container_width=True)
        else:
            st.info("No hay datos para la matriz de confusión.")

    # ── 5. Correcciones por categoría ──
    st.markdown("---")
    st.markdown("### Correcciones por categoría de odio")

    df_odio_corr = df_f[df_f["y_odio_bin"] == 1].dropna(subset=["y_categoria_final"]).copy()
    if not df_odio_corr.empty:
        corr_by_cat = (
            df_odio_corr.groupby("y_categoria_final")
            .agg(
                total=("message_uuid", "count"),
                corr_odio=("corrigio_odio", "sum"),
                corr_cat=("corrigio_categoria", "sum"),
                corr_int=("corrigio_intensidad", "sum"),
            )
            .reset_index()
        )
        corr_by_cat["Categoría"] = corr_by_cat["y_categoria_final"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x)
        )
        corr_by_cat["% Corr. odio"] = (pd.to_numeric(corr_by_cat["corr_odio"], errors="coerce").fillna(0) / corr_by_cat["total"] * 100).round(1)
        corr_by_cat["% Corr. categoría"] = (pd.to_numeric(corr_by_cat["corr_cat"], errors="coerce").fillna(0) / corr_by_cat["total"] * 100).round(1)
        corr_by_cat["% Corr. intensidad"] = (pd.to_numeric(corr_by_cat["corr_int"], errors="coerce").fillna(0) / corr_by_cat["total"] * 100).round(1)

        corr_melted = corr_by_cat.melt(
            id_vars=["Categoría"],
            value_vars=["% Corr. odio", "% Corr. categoría", "% Corr. intensidad"],
            var_name="Tipo de corrección",
            value_name="%",
        )
        fig_corr_cat = px.bar(
            corr_melted, x="Categoría", y="%", color="Tipo de corrección",
            barmode="group",
            color_discrete_sequence=[COLORS["danger"], COLORS["warning"], COLORS["accent"]],
            title="% de correcciones humanas por categoría",
        )
        fig_corr_cat.update_layout(height=420, xaxis_tickangle=-25)
        st.plotly_chart(fig_corr_cat, use_container_width=True)

    # ── 6. Análisis por anotador ──
    st.markdown("---")
    st.markdown("### Análisis por anotador")

    col_a1, col_a2 = st.columns(2)

    with col_a1:
        annot_counts = df_f["annotator_id"].value_counts().reset_index()
        annot_counts.columns = ["Anotador", "Mensajes"]
        fig_annot = px.bar(
            annot_counts, x="Anotador", y="Mensajes",
            color="Anotador",
            color_discrete_sequence=DELITOS_COLORS,
            title="Mensajes por anotador",
        )
        fig_annot.update_layout(height=350, showlegend=False)
        st.plotly_chart(fig_annot, use_container_width=True)

    with col_a2:
        # Tasa de corrección por anotador
        corr_annot = (
            df_f.groupby("annotator_id")
            .agg(
                total=("message_uuid", "count"),
                corr_odio=("corrigio_odio", "mean"),
            )
            .reset_index()
        )
        corr_annot["% Corrigió odio"] = (pd.to_numeric(corr_annot["corr_odio"], errors="coerce").fillna(0) * 100).round(1)

        fig_corr_annot = px.bar(
            corr_annot, x="annotator_id", y="% Corrigió odio",
            color="annotator_id",
            color_discrete_sequence=DELITOS_COLORS,
            title="% de veces que corrigió al LLM (clasif. odio)",
        )
        fig_corr_annot.update_layout(height=350, showlegend=False, xaxis_title="Anotador")
        st.plotly_chart(fig_corr_annot, use_container_width=True)

    # ── 7. Label source & Split ──
    st.markdown("---")
    st.markdown("### Origen del label y split")

    col_s1, col_s2 = st.columns(2)

    with col_s1:
        source_counts = df_f["label_source"].value_counts().reset_index()
        source_counts.columns = ["Origen", "Cantidad"]
        source_counts["Origen"] = source_counts["Origen"].map({
            "llm_validated": "LLM validado por humano",
            "human_explicit": "Etiquetado humano explícito",
        }).fillna(source_counts["Origen"])
        fig_source = px.pie(
            source_counts, names="Origen", values="Cantidad",
            color_discrete_sequence=[COLORS["accent"], COLORS["warning"]],
            title="Origen del label final",
        )
        fig_source.update_layout(height=350)
        st.plotly_chart(fig_source, use_container_width=True)

    with col_s2:
        split_counts = df_f["split"].value_counts().reset_index()
        split_counts.columns = ["Split", "Cantidad"]
        fig_split = px.pie(
            split_counts, names="Split", values="Cantidad",
            color_discrete_map={"TRAIN": COLORS["primary"], "TEST": COLORS["success"]},
            title="Distribución Train / Test",
        )
        fig_split.update_layout(height=350)
        st.plotly_chart(fig_split, use_container_width=True)

    # ── 8. Tabla detalle ──
    st.markdown("---")
    with st.expander("Tabla de datos completa"):
        display_cols = [
            "platform_label", "message_uuid", "y_odio_final", "y_categoria_final",
            "y_intensidad_final",
            "llm_clasif", "llm_categoria", "llm_intensidad",
            "corrigio_odio", "corrigio_categoria", "corrigio_intensidad",
            "annotator_id", "label_source", "split",
        ]
        st.dataframe(
            df_f[display_cols].rename(columns={
                "platform_label": "Plataforma",
                "y_odio_final": "Label final",
                "y_categoria_final": "Categoría final",
                "y_intensidad_final": "Intensidad final",
                "llm_clasif": "LLM clasif.",
                "llm_categoria": "LLM categoría",
                "llm_intensidad": "LLM intensidad",
                "corrigio_odio": "Corr. odio",
                "corrigio_categoria": "Corr. cat.",
                "corrigio_intensidad": "Corr. int.",
                "annotator_id": "Anotador",
                "label_source": "Origen",
                "split": "Split",
            }),
            use_container_width=True,
            hide_index=True,
            height=400,
        )

    render_section_exports(
        section_key="dataset_gold",
        section_title="Dataset Gold",
        csv_items=[
            ("gold_filtrado", df_f if "df_f" in locals() else pd.DataFrame()),
            ("dudosos_pendientes", df_dudosos if "df_dudosos" in locals() else pd.DataFrame()),
            ("resumen_plataforma", plat_summary_df if "plat_summary_df" in locals() else pd.DataFrame()),
            ("correcciones_categoria", corr_by_cat if "corr_by_cat" in locals() else pd.DataFrame()),
            ("correcciones_anotador", corr_annot if "corr_annot" in locals() else pd.DataFrame()),
            ("resumen_anual", summary.reset_index() if "summary" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Muestras por plataforma", "fig": fig_plat if "fig_plat" in locals() else None, "kind": "plotly"},
            {"title": "Porcentaje de odio por plataforma", "fig": fig_plat_odio if "fig_plat_odio" in locals() else None, "kind": "plotly"},
            {"title": "Distribución odio/no odio", "fig": fig_odio if "fig_odio" in locals() else None, "kind": "plotly"},
            {"title": "Categorías finales", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
            {"title": "Intensidad del odio", "fig": fig_int if "fig_int" in locals() else None, "kind": "plotly"},
            {"title": "Intensidad por categoría", "fig": fig_int_cat if "fig_int_cat" in locals() else None, "kind": "plotly"},
            {"title": "Tasa de corrección humana", "fig": fig_corr if "fig_corr" in locals() else None, "kind": "plotly"},
            {"title": "Matriz de confusión", "fig": fig_cm if "fig_cm" in locals() else None, "kind": "plotly"},
            {"title": "Correcciones por categoría", "fig": fig_corr_cat if "fig_corr_cat" in locals() else None, "kind": "plotly"},
            {"title": "Mensajes por anotador", "fig": fig_annot if "fig_annot" in locals() else None, "kind": "plotly"},
            {"title": "Corrección por anotador", "fig": fig_corr_annot if "fig_corr_annot" in locals() else None, "kind": "plotly"},
            {"title": "Origen del label", "fig": fig_source if "fig_source" in locals() else None, "kind": "plotly"},
            {"title": "Distribución train/test", "fig": fig_split if "fig_split" in locals() else None, "kind": "plotly"},
        ],
    )
