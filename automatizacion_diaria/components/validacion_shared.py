"""Renderers compartidos de análisis de errores vLLM (Calidad LLM + Anotación)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Callable
import json
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import CATEGORIAS_LABELS, COLORS
from components.db_helpers import _load_vllm_yt_corrections


def _render_vllm_label_error_analysis(
    corrections_fn: Callable[[], pd.DataFrame],
    *,
    key_prefix: str,
    file_tag: str,
) -> None:
    """Panel de concordancia LLM vs Humano (YouTube o X); `corrections_fn` devuelve el DataFrame."""
    df = corrections_fn()

    if df.empty or len(df) < 3:
        st.info(
            f"Se necesitan al menos 3 validaciones para mostrar el análisis "
            f"(actualmente: {len(df)})."
        )
        return

    total = len(df)
    coincide_col = df["coincide_con_llm"]
    n_coincide = int(coincide_col.sum()) if coincide_col.notna().any() else 0
    n_corregidos = total - n_coincide
    accuracy = n_coincide / total * 100 if total else 0

    corrigio_clasif = (df["llm_clasif"] != df["humano_clasif"]).sum()
    pct_corr_clasif = corrigio_clasif / total * 100

    df_ambos_odio = df[
        (df["llm_clasif"] == "ODIO") & (df["humano_clasif"] == "ODIO")
    ]
    corrigio_cat = 0
    corrigio_int = 0
    if not df_ambos_odio.empty:
        corrigio_cat = (
            df_ambos_odio["llm_categoria"].fillna("") !=
            df_ambos_odio["humano_categoria"].fillna("")
        ).sum()
        corrigio_int = (
            df_ambos_odio["llm_intensidad"].astype(str).fillna("") !=
            df_ambos_odio["humano_intensidad"].astype(str).fillna("")
        ).sum()

    # ── KPIs ──
    st.markdown("#### Resumen de concordancia")
    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Validaciones", f"{total:,}")
    m2.metric("Concordancia total", f"{accuracy:.1f}%")
    m3.metric("Corrigió clasificación", f"{corrigio_clasif:,} ({pct_corr_clasif:.0f}%)")
    m4.metric("Corrigió categoría", f"{corrigio_cat:,}" if not df_ambos_odio.empty else "—")
    m5.metric("Corrigió intensidad", f"{corrigio_int:,}" if not df_ambos_odio.empty else "—")

    # ── Matriz de confusión: clasificación ──
    st.markdown("---")
    col_cm1, col_cm2 = st.columns(2)

    with col_cm1:
        st.markdown("##### Matriz de confusión — Clasificación")
        labels_order = ["ODIO", "NO_ODIO", "DUDOSO"]
        ct = pd.crosstab(
            df["llm_clasif"], df["humano_clasif"],
            rownames=["LLM"], colnames=["Humano"],
        ).reindex(index=labels_order, columns=labels_order, fill_value=0)

        fig_cm = go.Figure(data=go.Heatmap(
            z=ct.values,
            x=ct.columns.tolist(),
            y=ct.index.tolist(),
            text=ct.values,
            texttemplate="%{text}",
            colorscale="RdYlGn_r",
            showscale=False,
        ))
        fig_cm.update_layout(
            xaxis_title="Humano (gold)", yaxis_title="LLM (predicción)",
            height=340, margin=dict(t=10),
        )
        st.plotly_chart(fig_cm, use_container_width=True)

    with col_cm2:
        st.markdown("##### Tasa de corrección por dimensión")
        corr_data = pd.DataFrame({
            "Dimensión": ["Clasificación", "Categoría", "Intensidad"],
            "% Corregido": [
                pct_corr_clasif,
                (corrigio_cat / len(df_ambos_odio) * 100) if len(df_ambos_odio) else 0,
                (corrigio_int / len(df_ambos_odio) * 100) if len(df_ambos_odio) else 0,
            ],
        })
        corr_data["% Coincide"] = 100 - corr_data["% Corregido"]

        fig_corr = go.Figure()
        fig_corr.add_trace(go.Bar(
            x=corr_data["Dimensión"], y=corr_data["% Coincide"],
            name="Coincide", marker_color=COLORS["success"],
        ))
        fig_corr.add_trace(go.Bar(
            x=corr_data["Dimensión"], y=corr_data["% Corregido"],
            name="Corregido", marker_color=COLORS["danger"],
        ))
        fig_corr.update_layout(
            barmode="stack", yaxis_title="%", height=340,
            margin=dict(t=10),
            legend=dict(orientation="h", yanchor="bottom", y=-0.3),
        )
        st.plotly_chart(fig_corr, use_container_width=True)

    # ── Matriz de confusión: categoría (solo casos donde ambos = ODIO) ──
    if not df_ambos_odio.empty and corrigio_cat > 0:
        st.markdown("---")
        st.markdown("##### Confusión de categorías (donde LLM y humano = ODIO)")

        df_cat_cm = df_ambos_odio.dropna(subset=["llm_categoria", "humano_categoria"])
        if not df_cat_cm.empty:
            cat_order = list(CATEGORIAS_LABELS.keys())
            ct_cat = pd.crosstab(
                df_cat_cm["llm_categoria"], df_cat_cm["humano_categoria"],
                rownames=["LLM"], colnames=["Humano"],
            ).reindex(index=cat_order, columns=cat_order, fill_value=0)
            ct_cat = ct_cat.loc[
                ct_cat.sum(axis=1) > 0, ct_cat.sum(axis=0) > 0
            ]

            cat_display = {k: v.split("/")[0].strip() for k, v in CATEGORIAS_LABELS.items()}
            fig_cat = go.Figure(data=go.Heatmap(
                z=ct_cat.values,
                x=[cat_display.get(c, c) for c in ct_cat.columns],
                y=[cat_display.get(c, c) for c in ct_cat.index],
                text=ct_cat.values,
                texttemplate="%{text}",
                colorscale="RdYlGn_r",
                showscale=False,
            ))
            fig_cat.update_layout(
                xaxis_title="Humano", yaxis_title="LLM",
                height=380, margin=dict(t=10),
                xaxis_tickangle=-25,
            )
            st.plotly_chart(fig_cat, use_container_width=True)

    # ── Sesgo de intensidad ──
    if not df_ambos_odio.empty:
        df_int = df_ambos_odio.dropna(subset=["llm_intensidad", "humano_intensidad"]).copy()
        if not df_int.empty:
            st.markdown("---")
            st.markdown("##### Sesgo de intensidad")
            df_int["llm_int"] = pd.to_numeric(df_int["llm_intensidad"], errors="coerce")
            df_int["hum_int"] = pd.to_numeric(df_int["humano_intensidad"], errors="coerce")
            df_int["diff"] = df_int["llm_int"] - df_int["hum_int"]
            mean_diff = df_int["diff"].mean()

            bias_text = (
                "sin sesgo" if abs(mean_diff) < 0.1
                else f"**sobreestima** en {mean_diff:.2f} puntos" if mean_diff > 0
                else f"**subestima** en {abs(mean_diff):.2f} puntos"
            )
            st.markdown(f"Sesgo medio del LLM: {bias_text} (media diff = {mean_diff:+.2f})")

            int_ct = pd.crosstab(
                df_int["llm_int"].astype(int), df_int["hum_int"].astype(int),
                rownames=["LLM"], colnames=["Humano"],
            ).reindex(index=[1, 2, 3], columns=[1, 2, 3], fill_value=0)

            fig_int = go.Figure(data=go.Heatmap(
                z=int_ct.values,
                x=["1 Leve", "2 Ofensivo", "3 Hostil"],
                y=["1 Leve", "2 Ofensivo", "3 Hostil"],
                text=int_ct.values,
                texttemplate="%{text}",
                colorscale="YlOrRd",
                showscale=False,
            ))
            fig_int.update_layout(
                xaxis_title="Humano", yaxis_title="LLM",
                height=300, margin=dict(t=10),
            )
            st.plotly_chart(fig_int, use_container_width=True)

    # ── Tabla de correcciones ──
    st.markdown("---")
    st.markdown("##### Mensajes donde el humano corrigió al LLM")

    df_err = df[df["coincide_con_llm"] == False].copy()  # noqa: E712
    if df_err.empty:
        st.success("No hay correcciones: el LLM acertó en todos los casos validados.")
    else:
        st.caption(f"{len(df_err)} correcciones de {total} validaciones")
        display_df = df_err[[
            "content_original",
            "llm_clasif", "humano_clasif",
            "llm_categoria", "humano_categoria",
            "llm_intensidad", "humano_intensidad",
            "humano_humor", "annotator_id",
        ]].copy()
        display_df.columns = [
            "Texto", "LLM Clasif.", "Humano Clasif.",
            "LLM Categoría", "Humano Categoría",
            "LLM Intens.", "Humano Intens.",
            "Humor", "Anotador",
        ]
        display_df["LLM Categoría"] = display_df["LLM Categoría"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x) if pd.notna(x) else "—"
        )
        display_df["Humano Categoría"] = display_df["Humano Categoría"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x) if pd.notna(x) else "—"
        )
        st.dataframe(display_df, use_container_width=True, hide_index=True,
                      key=f"{key_prefix}_errors_table")

    # ── Exportar correcciones como few-shot para el prompt ──
    st.markdown("---")
    st.markdown("##### Exportar para mejora del prompt")

    if df_err.empty:
        st.info("No hay correcciones para exportar.")
    else:
        few_shot_lines = []
        for _, row in df_err.iterrows():
            txt = str(row["content_original"])[:500]
            h_clasif = row["humano_clasif"]
            h_cat = row.get("humano_categoria") or ""
            h_int = row.get("humano_intensidad") or ""
            l_clasif = row["llm_clasif"]
            l_cat = row.get("llm_categoria") or ""

            entry = {
                "comentario": txt,
                "clasificacion_correcta": h_clasif,
                "error_del_llm": l_clasif,
            }
            if h_clasif == "ODIO":
                entry["categoria_correcta"] = h_cat
                entry["intensidad_correcta"] = str(h_int)
                if l_clasif == "ODIO" and l_cat != h_cat:
                    entry["categoria_erronea_llm"] = l_cat
            few_shot_lines.append(entry)

        few_shot_json = json.dumps(few_shot_lines, ensure_ascii=False, indent=2)

        st.download_button(
            "Descargar correcciones (JSON)",
            data=few_shot_json,
            file_name=f"correcciones_llm_{file_tag}_few_shot.json",
            mime="application/json",
            key=f"{key_prefix}_download_json",
        )

        prompt_block = "EJEMPLOS DE CORRECCIÓN (few-shot):\n"
        prompt_block += "Los siguientes son casos donde la clasificación correcta "
        prompt_block += "difiere de una predicción previa. Usalos como referencia:\n\n"
        for i, ex in enumerate(few_shot_lines[:15], 1):
            prompt_block += f"Ejemplo {i}:\n"
            prompt_block += f"  Comentario: {ex['comentario'][:200]}\n"
            prompt_block += f"  Clasificación correcta: {ex['clasificacion_correcta']}\n"
            if "categoria_correcta" in ex:
                prompt_block += f"  Categoría correcta: {ex['categoria_correcta']}\n"
                prompt_block += f"  Intensidad correcta: {ex['intensidad_correcta']}\n"
            prompt_block += f"  (El LLM había predicho: {ex['error_del_llm']}"
            if "categoria_erronea_llm" in ex:
                prompt_block += f", categoría: {ex['categoria_erronea_llm']}"
            prompt_block += ")\n\n"

        st.download_button(
            "Descargar bloque para prompt (TXT)",
            data=prompt_block,
            file_name=f"few_shot_block_prompt_{file_tag}.txt",
            mime="text/plain",
            key=f"{key_prefix}_download_prompt",
        )

        with st.expander("Vista previa del bloque few-shot"):
            st.code(prompt_block[:3000], language="text")


def _render_vllm_yt_error_analysis() -> None:
    _render_vllm_label_error_analysis(
        _load_vllm_yt_corrections, key_prefix="vllm_yt", file_tag="yt",
    )
