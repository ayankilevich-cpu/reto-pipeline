"""Sección «Categorías de odio (LLM)» del dashboard."""
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
    CAT_COLOR_MAP,
    INTENSITY_COLORS,
    _expand_platforms,
    platform_label,
)
from components.ui import (
    _anonimizar_texto_mensaje,
    _apply_horizontal_bar_labels,
    _is_viewer,
    _render_section_header,
    _role_can_access_raw,
)
from components.exports import render_section_exports
from components.db_helpers import _public_medio_label, load_filter_options


@st.cache_data(ttl=60)
def load_llm_stats() -> dict:
    """Total de mensajes procesados por LLM, desglosado por plataforma."""
    with get_conn() as conn:
        row = pd.read_sql("""
            SELECT
                COUNT(*)                                           AS total_procesados,
                MAX(e.etiquetado_date::date)                       AS ultima_fecha,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    )
                )                                                  AS agregados_ultima,
                COUNT(*) FILTER (WHERE pm.platform IN ('x','twitter'))  AS total_x,
                COUNT(*) FILTER (WHERE pm.platform = 'youtube')         AS total_yt,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    ) AND pm.platform IN ('x','twitter')
                )                                                  AS agregados_x,
                COUNT(*) FILTER (
                    WHERE e.etiquetado_date::date = (
                        SELECT MAX(etiquetado_date::date)
                        FROM processed.etiquetas_llm
                    ) AND pm.platform = 'youtube'
                )                                                  AS agregados_yt
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
        """, conn).iloc[0]
    return {
        "total_procesados": int(row["total_procesados"]),
        "ultima_fecha": row["ultima_fecha"],
        "agregados_ultima": int(row["agregados_ultima"]),
        "total_x": int(row["total_x"]),
        "total_yt": int(row["total_yt"]),
        "agregados_x": int(row["agregados_x"]),
        "agregados_yt": int(row["agregados_yt"]),
    }


@st.cache_data(ttl=300)
def load_categorias(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    intensidades: Optional[Tuple] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    intensidades = list(intensidades) if intensidades else None

    conds = [
        "e.clasificacion_principal = 'ODIO'",
        "e.categoria_odio_pred IS NOT NULL",
        "e.categoria_odio_pred != ''",
    ]
    params = []
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if intensidades:
        conds.append("e.intensidad_pred IN %s"); params.append(tuple(intensidades))

    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred
            ORDER BY total DESC
        """, conn, params=params)
    return df


@st.cache_data(ttl=300)
def load_intensidad_por_categoria(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    categorias = list(categorias) if categorias else None

    conds = [
        "e.clasificacion_principal = 'ODIO'",
        "e.categoria_odio_pred IS NOT NULL AND e.categoria_odio_pred != ''",
        "e.intensidad_pred IS NOT NULL AND e.intensidad_pred != ''",
    ]
    params = []
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if categorias:
        conds.append("e.categoria_odio_pred IN %s"); params.append(tuple(categorias))

    where = " AND ".join(conds)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, e.intensidad_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred, e.intensidad_pred
            ORDER BY e.categoria_odio_pred, e.intensidad_pred
        """, conn, params=params)
    return df


def load_muestra_ultima_corrida_llm(limit: int = 20) -> Tuple[pd.DataFrame, Optional[object]]:
    """
    Hasta `limit` mensajes elegidos al azar entre los etiquetados por LLM
    en la misma fecha calendario que load_llm_stats (última actualización).
    El texto viene de processed.mensajes (contenido ya anonimizado en pipeline).

    No usa @st.cache_data: la muestra aleatoria debe poder variar entre ejecuciones.
    """
    with get_conn() as conn:
        df_meta = pd.read_sql(
            """
            SELECT MAX(etiquetado_date::date) AS ultima_fecha
            FROM processed.etiquetas_llm
            """,
            conn,
        )
        ultima = df_meta.iloc[0]["ultima_fecha"]
        if pd.isna(ultima):
            return pd.DataFrame(), None

        df = pd.read_sql(
            f"""
            SELECT * FROM (
                SELECT DISTINCT ON (e.message_uuid)
                    e.message_uuid,
                    e.clasificacion_principal,
                    e.categoria_odio_pred,
                    e.intensidad_pred,
                    e.resumen_motivo,
                    e.etiquetado_date,
                    pm.content_original,
                    pm.platform,
                    pm.source_media
                FROM processed.etiquetas_llm e
                INNER JOIN processed.mensajes pm USING (message_uuid)
                WHERE e.etiquetado_date::date = %s
                ORDER BY e.message_uuid, e.etiquetado_date DESC NULLS LAST
            ) t
            ORDER BY random()
            LIMIT {int(limit)}
            """,
            conn,
            params=[ultima],
        )
    return df, ultima


def _render_one_muestra_card(row) -> None:
    """Una sola tarjeta de mensaje (fila Series o dict-like)."""
    plat = platform_label(str(row.get("platform") or ""))
    medio_pub = _public_medio_label(row.get("source_media"))
    clasif = (row.get("clasificacion_principal") or "—").strip()
    raw_cat = (row.get("categoria_odio_pred") or "").strip()
    cat_label = CATEGORIAS_LABELS.get(raw_cat, raw_cat or "—")
    intens = (row.get("intensidad_pred") or "").strip() or "—"
    motivo = (row.get("resumen_motivo") or "").strip()
    texto = _anonimizar_texto_mensaje(str(row.get("content_original") or "").strip())
    if len(texto) > 4000:
        texto = texto[:4000] + "…"

    with st.container(border=True):
        if medio_pub:
            st.markdown(
                f"**Plataforma:** {plat} · **Medio monitorizado:** {medio_pub}"
            )
        else:
            st.markdown(f"**Plataforma:** {plat}")
        st.markdown(
            f"**Clasificación:** `{clasif}` · **Categoría:** {cat_label} · "
            f"**Intensidad:** `{intens}`"
        )
        if motivo:
            st.markdown(f"*Resumen (LLM):* {motivo}")
        st.markdown("**Mensaje (anonimizado)**")
        st.text(texto)


def _render_muestra_ultima_corrida_llm_section(*, key_suffix: str = "") -> None:
    """Bloque de UI: carrusel de una tarjeta + flechas (evita scroll vertical largo)."""
    ks = key_suffix or "main"
    idx_key = f"cat_llm_carousel_idx_{ks}"

    st.markdown("### Muestra de mensajes etiquetados por el LLM")
    st.caption(
        "Hasta **20** mensajes al azar del **último día calendario** con etiquetas en "
        "`processed.etiquetas_llm` (misma fecha que la métrica *Última actualización*). "
        "Navegación horizontal con **◀ ▶**; texto desde **processed.mensajes** (anonimizado). "
        "**No se muestran cuentas de usuario ni identificadores personales.** "
        "El nombre del medio solo aparece si consta en el catálogo oficial de medios monitorizados."
    )

    c_btn, _ = st.columns([1, 4])
    if c_btn.button("Nueva muestra aleatoria", key=f"cat_llm_muestra_reroll_{ks}"):
        st.session_state[idx_key] = 0
        st.rerun()

    df_muestra, fecha_muestra = load_muestra_ultima_corrida_llm(limit=20)
    if fecha_muestra is None:
        st.warning(
            "**Sin muestra:** no hay filas en `processed.etiquetas_llm` en la base a la que "
            "conecta esta app (revisá secrets / misma BD que el pipeline)."
        )
    elif df_muestra.empty:
        st.warning(
            "**Sin muestra:** hay fecha de etiquetado en BD pero ningún mensaje hace join con "
            "`processed.mensajes` ese día (UUID desalineados o mensajes no cargados)."
        )
    else:
        n = len(df_muestra)
        if idx_key not in st.session_state:
            st.session_state[idx_key] = 0
        st.session_state[idx_key] = max(0, min(int(st.session_state[idx_key]), n - 1))
        i = int(st.session_state[idx_key])

        if hasattr(fecha_muestra, "strftime"):
            fecha_txt = fecha_muestra.strftime("%d/%m/%Y")
        else:
            fecha_txt = str(fecha_muestra)
        st.caption(
            f"Muestra del **{fecha_txt}** — **{n}** mensajes en el lote · "
            f"mostrando **{i + 1}** de **{n}**."
        )

        col_prev, col_ctr, col_next = st.columns([1, 6, 1])
        with col_prev:
            prev_dis = n <= 1 or i <= 0
            if st.button("◀", key=f"cat_llm_prev_{ks}", help="Anterior", disabled=prev_dis):
                st.session_state[idx_key] = i - 1
                st.rerun()
        with col_ctr:
            st.markdown(
                f"<div style='text-align:center;padding:0.35rem 0;color:#5c6b7a;font-size:0.9rem;'>"
                f"Mensaje <b>{i + 1}</b> / {n}</div>",
                unsafe_allow_html=True,
            )
        with col_next:
            next_dis = n <= 1 or i >= n - 1
            if st.button("▶", key=f"cat_llm_next_{ks}", help="Siguiente", disabled=next_dis):
                st.session_state[idx_key] = i + 1
                st.rerun()

        row = df_muestra.iloc[i]
        _render_one_muestra_card(row)


def render_categorias():
    if _is_viewer():
        _render_section_header(
            "Distribución por categoría de odio",
            "Clasificación por IA en las 6 categorías del proyecto ReTo. "
            "Métricas y gráficos de distribución por categoría e intensidad.",
        )
    else:
        _render_section_header(
            "Distribución por categoría de odio",
            "Clasificación del LLM en las 6 categorías del proyecto ReTo. "
            "<strong>Primero</strong> la muestra de mensajes; <strong>debajo</strong>, métricas y gráficos.",
        )
        _render_muestra_ultima_corrida_llm_section(key_suffix="")
        st.markdown("---")

    llm_stats = load_llm_stats()

    uf = llm_stats["ultima_fecha"]
    total_mensajes_procesados = llm_stats["total_procesados"]
    agregados_ultima_actualizacion = llm_stats["agregados_ultima"]
    ultima_actualizacion = uf.strftime("%d/%m/%Y") if uf is not None and not pd.isna(uf) else "—"
    x_total = llm_stats["total_x"]
    x_ultimos = llm_stats["agregados_x"]
    yt_total = llm_stats["total_yt"]
    yt_ultimos = llm_stats["agregados_yt"]

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
    <div class="label">Total mensajes procesados</div>
    <div class="value">{total_mensajes_procesados:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Agregados en última actualización</div>
    <div class="value">{agregados_ultima_actualizacion:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">Última actualización</div>
    <div class="value">{ultima_actualizacion}</div>
  </div>
  <div class="metric-card">
    <div class="label">X — Total</div>
    <div class="value">{x_total:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">X — Últimos agregados</div>
    <div class="value">{x_ultimos:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">YouTube — Total</div>
    <div class="value">{yt_total:,}</div>
  </div>
  <div class="metric-card">
    <div class="label">YouTube — Últimos agregados</div>
    <div class="value">{yt_ultimos:,}</div>
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown("---")

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2, fc3 = st.columns(3)
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="cat_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="cat_med",
        placeholder="Todos los medios",
    )
    sel_intensidades = fc3.multiselect(
        "Intensidad", opts["intensidades"], default=[], key="cat_int",
        placeholder="Seleccionar…",
    )

    df = load_categorias(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        intensidades=tuple(sel_intensidades) if sel_intensidades else None,
    )
    if df.empty:
        st.warning("No hay datos de categorías con los filtros seleccionados.")
        return

    df["categoria_label"] = df["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df["categoria_odio_pred"])

    col1, col2 = st.columns(2)

    with col1:
        df_categoria = df.sort_values("total", ascending=True)
        fig = px.bar(
            df_categoria, x="total", y="categoria_label", orientation="h",
            color="categoria_label", color_discrete_map=CAT_COLOR_MAP,
            labels={"total": "Mensajes", "categoria_label": ""},
            title="Mensajes de odio por categoría",
            text_auto=True,
        )
        fig.update_layout(showlegend=False, height=400, yaxis=dict(autorange="reversed"))
        _apply_horizontal_bar_labels(fig)
        st.plotly_chart(fig, use_container_width=True, theme=None)

    with col2:
        fig2 = px.pie(
            df, values="total", names="categoria_label",
            color="categoria_label", color_discrete_map=CAT_COLOR_MAP,
            title="Proporción por categoría", hole=0.5,
        )
        fig2.update_traces(
            textposition="inside",
            textinfo="percent",
            textfont_color="white",
            marker=dict(line=dict(color="#FFFFFF", width=2)),
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True, theme=None)

    st.markdown("### Intensidad por categoría")

    sel_cats_int = st.multiselect(
        "Filtrar categorías",
        options=list(CATEGORIAS_LABELS.keys()),
        format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
        default=[],
        key="cat_int_filter",
        placeholder="Todas las categorías",
    )

    df_int = load_intensidad_por_categoria(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats_int) if sel_cats_int else None,
    )
    if not df_int.empty:
        df_int["categoria_label"] = df_int["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df_int["categoria_odio_pred"])
        fig3 = px.bar(
            df_int, x="categoria_label", y="total", color="intensidad_pred",
            barmode="group",
            color_discrete_map=INTENSITY_COLORS,
            labels={"total": "Mensajes", "categoria_label": "", "intensidad_pred": "Intensidad"},
            title="Distribución de intensidad (1=baja, 2=media, 3=alta)",
        )
        fig3.update_layout(height=400, xaxis_tickangle=-30)
        st.plotly_chart(fig3, use_container_width=True, theme=None)

    render_section_exports(
        section_key="categorias_odio",
        section_title="Distribución por categoría de odio",
        csv_items=[
            ("categorias", df),
            ("intensidad_categoria", df_int if "df_int" in locals() else pd.DataFrame()),
        ],
        fig_items=[
            {"title": "Mensajes por categoría", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
            {"title": "Proporción por categoría", "fig": fig2 if "fig2" in locals() else None, "kind": "plotly"},
            {"title": "Intensidad por categoría", "fig": fig3 if "fig3" in locals() else None, "kind": "plotly"},
        ],
    )
