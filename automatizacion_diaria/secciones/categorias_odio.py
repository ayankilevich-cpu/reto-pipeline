"""Sección «Categorías de odio (LLM)» del dashboard."""
from __future__ import annotations

import html
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

from components.constants import (
    CATEGORIAS_LABELS,
    _expand_platforms,
    platform_label,
)
from components.ui import (
    _anonimizar_texto_mensaje,
    _is_viewer,
    _render_section_header,
    _role_can_access_raw,
)
from components.exports import render_descargas_menu
from components.chart_style import (
    NAVY, INT_COLOR as _INT_COLOR, INT_LABEL as _INT_LABEL,
    encabezado_grafico, fmt_int, fmt_pct, kpi_tarjetas, linea_actividad, nota_fuente, tarjeta,
)
from components.db_helpers import (
    _public_medio_label, load_filter_options, _pooled_conn,
    fecha_fin_clasificacion_yt, load_youtube_recorrido,
)


@st.cache_data(ttl=86400, max_entries=64)
def load_llm_stats() -> dict:
    """Total de mensajes procesados por LLM, desglosado por plataforma."""
    with _pooled_conn() as conn:
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


@st.cache_data(ttl=86400, max_entries=64)
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

    with _pooled_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred
            ORDER BY total DESC
        """, conn, params=params)
    return df


@st.cache_data(ttl=86400, max_entries=64)
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

    with _pooled_conn() as conn:
        df = pd.read_sql(f"""
            SELECT e.categoria_odio_pred, e.intensidad_pred, count(*) AS total
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE {where}
            GROUP BY e.categoria_odio_pred, e.intensidad_pred
            ORDER BY e.categoria_odio_pred, e.intensidad_pred
        """, conn, params=params)
    return df


_MIN_MUESTRA_PRECISION = 100
_ETIQUETAS_CLASIF = {"ODIO": "Odio", "NO_ODIO": "No Odio", "DUDOSO": "Dudoso"}

_SQL_LLM_ULTIMA_CAT = """
    SELECT DISTINCT ON (message_uuid)
           message_uuid, clasificacion_principal
    FROM processed.etiquetas_llm
    ORDER BY message_uuid, etiquetado_date DESC NULLS LAST
"""

_SOLO_X = ("x",)   # la sección solo muestra X (YouTube no tiene clasificación completa desde 30/09/2026)
_COLORES_CLASIF = {"Odio": "#A32D2D", "No Odio": "#8AA4BF", "Dudoso": "#D69E2E"}


@st.cache_data(ttl=86400, max_entries=64)
def load_resultado_clasificacion(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> Tuple[pd.DataFrame, int, int]:
    """Clasificación LLM (última versión por mensaje) + precisión vs revisión humana."""
    platforms_l = _expand_platforms(list(platforms) if platforms else None)
    medios_l = list(medios) if medios else None
    conds, params = [], []
    if platforms_l:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms_l))
    if medios_l:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios_l))
    where = f"WHERE {' AND '.join(conds)}" if conds else ""
    and_conds = f"AND {' AND '.join(conds)}" if conds else ""

    cte = f"""
        WITH e AS ({_SQL_LLM_ULTIMA_CAT})
    """
    with _pooled_conn() as conn:
        df = pd.read_sql(f"""
            {cte}
            SELECT e.clasificacion_principal, COUNT(*) AS total
            FROM e JOIN processed.mensajes pm USING (message_uuid)
            {where}
            GROUP BY e.clasificacion_principal
        """, conn, params=params)
        prec = pd.read_sql(f"""
            {cte}
            SELECT COUNT(*) FILTER (WHERE g.y_odio_final IS NOT NULL) AS revisados,
                   COUNT(*) FILTER (WHERE g.y_odio_final = 'Odio')    AS confirmados
            FROM e
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE e.clasificacion_principal = 'ODIO' {and_conds}
        """, conn, params=params).iloc[0]
    return df, int(prec["revisados"] or 0), int(prec["confirmados"] or 0)


def _render_resultado_clasificacion(sel_medios):
    df_res, n_rev, n_conf = load_resultado_clasificacion(
        platforms=_SOLO_X,
        medios=tuple(sel_medios) if sel_medios else None,
    )
    if df_res.empty:
        st.info("Sin resultados de clasificación para los filtros seleccionados.")
        return None
    df_res = df_res.copy()
    df_res["Clasificación"] = (
        df_res["clasificacion_principal"].map(_ETIQUETAS_CLASIF)
        .fillna(df_res["clasificacion_principal"])
    )
    total = int(df_res["total"].sum())
    orden = ["Odio", "No Odio", "Dudoso"]
    vals = {e: int(df_res.loc[df_res["Clasificación"] == e, "total"].sum()) for e in orden}
    pcts = {e: (vals[e] / total * 100 if total else 0.0) for e in orden}

    fig = go.Figure()
    for e in orden:
        fig.add_trace(go.Bar(
            orientation="h", y=[""], x=[pcts[e]], name=e, marker_color=_COLORES_CLASIF[e],
            customdata=[[fmt_int(vals[e]), fmt_pct(pcts[e])]],
            hovertemplate=f"{e}: %{{customdata[1]}}<br>%{{customdata[0]}} mensajes<extra></extra>",
        ))
    fig.update_layout(
        barmode="stack", height=60, showlegend=False, margin=dict(l=0, r=0, t=4, b=4),
        xaxis=dict(visible=False, range=[0, 100], fixedrange=True),
        yaxis=dict(visible=False, fixedrange=True),
    )
    filas = "".join(
        "<div style='display:grid;grid-template-columns:18px 90px 70px 1fr;align-items:center;"
        "font-size:14.5px;color:#14202E'>"
        f"<span style='width:11px;height:11px;border-radius:3px;background:{_COLORES_CLASIF[e]};display:inline-block'></span>"
        f"<span>{e}</span><span style='font-weight:700;text-align:right;padding-right:16px'>{fmt_pct(pcts[e])}</span>"
        f"<span style='font-size:13px;color:#7A8699'>{fmt_int(vals[e])} mensajes</span></div>"
        for e in orden
    )
    nota = (
        "No es la proporción de odio en el total de conversaciones: la IA solo analiza los candidatos "
        "del filtro de términos (ver el recorrido en el Panel general)."
    )
    if n_rev >= _MIN_MUESTRA_PRECISION:
        nota += (
            f" En la muestra revisada por personas ({fmt_int(n_rev)} mensajes), el "
            f"{n_conf / n_rev * 100:.0f} % de lo marcado como odio fue confirmado."
        )
    encabezado_grafico(
        f"La IA clasificó como odio el {fmt_pct(pcts['Odio'])} de los mensajes analizados",
        "Mensajes de X analizados por la IA tras el filtro de términos.",
    )
    st.plotly_chart(fig, use_container_width=True, theme=None)
    st.markdown(
        "<div style='display:grid;gap:10px;margin:6px 0 0'>" + filas + "</div>"
        "<p style='font-size:13.5px;color:#5B6675;margin:14px 0 4px;border-top:1px solid #EEF1F5;"
        f"padding-top:12px'>{html.escape(nota)}</p>",
        unsafe_allow_html=True,
    )
    return fig


def _render_recuadro_youtube() -> None:
    try:
        yt = load_youtube_recorrido()
    except Exception:
        return
    if not yt["total"]:
        return
    pct_an = yt["analizados"] / yt["total"] * 100
    pct_rel = yt["relevantes"] / yt["analizados"] * 100 if yt["analizados"] else 0.0
    pasos = [
        (fmt_int(yt["total"]), "comentarios recogidos", "#14202E"),
        (fmt_int(yt["analizados"]), f"analizados por el filtro de odio de la IA ({fmt_pct(pct_an)} del total)", "#1F4E79"),
        (fmt_int(yt["relevantes"]), f"señalados como posible odio ({fmt_pct(pct_rel)} de los analizados)", "#A32D2D"),
    ]
    flecha = (
        "<div style='align-self:center;color:#A0AEC0;font-size:22px;line-height:1;padding:0 4px'>→</div>"
    )
    celdas = flecha.join(
        "<div style='flex:1 1 180px;min-width:0'>"
        f"<div style='font-size:26px;font-weight:700;letter-spacing:-0.02em;color:{color};"
        f"font-variant-numeric:tabular-nums'>{v}</div>"
        f"<div style='font-size:13.5px;color:#5B6675;line-height:1.35'>{html.escape(t)}</div></div>"
        for v, t, color in pasos
    )
    st.markdown(
        "<div style='border:1px solid #E3E8EF;background:#F6F8FB;border-radius:12px;padding:16px 18px;margin:0 0 22px'>"
        "<div style='display:flex;gap:10px;align-items:flex-start'>"
        "<span style='flex:none;width:18px;height:18px;border-radius:50%;background:#1F4E79;color:#fff;"
        "font:700 12px/18px Inter,sans-serif;text-align:center;margin-top:2px'>i</span>"
        "<div><strong style='color:#14202E'>YouTube sigue otro proceso</strong>"
        "<p style='margin:2px 0 0;font-size:14px;color:#3B4757;max-width:95ch'>"
        f"Desde el {fecha_fin_clasificacion_yt(yt)} los comentarios de YouTube no se clasifican por categoría con IA. "
        "Un filtro de IA analiza los comentarios más recientes, con un límite por ejecución, y señala los que "
        "pueden contener odio para que el equipo los revise. Por eso esta sección muestra solo X. "
        "Las categorías de YouTube validadas por personas están en <em>Dataset validado</em>.</p></div></div>"
        "<div style='display:flex;flex-wrap:wrap;gap:12px;margin-top:16px;padding-left:28px'>"
        f"{celdas}</div></div>",
        unsafe_allow_html=True,
    )


def load_muestra_ultima_corrida_llm(limit: int = 20) -> Tuple[pd.DataFrame, Optional[object]]:
    """
    Hasta `limit` mensajes elegidos al azar entre los etiquetados por LLM
    en la misma fecha calendario que load_llm_stats (última actualización).
    El texto viene de processed.mensajes (contenido ya anonimizado en pipeline).

    No usa @st.cache_data: la muestra aleatoria debe poder variar entre ejecuciones.
    """
    with _pooled_conn() as conn:
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
    _col_tit, _col_dl = st.columns([6, 1])
    with _col_tit:
        if _is_viewer():
            _render_section_header(
                "Categorías de odio",
                "Clasificación por IA de los mensajes de X en las 6 categorías del proyecto ReTo, y su intensidad.",
            )
        else:
            _render_section_header(
                "Categorías de odio",
                "Clasificación del LLM en las 6 categorías del proyecto ReTo. "
                "<strong>Primero</strong> la muestra de mensajes; <strong>debajo</strong>, métricas y gráficos.",
            )
    if not _is_viewer():
        _render_muestra_ultima_corrida_llm_section(key_suffix="")
        st.markdown("---")

    llm_stats = load_llm_stats()

    uf = llm_stats["ultima_fecha"]
    ultima_actualizacion = uf.strftime("%d/%m/%Y") if uf is not None and not pd.isna(uf) else "—"
    x_total = llm_stats["total_x"]
    x_ultimos = llm_stats["agregados_x"]

    kpi_tarjetas([
        ("Mensajes de X clasificados por IA", fmt_int(x_total), ""),
        ("Añadidos en la última actualización", fmt_int(x_ultimos), ""),
        ("Última clasificación", ultima_actualizacion, ""),
    ])
    linea_actividad("Clasificación diaria de los mensajes de X que superan el filtro de términos.")
    _render_recuadro_youtube()

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2 = st.columns(2)
    sel_medios = fc1.multiselect(
        "Medio", opts["medios"], default=[], key="cat_med",
        placeholder="Todos los medios",
    )
    sel_intensidades = fc2.multiselect(
        "Intensidad", opts["intensidades"], default=[], key="cat_int",
        placeholder="Todas",
    )

    df = load_categorias(
        platforms=_SOLO_X,
        medios=tuple(sel_medios) if sel_medios else None,
        intensidades=tuple(sel_intensidades) if sel_intensidades else None,
    )

    col_res, col_cat = st.columns([5, 7])
    with col_res:
        with tarjeta():
            fig_res = _render_resultado_clasificacion(sel_medios)

    if df.empty:
        with col_cat:
            st.warning("No hay datos de categorías con los filtros seleccionados.")
        return

    df["categoria_label"] = df["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df["categoria_odio_pred"])
    df_c = df.sort_values("total", ascending=False).reset_index(drop=True)
    _tot_c = int(df_c["total"].sum()) or 1
    df_c["pct"] = df_c["total"] / _tot_c * 100

    with col_cat:
        with tarjeta():
            fig = go.Figure(go.Bar(
                orientation="h", y=df_c["categoria_label"], x=df_c["total"],
                marker_color=[NAVY if i == 0 else "#8AA4BF" for i in range(len(df_c))],
                text=[f"<b>{fmt_int(t)}</b>  {fmt_pct(p)}" for t, p in zip(df_c["total"], df_c["pct"])],
                textposition="outside", cliponaxis=False, textfont=dict(size=13, color="#14202E"),
                customdata=[[fmt_int(t), fmt_pct(p)] for t, p in zip(df_c["total"], df_c["pct"])],
                hovertemplate="<b>%{y}</b><br>%{customdata[0]} mensajes · %{customdata[1]} del odio<extra></extra>",
            ))
            fig.update_layout(
                height=70 + 48 * len(df_c), showlegend=False, bargap=0.4,
                margin=dict(l=8, r=120, t=6, b=6),
                xaxis=dict(visible=False, range=[0, float(df_c["total"].max()) * 1.35], fixedrange=True),
                yaxis=dict(autorange="reversed", showgrid=False, fixedrange=True, ticksuffix="  ",
                           tickfont=dict(size=13, color="#14202E")),
            )
            encabezado_grafico(
                f"La categoría «{df_c.loc[0, 'categoria_label']}» concentra el "
                f"{fmt_pct(df_c.loc[0, 'pct'])} del odio identificado",
                "Mensajes de X clasificados como odio en cada categoría y su peso sobre el total de odio.",
            )
            st.plotly_chart(fig, use_container_width=True, theme=None)

    df_int = pd.DataFrame()
    with tarjeta():
        sel_cats_int = st.multiselect(
            "Filtrar categorías",
            options=list(CATEGORIAS_LABELS.keys()),
            format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
            default=[],
            key="cat_int_filter",
            placeholder="Todas las categorías",
        )
        df_int = load_intensidad_por_categoria(
            platforms=_SOLO_X,
            medios=tuple(sel_medios) if sel_medios else None,
            categorias=tuple(sel_cats_int) if sel_cats_int else None,
        )
        if df_int.empty:
            st.info("Sin datos de intensidad para los filtros seleccionados.")
        else:
            df_int["categoria_label"] = df_int["categoria_odio_pred"].map(CATEGORIAS_LABELS).fillna(df_int["categoria_odio_pred"])
            df_int["intensidad_pred"] = df_int["intensidad_pred"].astype(str)
            df_int["total_categoria"] = df_int.groupby("categoria_label")["total"].transform("sum")
            df_int["pct"] = (df_int["total"] / df_int["total_categoria"] * 100).round(1)
            df_int["pct_txt"] = df_int["pct"].map(lambda v: f"{v:.0f} %" if v >= 12 else "")
            _alta = (
                df_int[df_int["intensidad_pred"] == "3"].set_index("categoria_label")["pct"]
                .reindex(df_int["categoria_label"].unique(), fill_value=0)
                .sort_values(ascending=False)
            )
            orden_cat = _alta.index.tolist()

            fig3 = px.bar(
                df_int, x="pct", y="categoria_label", color="intensidad_pred",
                orientation="h", barmode="stack", text="pct_txt",
                category_orders={"intensidad_pred": ["1", "2", "3"], "categoria_label": orden_cat},
                color_discrete_map=_INT_COLOR,
                custom_data=["total", "total_categoria"],
            )
            fig3.for_each_trace(lambda t: t.update(name=_INT_LABEL.get(t.name, t.name)))
            fig3.update_traces(
                textposition="inside", insidetextanchor="middle",
                textfont=dict(color="#FFFFFF", size=12),
                hovertemplate=("<b>%{y}</b><br>Nivel %{fullData.name}: %{x:.1f} %"
                               "<br>%{customdata[0]:,} de %{customdata[1]:,} mensajes<extra></extra>"),
            )
            fig3.for_each_trace(lambda t: t.update(textfont_color="#1A202C") if t.name == "Baja (1)" else None)
            for cat, v in _alta.items():
                fig3.add_annotation(
                    x=100, y=cat, xanchor="left", xshift=10, showarrow=False,
                    text=f"<b>{fmt_pct(v, 0)}</b>", font=dict(size=14, color="#A32D2D"),
                )
            fig3.update_layout(
                height=70 + 46 * len(orden_cat), bargap=0.35, showlegend=False,
                margin=dict(l=8, r=70, t=6, b=6),
                xaxis=dict(visible=False, range=[0, 100], fixedrange=True),
                yaxis=dict(autorange="reversed", automargin=True, showgrid=False, fixedrange=True,
                           ticksuffix="  ", tickfont=dict(size=13, color="#14202E"), title=None),
            )
            _sw = lambda c: (f"<span style='display:inline-block;width:11px;height:11px;border-radius:3px;"
                             f"background:{c};margin-right:6px;vertical-align:-1px'></span>")
            encabezado_grafico(
                f"«{orden_cat[0]}» es la categoría con más odio de intensidad alta "
                f"({fmt_pct(_alta.iloc[0], 0)})",
                "Distribución de la intensidad dentro de cada categoría, ordenada por % de intensidad alta "
                "(cifra en rojo a la derecha).",
            )
            st.markdown(
                "<div style='display:flex;gap:16px;flex-wrap:wrap;font-size:13px;color:#5B6675;margin:2px 0 6px'>"
                + "".join(f"<span>{_sw(_INT_COLOR[k])}{_INT_LABEL[k]}</span>" for k in ["1", "2", "3"])
                + "</div>",
                unsafe_allow_html=True,
            )
            st.plotly_chart(fig3, use_container_width=True, theme=None)

    nota_fuente()

    with _col_dl:
        render_descargas_menu(
            section_key="categorias_odio",
            section_title="Categorías de odio",
            csv_items=[
                ("categorias", df),
                ("intensidad_categoria", df_int),
            ],
            fig_items=[
                {"title": "Resultado de la clasificación por IA", "fig": fig_res, "kind": "plotly"},
                {"title": "Mensajes por categoría", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad por categoría", "fig": fig3 if "fig3" in locals() else None, "kind": "plotly"},
            ],
        )
