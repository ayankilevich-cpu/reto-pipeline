"""Sección «Buscador y Análisis» del dashboard."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Tuple
import re
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import get_conn
from components.constants import CATEGORIAS_LABELS, platform_label
from components.ui import (
    _anonimizar_texto_mensaje,
    _apply_horizontal_bar_labels,
    _render_section_header,
    _ui_label,
)
from components.exports import df_to_csv_bytes
from components.db_helpers import _public_medio_label


_INTENSIDAD_NUMERICA = {"1": 1, "2": 2, "3": 3}


_MEDIOS_AUDITADOS = {
    # X
    "DiarioDAlmeria", "101tvMalaga", "diariosevilla", "DiarioJAENes",
    "granadahoy", "europapress", "ideal_granada", "canalsur",
    "EFEnoticias", "AhoraGranada", "granadadigital", "OndaCero_es",
    "DiarioSUR", "HoraJaen", "malaga_ee", "opiniondemalaga",
    "JaenHoyDiario", "CadizDirecto", "ElSoldAntequera", "IndeGranada",
    "MijasCom", "8Directo_", "noticiasmira", "DiarioAvanza",
    "101tvAntequera", "lacontradejaen", "EstrechoDigital", "andaluciainf",
    "9laLoma", "Sur_de_Cordoba", "motrildigital", "NoticiasGr",
    "RTVMarbella", "LosBarriosHoy", "CastilloSanFdo", "DIARIOBC",
    "CordobaBN", "lasemana", "AljarafeDigital", "7TVAndalucia",
    "fuengirolatv", "IInfoLinares",
    # YouTube
    "El País – Edición Andalucía", "Onda Cero Andalucía",
    "Europa Press Andalucía", "COPE Andalucía", "Agencia EFE",
    "Betis TV", "Cádiz Directo",
}


@st.cache_data(ttl=300)
def _load_buscador_resultados(termino: str) -> Tuple[pd.DataFrame, bool]:
    """
    Busca mensajes cuyo content_original contenga el término (ILIKE).
    Devuelve (df, truncado). Si hay >5000 filas, corta a 5000 y truncado=True.
    """
    if not termino:
        return pd.DataFrame(), False

    pattern = f"%{termino}%"
    with get_conn() as conn:
        df = pd.read_sql(
            """
            SELECT
                m.message_uuid,
                m.content_original,
                m.source_media,
                m.platform,
                m.created_at,
                e.clasificacion_principal,
                e.categoria_odio_pred,
                e.intensidad_pred,
                e.resumen_motivo
            FROM processed.mensajes m
            LEFT JOIN processed.etiquetas_llm e ON m.message_uuid = e.message_uuid
            WHERE m.content_original ILIKE %(termino)s
              AND m.source_media = ANY(%(medios)s)
            ORDER BY m.created_at DESC
            LIMIT 5001
            """,
            conn,
            params={
                "termino": pattern,
                "medios": list(_MEDIOS_AUDITADOS),
            },
        )

    truncado = len(df) > 5000
    if truncado:
        df = df.head(5000).copy()
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")
    return df, truncado


def render_buscador_terminos() -> None:
    _render_section_header(
        "Buscador y análisis",
        "Búsqueda de términos o frases en el corpus monitorizado (reacciones agregadas, sin identificar usuarios).",
    )

    # 2) Input + 3) Toggle LLM
    col_input, col_toggle = st.columns([3, 2])
    with col_input:
        termino = st.text_input(
            "Término o frase a buscar",
            key="buscador_terminos_input",
            placeholder="Ej.: inmigración, okupas, vivienda, etc.",
        ).strip()
    with col_toggle:
        solo_llm = st.checkbox(
            _ui_label("Analizar solo mensajes clasificados por LLM"),
            value=False,
            key="buscador_terminos_only_llm",
        )
    plataforma = st.radio(
        "Plataforma",
        options=["Todas", "X", "YouTube"],
        horizontal=True,
        key="buscador_plataforma",
    )

    categoria_options = ["Todas", *list(CATEGORIAS_LABELS.keys())]
    col_cat, col_fd, col_fh = st.columns([2, 1, 1])
    with col_cat:
        categoria_sel = st.selectbox(
            _ui_label("Filtrar por categoría LLM"),
            options=categoria_options,
            format_func=lambda x: (
                "Todas" if x == "Todas" else CATEGORIAS_LABELS.get(x, x)
            ),
            index=0,
            key="buscador_categoria_filter",
        )
    with col_fd:
        fecha_desde = st.date_input(
            "Fecha desde", value=None, key="buscador_fecha_desde",
        )
    with col_fh:
        fecha_hasta = st.date_input(
            "Fecha hasta", value=None, key="buscador_fecha_hasta",
        )
    categoria_filter = categoria_sel if categoria_sel != "Todas" else None

    if fecha_desde and fecha_hasta and fecha_desde > fecha_hasta:
        st.warning("La fecha **desde** no puede ser posterior a la fecha **hasta**.")
        return

    if not termino:
        st.info("Ingresá un término para comenzar.")
        return

    df_all, truncado = _load_buscador_resultados(termino)

    # Reset paginación si cambió el término
    if st.session_state.get("buscador_ultimo_termino") != termino:
        st.session_state["buscador_page_idx"] = 0
        st.session_state["buscador_ultimo_termino"] = termino

    if truncado:
        st.warning("Resultado amplio — mostrando primeros 5000 mensajes.")

    if df_all.empty:
        st.info("No se encontraron mensajes para ese término.")
        return

    # Filtro toggle LLM
    if solo_llm:
        df = df_all[df_all["clasificacion_principal"].notna()].copy()
    else:
        df = df_all.copy()
    if plataforma == "X":
        df = df[df["platform"] == "x"].copy()
    elif plataforma == "YouTube":
        df = df[df["platform"] == "youtube"].copy()
    if categoria_filter:
        df = df[df["categoria_odio_pred"] == categoria_filter].copy()
    # created_at es tz-aware (timestamptz → datetime64[..., UTC]);
    # st.date_input entrega date naive → Timestamp(fecha) naive rompe en pandas 2.x.
    if fecha_desde or fecha_hasta:
        tz = df["created_at"].dt.tz
        if fecha_desde:
            lim_desde = pd.Timestamp(fecha_desde, tz=tz)
            df = df[df["created_at"].notna() & (df["created_at"] >= lim_desde)].copy()
        if fecha_hasta:
            lim_hasta = pd.Timestamp(fecha_hasta, tz=tz) + pd.Timedelta(days=1)
            df = df[df["created_at"].notna() & (df["created_at"] < lim_hasta)].copy()

    if df.empty:
        st.info("No se encontraron mensajes para los filtros seleccionados.")
        return

    # Texto anonimizado reusable
    df["content_clean"] = (
        df["content_original"].fillna("").astype(str).map(_anonimizar_texto_mensaje)
    )

    # 4) KPIs
    total = len(df)
    medios_distintos = int(df["source_media"].nunique())
    total_clasificados = int(df["clasificacion_principal"].notna().sum())
    pct_clasif = (total_clasificados / total * 100.0) if total else 0.0
    fmin = df["created_at"].min()
    fmax = df["created_at"].max()

    if solo_llm:
        k1, k2, k3, k4, k5 = st.columns(5)
    else:
        k1, k2, k3, k4 = st.columns(4)
    k1.metric("Menciones encontradas", f"{total:,}")
    k2.metric("Medios distintos", f"{medios_distintos:,}")
    k3.metric(_ui_label("% clasificado por LLM"), f"{pct_clasif:.1f}%")
    periodo_txt = (
        f"{fmin.strftime('%d/%m/%y')} → {fmax.strftime('%d/%m/%y')}"
        if pd.notna(fmin) and pd.notna(fmax)
        else "—"
    )
    k4.markdown(
        f"**Período cubierto**<br><span style='font-size:0.8rem'>{periodo_txt}</span>",
        unsafe_allow_html=True,
    )

    if solo_llm:
        df_odio = df[df["clasificacion_principal"] == "ODIO"].copy()
        df_odio["intens_num"] = df_odio["intensidad_pred"].map(_INTENSIDAD_NUMERICA)
        intens_media = df_odio["intens_num"].mean()
        intens_txt = f"{intens_media:.2f} / 3" if pd.notna(intens_media) else "—"
        k5.metric("Intensidad media (odio)", intens_txt)

    # 5) Evolución temporal
    st.markdown("### Evolución de menciones en el tiempo")
    df_sem = df.copy()
    df_sem["semana"] = (
        pd.to_datetime(df_sem["created_at"]).dt.to_period("W-MON").dt.start_time
    )
    semanal = (
        df_sem.dropna(subset=["semana"])
        .groupby("semana")
        .size()
        .reset_index(name="menciones")
    )

    fig_evol = go.Figure()
    fig_evol.add_trace(
        go.Scatter(
            x=semanal["semana"],
            y=semanal["menciones"],
            name="Menciones",
            mode="lines+markers",
        )
    )

    if solo_llm:
        df_odio = df_sem[df_sem["clasificacion_principal"] == "ODIO"]
        sem_odio = (
            df_odio.dropna(subset=["semana"])
            .groupby("semana")
            .size()
            .reset_index(name="odio")
        )
        sem_join = semanal.merge(sem_odio, on="semana", how="left").fillna({"odio": 0})
        sem_join["pct_odio"] = (
            sem_join["odio"] / sem_join["menciones"].replace(0, pd.NA)
        ) * 100.0
        fig_evol.add_trace(
            go.Scatter(
                x=sem_join["semana"],
                y=sem_join["pct_odio"],
                name="% ODIO",
                mode="lines+markers",
                yaxis="y2",
            )
        )
        fig_evol.update_layout(
            yaxis2=dict(title="% ODIO", overlaying="y", side="right", rangemode="tozero"),
        )

    fig_evol.update_layout(
        xaxis_title="Semana",
        yaxis_title="Menciones",
        height=400,
        legend=dict(orientation="h", y=-0.2),
        margin=dict(t=30, b=40),
    )
    st.plotly_chart(fig_evol, use_container_width=True)

    # 6) Comparativa por medio
    st.markdown("### Reacción de audiencia por medio")

    def _intens_media(s):
        nums = s.map(_INTENSIDAD_NUMERICA)
        return round(nums.mean(), 2) if nums.notna().any() else None

    por_medio = (
        df.groupby("source_media")
        .agg(
            total_menciones=("message_uuid", "count"),
            total_odio=(
                "clasificacion_principal",
                lambda s: int((s == "ODIO").sum()),
            ),
            intens_media=("intensidad_pred", _intens_media),
        )
        .reset_index()
        .sort_values("total_menciones", ascending=False)
        .head(20)
    )

    if solo_llm:
        por_medio["pct_odio"] = (
            por_medio["total_odio"] / por_medio["total_menciones"].replace(0, pd.NA)
        ) * 100.0
        fig_medios = px.bar(
            por_medio.sort_values("total_menciones"),
            x="total_menciones",
            y="source_media",
            orientation="h",
            color="pct_odio",
            color_continuous_scale="Reds",
            hover_data=["intens_media"],
            labels={
                "total_menciones": "Menciones",
                "pct_odio": "% ODIO",
                "source_media": "Medio",
                "intens_media": "Intensidad media",
            },
        )
    else:
        fig_medios = px.bar(
            por_medio.sort_values("total_menciones"),
            x="total_menciones",
            y="source_media",
            orientation="h",
            labels={
                "total_menciones": "Menciones",
                "source_media": "Medio",
            },
        )
    fig_medios.update_layout(height=520, margin=dict(l=160, t=20))
    _apply_horizontal_bar_labels(fig_medios)
    st.plotly_chart(fig_medios, use_container_width=True)

    # 7) Distribución de categorías por medio (solo si filtro IA/LLM activo)
    if solo_llm:
        st.markdown(_ui_label("### Categorías de odio por medio"))
        df_odio_cat = df[df["clasificacion_principal"] == "ODIO"].copy()
        if df_odio_cat.empty:
            st.info("No hay mensajes ODIO para este término.")
        else:
            cat_pivot = (
                df_odio_cat.groupby(["source_media", "categoria_odio_pred"])
                .size()
                .reset_index(name="total")
            )
            cat_pivot["categoria_label"] = cat_pivot["categoria_odio_pred"].map(
                lambda x: CATEGORIAS_LABELS.get(x, x or "—")
            )
            top_medios = (
                cat_pivot.groupby("source_media")["total"]
                .sum()
                .sort_values(ascending=False)
                .head(20)
                .index.tolist()
            )
            cat_pivot = cat_pivot[cat_pivot["source_media"].isin(top_medios)]
            fig_cat = px.bar(
                cat_pivot,
                x="source_media",
                y="total",
                color="categoria_label",
                barmode="stack",
                labels={
                    "total": "Mensajes ODIO",
                    "source_media": "Medio",
                    "categoria_label": "Categoría",
                },
            )
            fig_cat.update_layout(height=500, xaxis_tickangle=-30, margin=dict(t=20))
            st.plotly_chart(fig_cat, use_container_width=True)

    # 8) Muestra de mensajes
    st.markdown("### Muestra de mensajes")
    df_sample = (
        df.sort_values("created_at", ascending=False).head(50).reset_index(drop=True)
    )
    page_size = 10
    total_pages = max(1, (len(df_sample) + page_size - 1) // page_size)
    if "buscador_page_idx" not in st.session_state:
        st.session_state["buscador_page_idx"] = 0
    st.session_state["buscador_page_idx"] = max(
        0, min(int(st.session_state["buscador_page_idx"]), total_pages - 1)
    )
    page_idx = int(st.session_state["buscador_page_idx"])

    c_prev, c_info, c_next = st.columns([1, 3, 1])
    with c_prev:
        if st.button(
            "◀ Anterior",
            key="buscador_prev",
            disabled=page_idx <= 0,
        ):
            st.session_state["buscador_page_idx"] = max(0, page_idx - 1)
            st.rerun()
    with c_info:
        st.markdown(
            f"<div style='text-align:center;padding:0.35rem 0;color:#5c6b7a;'>"
            f"Página <b>{page_idx + 1}</b> / {total_pages} · "
            f"mostrando hasta {page_size} mensajes</div>",
            unsafe_allow_html=True,
        )
    with c_next:
        if st.button(
            "Siguiente ▶",
            key="buscador_next",
            disabled=page_idx >= total_pages - 1,
        ):
            st.session_state["buscador_page_idx"] = min(total_pages - 1, page_idx + 1)
            st.rerun()

    start = page_idx * page_size
    end = start + page_size
    for _, row in df_sample.iloc[start:end].iterrows():
        with st.container(border=True):
            plat = platform_label(str(row["platform"] or ""))
            medio_pub = _public_medio_label(row.get("source_media"))
            fecha = (
                pd.to_datetime(row["created_at"]).strftime("%d/%m/%Y %H:%M")
                if pd.notna(row["created_at"])
                else "—"
            )
            if medio_pub:
                st.markdown(
                    f"**{plat}** · Medio monitorizado: **{medio_pub}** · {fecha}"
                )
            else:
                st.markdown(f"**{plat}** · {fecha}")
            if solo_llm:
                clasif = (row.get("clasificacion_principal") or "—")
                raw_cat = row.get("categoria_odio_pred") or ""
                cat_label = CATEGORIAS_LABELS.get(raw_cat, raw_cat or "—")
                intens = (row.get("intensidad_pred") or "—")
                st.markdown(
                    f"**Clasificación:** `{clasif}` · **Categoría:** {cat_label} · "
                    f"**Intensidad:** `{intens}`"
                )
            st.text(row["content_clean"])

    # 9) Exportación CSV (todos los resultados filtrados)
    st.markdown("---")
    export_cols = ["platform", "source_media", "created_at", "content_clean"]
    if solo_llm:
        export_cols += [
            "clasificacion_principal",
            "categoria_odio_pred",
            "intensidad_pred",
        ]
    df_export = df[export_cols].copy()
    if "source_media" in df_export.columns:
        df_export["source_media"] = df_export["source_media"].apply(
            lambda x: _public_medio_label(x) or ""
        )
    df_export = df_export.rename(
        columns={"content_clean": "content_original_anon"}
    )
    safe_tag = re.sub(r"[^a-zA-Z0-9_-]+", "_", termino)[:40] or "busqueda"
    st.download_button(
        label=f"Descargar CSV — resultados de '{termino}'",
        data=df_to_csv_bytes(df_export),
        file_name=f"reto_buscador_{safe_tag}.csv",
        mime="text/csv",
        key="dl_csv_buscador_terminos",
    )
