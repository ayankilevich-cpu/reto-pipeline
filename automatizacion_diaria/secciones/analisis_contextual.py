"""Sección "Análisis contextual" del dashboard RETO."""
from __future__ import annotations

import html as _html
import json
import re
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from components.aviso_criterio import render_aviso_cambio_criterio
from components.db_helpers import _pooled_conn
from components.exports import render_descargas_menu
from components.ui import _render_section_header, _is_viewer
from components.chart_style import (
    INK, MUTED, NAVY, SUBTLE,
    INT_COLOR, INT_LABEL,
    encabezado_grafico, fmt_int, fmt_pct, kpi_tarjetas, nota_fuente,
)
from criterio_etiquetado import CRITERIO_CORTE_SEMANA, MIN_MENSAJES_SPIKE

try:
    from contexto_resumen_limpieza import (
        generar_eventos_desde_stats,
        generar_resumen_desde_stats,
        limpiar_eventos_relacionados,
        limpiar_resumen_contexto,
    )
except ImportError:
    generar_resumen_desde_stats = None
    generar_eventos_desde_stats = None
    limpiar_resumen_contexto = lambda t: t or ""
    limpiar_eventos_relacionados = lambda t: t or ""


# ── Constantes ────────────────────────────────────────────────
_MIN_MSGS_CHART = 100
_COLOR_NORMAL = "#8AA4BF"
_COLOR_ALERTA = "#A32D2D"
_COLOR_ACTUAL = "#D9E2EC"

_DIAS_ES = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]

_INT_LABEL_DETAIL = {
    "1": "Baja (1) · leve",
    "2": "Media (2) · ofensivo",
    "3": "Alta (3) · hostil",
}

CATEGORIAS_DISPLAY = {
    "odio_etnico_cultural_religioso": "Étnico / Cultural / Religioso",
    "odio_genero_identidad_orientacion": "Género / Identidad / Orientación",
    "odio_condicion_social_economica_salud": "Condición Social / Económica / Salud",
    "odio_ideologico_politico": "Ideológico / Político",
    "odio_personal_generacional": "Personal / Generacional",
    "odio_profesiones_roles_publicos": "Profesiones / Roles Públicos",
}


# ── Helpers ───────────────────────────────────────────────────

def _parse_json_col(val) -> dict:
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return {}


def _normalizar_semana(ts) -> Optional[date]:
    """Convierte cualquier timestamp a la fecha del lunes de su semana UTC."""
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    try:
        t = pd.Timestamp(ts)
        if t.tzinfo is not None:
            t = t.tz_convert("UTC").tz_localize(None)
        d = t.date()
        return d - timedelta(days=d.weekday())
    except Exception:
        return None


def _preparar_serie_semanal(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza semanas a lunes UTC, deduplication por analisis_date más reciente."""
    if df.empty:
        return df
    df = df.copy()
    df["semana_lunes"] = df["semana_inicio"].apply(_normalizar_semana)
    df = df.dropna(subset=["semana_lunes"])
    if "analisis_date" in df.columns:
        df["analisis_date"] = pd.to_datetime(df["analisis_date"], errors="coerce")
        df = df.sort_values("analisis_date", na_position="first")
    df = df.groupby("semana_lunes", as_index=False).last()
    df["semana_inicio"] = df["semana_lunes"].apply(lambda d: pd.Timestamp(d))
    df["semana_fin"] = df["semana_lunes"].apply(lambda d: pd.Timestamp(d + timedelta(days=6)))
    _INICIO = pd.Timestamp("2025-11-24")
    df = df[df["semana_inicio"] >= _INICIO].copy()
    return df.sort_values("semana_inicio").reset_index(drop=True)


def _es_alerta(row, spike_fallback: float) -> bool:
    try:
        if bool(row.get("es_spike")):
            return True
    except Exception:
        pass
    try:
        pct = float(row.get("pct_odio"))
        tot = int(row.get("total_mensajes"))
    except (TypeError, ValueError):
        return False
    if tot < _MIN_MSGS_CHART:
        return False
    um = row.get("umbral_spike_pct")
    if um is not None and not (isinstance(um, float) and pd.isna(um)):
        try:
            return pct >= float(um)
        except (TypeError, ValueError):
            pass
    return pct >= spike_fallback


def _fmt_semana(si, sf) -> str:
    """Formato 'Del dd/mm al dd/mm/aaaa'."""
    try:
        s = pd.Timestamp(si).strftime("%d/%m")
        e = pd.Timestamp(sf).strftime("%d/%m/%Y")
        return f"Del {s} al {e}"
    except Exception:
        return str(si)


def _fmt_dia_pico(val) -> str:
    """Convierte dia_pico a 'lunes dd/mm'."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return "—"
    try:
        t = pd.Timestamp(val)
        return f"{_DIAS_ES[t.weekday()]} {t.strftime('%d/%m')}"
    except Exception:
        return str(val)


def _formatear_resumen(texto: str) -> str:
    """Convierte fechas ISO y miles en formato inglés al estilo español."""
    if not texto:
        return texto
    texto = re.sub(
        r'\b(\d{4})-(\d{2})-(\d{2})\b',
        lambda m: f"{m.group(3)}/{m.group(2)}/{m.group(1)}",
        texto,
    )
    texto = re.sub(
        r'(?<!\d)(\d{1,3}),(\d{3})(?!\d)',
        lambda m: f"{m.group(1)}.{m.group(2)}",
        texto,
    )
    return texto


# ── Loader ────────────────────────────────────────────────────

@st.cache_data(ttl=86400, max_entries=64)
def load_analisis_semanal() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT *
            FROM processed.analisis_semanal
            ORDER BY semana_inicio
        """, conn)
    return df


# ── Helpers de stats ──────────────────────────────────────────

def _stats_desde_fila_analisis(row: pd.Series) -> Dict[str, Any]:
    cats = _parse_json_col(row.get("categorias"))
    stats: Dict[str, Any] = {
        "semana_inicio": row.get("semana_inicio"),
        "semana_fin": row.get("semana_fin"),
        "total_mensajes": int(row.get("total_mensajes") or 0),
        "total_odio": int(row.get("total_odio") or 0),
        "pct_odio": float(row.get("pct_odio") or 0),
        "es_spike": bool(row.get("es_spike")),
        "promedio_referencia_pct": float(row.get("promedio_referencia_pct") or 0),
        "umbral_spike_pct": float(row.get("umbral_spike_pct") or 0),
        "targets": _parse_json_col(row.get("targets")),
        "temas": _parse_json_col(row.get("temas")),
        "intensidad": _parse_json_col(row.get("intensidad")),
        "dia_pico": row.get("dia_pico"),
        "dia_pico_odio": int(row.get("dia_pico_odio") or 0),
        "categoria_lider": None,
        "categoria_lider_cnt": 0,
        "categoria_lider_pct": None,
    }
    if cats:
        lead_key, lead_cnt = max(cats.items(), key=lambda x: x[1])
        stats["categoria_lider"] = lead_key
        stats["categoria_lider_cnt"] = int(lead_cnt)
        if stats["total_odio"] > 0:
            stats["categoria_lider_pct"] = round(100.0 * int(lead_cnt) / stats["total_odio"], 1)
    return stats


def _resumen_contextual_para_ui(row: pd.Series) -> str:
    raw = str(row.get("resumen_contexto") or "")
    limpio = limpiar_resumen_contexto(raw)
    if len(limpio) >= 80:
        return _formatear_resumen(limpio)
    if generar_resumen_desde_stats is not None:
        return _formatear_resumen(generar_resumen_desde_stats(_stats_desde_fila_analisis(row)))
    return _formatear_resumen(limpio or raw)


def _eventos_relacionados_para_ui(row: pd.Series) -> str:
    raw = str(row.get("eventos_relacionados") or "")
    limpio = limpiar_eventos_relacionados(raw)
    if len(limpio) >= 40:
        return limpio
    if generar_eventos_desde_stats is not None:
        return generar_eventos_desde_stats(_stats_desde_fila_analisis(row))
    return limpio or raw


# ── Render principal ──────────────────────────────────────────

def render_analisis_contextual():
    # 2.1 Cabecera
    col_titulo, col_dl = st.columns([6, 1])
    with col_titulo:
        _render_section_header(
            "Análisis contextual semanal",
            "Evolución semanal del odio en X, con alertas, colectivos atacados y temas dominantes. "
            "Resumen generado automáticamente cada semana.",
        )

    # 2.2 Aviso y alcance
    render_aviso_cambio_criterio(con_umbral=True)
    st.caption("Esta sección analiza solo mensajes de X clasificados por IA.")

    # Cargar y preparar datos
    df_raw = load_analisis_semanal()
    if df_raw.empty:
        st.warning("No hay datos de análisis semanal.")
        return

    df = _preparar_serie_semanal(df_raw)
    if df.empty:
        st.warning("No hay semanas disponibles desde el inicio de medición (24/11/2025).")
        return

    for _col in ("promedio_referencia_pct", "umbral_spike_pct", "n_semanas_base"):
        if _col not in df.columns:
            df[_col] = pd.NA

    df_chart = df[df["total_mensajes"] >= _MIN_MSGS_CHART].copy()
    df_chart["semana_label"] = df_chart["semana_inicio"].apply(
        lambda d: d.strftime("%d/%m/%y") if hasattr(d, "strftime") else str(d)
    )

    # Semana en curso
    hoy = date.today()
    cal_ini = hoy - timedelta(days=hoy.weekday())
    mask_actual = df_chart["semana_lunes"] == cal_ini

    # Fallback threshold (para semanas sin umbral congelado)
    df_cerradas = df_chart[~mask_actual]
    avg_pct = float(df_cerradas["pct_odio"].mean()) if not df_cerradas.empty else float(df_chart["pct_odio"].mean()) if not df_chart.empty else 0.0
    spike_fallback = avg_pct * 1.5

    # 2.4 Gráfico de evolución
    with st.container(border=True):
        # Título dinámico basado en alertas de las últimas 4 semanas cerradas
        ultimas4 = df_cerradas.sort_values("semana_inicio").tail(4)
        n_alertas_4 = sum(1 for _, r in ultimas4.iterrows() if _es_alerta(r, spike_fallback))
        if n_alertas_4 > 0:
            titulo_ev = f"{n_alertas_4} de las últimas 4 semanas cerradas superaron el umbral de alerta"
        else:
            titulo_ev = "Ninguna de las últimas 4 semanas cerradas superó el umbral de alerta"

        encabezado_grafico(
            titulo_ev,
            "% de mensajes clasificados como odio cada semana. "
            "Una semana entra en alerta cuando supera 1,5 veces el promedio de las semanas anteriores.",
        )

        # Colores por barra
        bar_colors = []
        bar_customdata: List[List] = []
        for _, row in df_chart.iterrows():
            es_act = row["semana_lunes"] == cal_ini
            es_alerta_row = (not es_act) and _es_alerta(row, spike_fallback)
            if es_act:
                bar_colors.append(_COLOR_ACTUAL)
            elif es_alerta_row:
                bar_colors.append(_COLOR_ALERTA)
            else:
                bar_colors.append(_COLOR_NORMAL)
            _um = row.get("umbral_spike_pct")
            um_fmt = fmt_pct(float(_um), dec=2) if (_um is not None and not (isinstance(_um, float) and pd.isna(_um))) else "—"
            alerta_txt = "<br>En alerta" if es_alerta_row else ""
            bar_customdata.append([row["semana_label"], fmt_pct(row["pct_odio"], dec=2), um_fmt, alerta_txt])

        fig_ev = go.Figure()

        # Dummy traces para leyenda (deben ir antes del bar para que aparezcan primero)
        fig_ev.add_trace(go.Scatter(x=[None], y=[None], mode="markers", marker=dict(symbol="square", size=14, color=_COLOR_NORMAL), name="Semana sin alerta", showlegend=True))
        fig_ev.add_trace(go.Scatter(x=[None], y=[None], mode="markers", marker=dict(symbol="square", size=14, color=_COLOR_ALERTA), name="Semana en alerta", showlegend=True))
        fig_ev.add_trace(go.Scatter(x=[None], y=[None], mode="markers", marker=dict(symbol="square", size=14, color=_COLOR_ACTUAL), name="Semana en curso", showlegend=True))

        fig_ev.add_trace(go.Bar(
            x=df_chart["semana_label"],
            y=df_chart["pct_odio"],
            marker_color=bar_colors,
            marker_line=dict(color="#FFFFFF", width=0.5),
            showlegend=False,
            customdata=bar_customdata,
            hovertemplate="Semana del %{customdata[0]} · %{customdata[1]} de odio · umbral %{customdata[2]}%{customdata[3]}<extra></extra>",
        ))

        # Umbral escalonado en NAVY punteado (sin semanas con umbral=0)
        _umbral_vals = []
        for _, row in df_chart.iterrows():
            _um = row.get("umbral_spike_pct")
            if _um is not None and not (isinstance(_um, float) and pd.isna(_um)) and float(_um) > 0:
                _umbral_vals.append(float(_um))
            else:
                _umbral_vals.append(None)

        fig_ev.add_trace(go.Scatter(
            x=df_chart["semana_label"],
            y=_umbral_vals,
            mode="lines",
            line=dict(shape="hv", dash="dot", color=NAVY, width=1.6),
            name="Umbral de alerta",
            connectgaps=False,
            hovertemplate="Umbral: %{y:.2f} %<extra></extra>",
        ))

        # Línea vertical cambio de criterio
        _lunes_corte = CRITERIO_CORTE_SEMANA - timedelta(days=CRITERIO_CORTE_SEMANA.weekday())
        _mask_corte = df_chart["semana_lunes"] == _lunes_corte
        if _mask_corte.any():
            _x_corte = df_chart[_mask_corte].iloc[0]["semana_label"]
            fig_ev.add_vline(
                x=_x_corte,
                line=dict(dash="dot", color=MUTED, width=1.5),
                annotation_text="cambio de criterio X",
                annotation_position="top right",
                annotation_font=dict(size=11, color=MUTED),
            )

        fig_ev.update_layout(
            height=440,
            xaxis_title="",
            yaxis=dict(title="% de odio", tickformat=".1f", ticksuffix=" %"),
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="center", x=0.5),
            xaxis=dict(tickangle=-45, tickfont=dict(size=10), automargin=True),
            margin=dict(b=60, t=20),
            bargap=0.3,
        )
        st.plotly_chart(fig_ev, use_container_width=True, theme=None, key="ctx_timeline_v2")

        with st.expander("Cómo se calcula el umbral"):
            st.markdown(
                f"El umbral de cada semana se fija al cerrarla: 1,5 × el promedio de las semanas cerradas anteriores "
                f"con al menos {_MIN_MSGS_CHART} mensajes. Por eso la línea cambia de una semana a otra. "
                f"Desde el cambio de criterio de X se usa un umbral provisional."
            )

    # 2.5 Semana seleccionada
    df_selectable = df_chart[~mask_actual].copy()
    df_selectable = df_selectable.sort_values("semana_inicio", ascending=False)

    if df_selectable.empty:
        st.info("No hay semanas cerradas con suficientes datos.")
        row_sel = None
        fig_cat = fig_tgt = fig_tema = None
        df_semana_sel_csv = pd.DataFrame()
    else:
        with st.container(border=True):
            week_labels = [_fmt_semana(r["semana_inicio"], r["semana_fin"]) for _, r in df_selectable.iterrows()]
            week_starts = df_selectable["semana_inicio"].tolist()
            label_to_start = dict(zip(week_labels, week_starts))

            col_hdr_sem, col_sel = st.columns([2, 1])
            with col_sel:
                selected_label = st.selectbox("Semana", week_labels, index=0, key="ctx_week_sel_v2")

            selected_start = label_to_start.get(selected_label, week_starts[0])
            row_sel = df_selectable[df_selectable["semana_inicio"] == selected_start].iloc[0]

            _total_sel = int(row_sel.get("total_mensajes") or 0)
            _odio_sel = int(row_sel.get("total_odio") or 0)
            _pct_sel = float(row_sel.get("pct_odio") or 0)
            _es_alerta_sel = _es_alerta(row_sel, spike_fallback)
            _si_sel = pd.Timestamp(row_sel["semana_inicio"])
            _sf_sel = pd.Timestamp(row_sel["semana_fin"])

            with col_hdr_sem:
                _umbral_desc = "por encima del umbral (en alerta)" if _es_alerta_sel else "por debajo del umbral"
                _titulo_sem = (f"Semana del {_si_sel.day}/{_si_sel.month:02d} al "
                               f"{_sf_sel.day}/{_sf_sel.month:02d}/{_sf_sel.year}: "
                               f"{fmt_pct(_pct_sel, dec=2)} de odio, {_umbral_desc}")
                st.markdown(f"<p style='font-size:17px;font-weight:600;color:{INK};margin:0 0 2px'>{_html.escape(_titulo_sem)}</p>", unsafe_allow_html=True)
                st.caption("Detalle de la semana seleccionada.")

            # KPIs
            _um_sel = row_sel.get("umbral_spike_pct")
            _prom_sel = row_sel.get("promedio_referencia_pct")
            _alerta_color = "#A32D2D" if _es_alerta_sel else "#2F855A"
            _alerta_det = ""
            if _um_sel is not None and not (isinstance(_um_sel, float) and pd.isna(_um_sel)) and _prom_sel is not None and not (isinstance(_prom_sel, float) and pd.isna(_prom_sel)):
                _alerta_det = f"Umbral {fmt_pct(float(_um_sel), dec=2)} (1,5 × {fmt_pct(float(_prom_sel), dec=2)})"

            kpi_tarjetas([
                ("Mensajes analizados", fmt_int(_total_sel), ""),
                ("Mensajes de odio", fmt_int(_odio_sel), ""),
                ("% de odio", fmt_pct(_pct_sel, dec=2), ""),
                ("Alerta", "Sí" if _es_alerta_sel else "No", _alerta_det),
            ], value_colors=[None, None, None, _alerta_color])

            st.markdown("<div style='margin-top:16px'></div>", unsafe_allow_html=True)

            # 3:2 columnas
            col_res, col_cambios = st.columns([3, 2])

            with col_res:
                st.markdown(f"<p style='font-size:15px;font-weight:600;color:{INK};margin:0 0 8px'>Resumen de la semana</p>", unsafe_allow_html=True)
                resumen_txt = _resumen_contextual_para_ui(row_sel)
                if resumen_txt:
                    st.info(resumen_txt)
                else:
                    st.caption("Resumen no disponible.")

                if st.session_state.get("user_role") == "admin":
                    if st.button("Recargar resumen", key="ctx_reload_resumen", type="secondary"):
                        load_analisis_semanal.clear()
                        st.rerun()

            with col_cambios:
                st.markdown(f"<p style='font-size:15px;font-weight:600;color:{INK};margin:0 0 8px'>Cambios respecto a la semana anterior</p>", unsafe_allow_html=True)

                # Semana anterior
                _prev_lunes = row_sel["semana_lunes"] - timedelta(weeks=1)
                _prev_rows = df_chart[df_chart["semana_lunes"] == _prev_lunes]
                row_prev = _prev_rows.iloc[0] if not _prev_rows.empty else None

                targets_curr = _parse_json_col(row_sel.get("targets"))
                odio_curr = max(_odio_sel, 1)
                targets_prev = _parse_json_col(row_prev.get("targets")) if row_prev is not None else {}
                odio_prev = max(int(row_prev.get("total_odio") or 0), 1) if row_prev is not None else 1

                top10_curr = dict(sorted(targets_curr.items(), key=lambda x: x[1], reverse=True)[:10])
                cambios = []
                for name, cnt in top10_curr.items():
                    pct_c = cnt / odio_curr * 100
                    pct_p = targets_prev.get(name, 0) / odio_prev * 100
                    diff = pct_c - pct_p
                    cambios.append((name, diff))
                cambios.sort(key=lambda x: abs(x[1]), reverse=True)

                if cambios:
                    for name, diff in cambios[:8]:
                        if diff >= 0:
                            st.markdown(f"<div style='font-size:14px;margin:3px 0;color:#A32D2D'>▲ +{diff:.0f} pp {_html.escape(name)}</div>", unsafe_allow_html=True)
                        else:
                            st.markdown(f"<div style='font-size:14px;margin:3px 0;color:#2F855A'>▼ {diff:.0f} pp {_html.escape(name)}</div>", unsafe_allow_html=True)
                else:
                    st.caption("Sin datos de colectivos para comparar.")

                _dia_pico = row_sel.get("dia_pico")
                _odio_pico = int(row_sel.get("dia_pico_odio") or 0)
                if _dia_pico is not None and not (isinstance(_dia_pico, float) and pd.isna(_dia_pico)):
                    st.markdown(f"<p style='font-size:13px;color:{MUTED};margin-top:10px'>Día con más odio: {_fmt_dia_pico(_dia_pico)}, con {fmt_int(_odio_pico)} mensajes de odio.</p>", unsafe_allow_html=True)

            # Eventos relacionados
            ev_txt = _eventos_relacionados_para_ui(row_sel) if row_sel.get("eventos_relacionados") else ""
            if ev_txt:
                with st.expander("Eventos relacionados"):
                    st.markdown(ev_txt)

        # CSV de la semana seleccionada
        df_semana_sel_csv = pd.DataFrame([{
            "Semana": selected_label,
            "Mensajes": _total_sel,
            "Mensajes de odio": _odio_sel,
            "% de odio": fmt_pct(_pct_sel, dec=2),
            "Alerta": "Sí" if _es_alerta_sel else "No",
        }])
        fig_cat = fig_tgt = fig_tema = None

    # 2.6 Cuadrícula 2×2
    if row_sel is not None:
        _cats = _parse_json_col(row_sel.get("categorias"))
        _targets_grid = _parse_json_col(row_sel.get("targets"))
        _temas_grid = _parse_json_col(row_sel.get("temas"))
        _int_grid = _parse_json_col(row_sel.get("intensidad"))

        row1_col1, row1_col2 = st.columns(2)
        row2_col1, row2_col2 = st.columns(2)

        # Categorías
        with row1_col1:
            with st.container(border=True):
                if _cats:
                    cat_items = sorted(_cats.items(), key=lambda x: x[1], reverse=True)
                    lead_cat_key, lead_cat_cnt = cat_items[0]
                    lead_cat_label = CATEGORIAS_DISPLAY.get(lead_cat_key, lead_cat_key)
                    lead_cat_pct = round(100.0 * lead_cat_cnt / max(_odio_sel, 1), 1)
                    encabezado_grafico(
                        f"El odio de {lead_cat_label} fue el más frecuente ({fmt_pct(lead_cat_pct)})",
                        "Mensajes de odio de la semana por categoría.",
                    )
                    _cat_names = [CATEGORIAS_DISPLAY.get(k, k) for k, _ in cat_items]
                    _cat_vals = [v for _, v in cat_items]
                    _cat_colors = [NAVY] + ["#8AA4BF"] * (len(_cat_names) - 1)
                    fig_cat = go.Figure(go.Bar(
                        orientation="h", y=_cat_names, x=_cat_vals,
                        marker_color=_cat_colors,
                        text=[fmt_int(v) for v in _cat_vals],
                        textposition="outside", cliponaxis=False,
                        hovertemplate="%{y}: %{x}<extra></extra>",
                    ))
                    fig_cat.update_layout(height=max(260, len(_cat_names) * 42), showlegend=False, margin=dict(l=4, r=40, t=4, b=4), xaxis=dict(visible=False), yaxis=dict(autorange="reversed", showgrid=False, automargin=True))
                    st.plotly_chart(fig_cat, use_container_width=True, theme=None, key="ctx_cats_v2")
                else:
                    st.info("Sin datos de categorías.")

        # Colectivos atacados
        with row1_col2:
            with st.container(border=True):
                if _targets_grid:
                    top_tgt = dict(sorted(_targets_grid.items(), key=lambda x: x[1], reverse=True)[:10])
                    tgt_items = list(top_tgt.items())
                    lead_tgt, lead_tgt_cnt = tgt_items[0]
                    encabezado_grafico(
                        f"{lead_tgt}, el colectivo más atacado ({fmt_int(lead_tgt_cnt)} menciones)",
                        "Menciones en los motivos clasificados (un mensaje puede citar varios).",
                    )
                    _tgt_names = [k for k, _ in tgt_items]
                    _tgt_vals = [v for _, v in tgt_items]
                    _tgt_colors = [NAVY] + ["#8AA4BF"] * (len(_tgt_names) - 1)
                    fig_tgt = go.Figure(go.Bar(
                        orientation="h", y=_tgt_names, x=_tgt_vals,
                        marker_color=_tgt_colors,
                        text=[fmt_int(v) for v in _tgt_vals],
                        textposition="outside", cliponaxis=False,
                        hovertemplate="%{y}: %{x}<extra></extra>",
                    ))
                    fig_tgt.update_layout(height=max(300, len(_tgt_names) * 36), showlegend=False, margin=dict(l=4, r=40, t=4, b=4), xaxis=dict(visible=False), yaxis=dict(autorange="reversed", showgrid=False, automargin=True))
                    st.plotly_chart(fig_tgt, use_container_width=True, theme=None, key="ctx_targets_v2")
                else:
                    st.info("Sin datos de colectivos.")

        # Temas detectados
        with row2_col1:
            with st.container(border=True):
                if _temas_grid:
                    top_tem = dict(sorted(_temas_grid.items(), key=lambda x: x[1], reverse=True)[:10])
                    tem_items = list(top_tem.items())
                    lead_tem, _ = tem_items[0]
                    encabezado_grafico(
                        f"{lead_tem}, tema dominante en los mensajes de odio",
                        "Apariciones en los mensajes de odio de la semana.",
                    )
                    _tem_names = [k for k, _ in tem_items]
                    _tem_vals = [v for _, v in tem_items]
                    _tem_colors = [NAVY] + ["#8AA4BF"] * (len(_tem_names) - 1)
                    fig_tema = go.Figure(go.Bar(
                        orientation="h", y=_tem_names, x=_tem_vals,
                        marker_color=_tem_colors,
                        text=[fmt_int(v) for v in _tem_vals],
                        textposition="outside", cliponaxis=False,
                        hovertemplate="%{y}: %{x}<extra></extra>",
                    ))
                    fig_tema.update_layout(height=max(300, len(_tem_names) * 36), showlegend=False, margin=dict(l=4, r=40, t=4, b=4), xaxis=dict(visible=False), yaxis=dict(autorange="reversed", showgrid=False, automargin=True))
                    st.plotly_chart(fig_tema, use_container_width=True, theme=None, key="ctx_temas_v2")
                else:
                    st.info("Sin datos de temas.")

        # Intensidad
        with row2_col2:
            with st.container(border=True):
                _total_int = sum(int(_int_grid.get(k, 0)) for k in ["1", "2", "3"])
                if _total_int > 0:
                    _pct_alta = round(100.0 * int(_int_grid.get("3", 0)) / _total_int, 1)
                    encabezado_grafico(f"El {fmt_pct(_pct_alta)} del odio de la semana fue de intensidad alta")
                    _int_fig = go.Figure()
                    for k in ["3", "2", "1"]:
                        v = int(_int_grid.get(k, 0))
                        _int_fig.add_trace(go.Bar(
                            orientation="h", y=["Intensidad"], x=[100.0 * v / _total_int],
                            name=_INT_LABEL_DETAIL[k], marker_color=INT_COLOR[k],
                            hovertemplate=f"{_INT_LABEL_DETAIL[k]}: {fmt_int(v)} msgs ({round(100.0 * v / _total_int, 1):.1f}%)<extra></extra>",
                        ))
                    _int_fig.update_layout(barmode="stack", height=80, showlegend=False, margin=dict(l=0, r=0, t=0, b=0), xaxis=dict(range=[0, 100], visible=False), yaxis=dict(visible=False))
                    st.plotly_chart(_int_fig, use_container_width=True, theme=None, key="ctx_intensidad_v2")
                    for k in ["3", "2", "1"]:
                        v = int(_int_grid.get(k, 0))
                        pct_v = round(100.0 * v / _total_int, 1) if _total_int > 0 else 0.0
                        c = INT_COLOR[k]
                        lbl = _INT_LABEL_DETAIL[k]
                        st.markdown(f"<div style='display:flex;align-items:center;gap:8px;margin:4px 0'><span style='width:12px;height:12px;border-radius:2px;background:{c};flex:none;display:inline-block'></span><span style='font-size:14px'>{_html.escape(lbl)}</span><span style='margin-left:auto;font-size:14px;font-weight:600'>{fmt_pct(pct_v)} · {fmt_int(v)}</span></div>", unsafe_allow_html=True)
                else:
                    st.info("Sin datos de intensidad.")

    # 2.8 Tabla al final
    with st.expander("Ver la tabla de todas las semanas"):
        def _fmt_pct_cell(v) -> str:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return "—"
            try:
                return fmt_pct(float(v), dec=2)
            except (TypeError, ValueError):
                return "—"

        _tbl_df = df_chart.sort_values("semana_inicio").copy()
        _tbl_rows = []
        for _, r in _tbl_df.iterrows():
            _tbl_rows.append({
                "Semana": _fmt_semana(r["semana_inicio"], r["semana_fin"]),
                "Mensajes": fmt_int(r.get("total_mensajes")),
                "Mensajes de odio": fmt_int(r.get("total_odio")),
                "% de odio": _fmt_pct_cell(r.get("pct_odio")),
                "Umbral": _fmt_pct_cell(r.get("umbral_spike_pct")),
                "Alerta": "Sí" if _es_alerta(r, spike_fallback) else "No",
            })
        st.dataframe(pd.DataFrame(_tbl_rows), use_container_width=True, hide_index=True, key="ctx_tabla_semanas")

    # Descargas
    _df_serie_csv = df_chart[["semana_lunes", "total_mensajes", "total_odio", "pct_odio", "umbral_spike_pct", "es_spike"]].copy()
    _df_serie_csv = _df_serie_csv.rename(columns={"semana_lunes": "Semana (lunes)", "total_mensajes": "Mensajes", "total_odio": "Mensajes de odio", "pct_odio": "% de odio", "umbral_spike_pct": "Umbral", "es_spike": "Alerta"})

    with col_dl:
        render_descargas_menu(
            "analisis_contextual",
            "Análisis contextual semanal",
            csv_items=[
                ("serie_semanal", _df_serie_csv),
                ("semana_seleccionada", df_semana_sel_csv if row_sel is not None else pd.DataFrame()),
            ],
            fig_items=[{"title": "Evolución semanal % de odio", "fig": fig_ev, "kind": "plotly"}],
        )
