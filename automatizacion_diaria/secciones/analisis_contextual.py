"""Sección "Análisis contextual" del dashboard RETO."""
import sys
import json
from pathlib import Path
from datetime import date, datetime, timedelta
from typing import Any, Dict, Optional, Tuple

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from components.constants import COLORS
from components.db_helpers import _pooled_conn
from components.ui import (
    _render_section_header,
    _ui_label,
    _is_viewer,
    _apply_horizontal_bar_labels,
)
from components.exports import render_section_exports

try:
    from contexto_resumen_limpieza import (
        generar_eventos_desde_stats,
        generar_resumen_desde_stats,
        limpiar_eventos_relacionados,
        limpiar_resumen_contexto,
    )
except ImportError:  # pragma: no cover
    generar_resumen_desde_stats = None
    generar_eventos_desde_stats = None
    limpiar_resumen_contexto = lambda t: t or ""
    limpiar_eventos_relacionados = lambda t: t or ""


# ============================================================
# ANÁLISIS CONTEXTUAL SEMANAL
# ============================================================
@st.cache_data(ttl=3600)
def load_analisis_semanal() -> pd.DataFrame:
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT *
            FROM processed.analisis_semanal
            ORDER BY semana_inicio
        """, conn)
    return df


CATEGORIAS_DISPLAY = {
    "odio_etnico_cultural_religioso": "Étnico / Cultural / Religioso",
    "odio_genero_identidad_orientacion": "Género / Identidad / Orientación",
    "odio_condicion_social_economica_salud": "Condición Social / Económica / Salud",
    "odio_ideologico_politico": "Ideológico / Político",
    "odio_personal_generacional": "Personal / Generacional",
    "odio_profesiones_roles_publicos": "Profesiones / Roles Públicos",
}


def _parse_json_col(val) -> dict:
    if isinstance(val, dict):
        return val
    if isinstance(val, str):
        try:
            return json.loads(val)
        except Exception:
            return {}
    return {}


def _bounds_semana_cal_reto(d: date) -> Tuple[date, date]:
    """Lunes–domingo, alineado con `DATE_TRUNC('week', ...)` / `analisis_contexto_semanal.py`."""
    start = d - timedelta(days=d.weekday())
    return start, start + timedelta(days=6)


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
        return limpio
    if generar_resumen_desde_stats is not None:
        return generar_resumen_desde_stats(_stats_desde_fila_analisis(row))
    return limpio or raw


def _eventos_relacionados_para_ui(row: pd.Series) -> str:
    raw = str(row.get("eventos_relacionados") or "")
    limpio = limpiar_eventos_relacionados(raw)
    if len(limpio) >= 40:
        return limpio
    if generar_eventos_desde_stats is not None:
        return generar_eventos_desde_stats(_stats_desde_fila_analisis(row))
    return limpio or raw


def render_analisis_contextual():
    _render_section_header(
        "Análisis contextual semanal",
        _ui_label(
            "Evolución semanal del discurso de odio con <strong>alertas</strong>, "
            "<strong>targets</strong> y <strong>temas dominantes</strong>; análisis contextual con IA "
            "(solo mensajes de <strong>X</strong> clasificados por LLM)."
        ),
    )
    st.info(
        _ui_label(
            "📌 Esta sección analiza exclusivamente mensajes de **X (Twitter)** "
            "clasificados por el modelo **LLM**. Los datos de YouTube con "
            "clasificación LLM se visualizan en la sección **Categorías de odio (LLM)**."
        )
    )

    df = load_analisis_semanal()
    if df.empty:
        st.warning("No hay datos de análisis semanal. Ejecutá `analisis_contexto_semanal.py` para generar el histórico.")
        return

    FECHA_INICIO_MEDICION = pd.Timestamp("2025-11-24")
    df["semana_inicio"] = pd.to_datetime(df["semana_inicio"], errors="coerce")
    df["semana_fin"] = pd.to_datetime(df["semana_fin"], errors="coerce")
    df = df[df["semana_inicio"] >= FECHA_INICIO_MEDICION].copy()
    if df.empty:
        st.warning("No hay semanas disponibles desde el inicio de medición (24/11/2025).")
        return

    for _col in ("promedio_referencia_pct", "umbral_spike_pct", "n_semanas_base"):
        if _col not in df.columns:
            df[_col] = pd.NA

    MIN_MSGS_CHART = 100
    df_chart = df[df["total_mensajes"] >= MIN_MSGS_CHART].copy()

    df_chart["semana_label"] = df_chart["semana_inicio"].apply(
        lambda d: d.strftime("%d/%m/%y") if hasattr(d, "strftime") else str(d)
    )

    def _semana_incluye_hoy(si, sf, hoy: date) -> bool:
        if si is None or sf is None or pd.isna(si) or pd.isna(sf):
            return False
        if hasattr(si, "date"):
            si = si.date()
        if hasattr(sf, "date"):
            sf = sf.date()
        try:
            return si <= hoy <= sf
        except TypeError:
            return False

    def _as_date_only_ctx(val) -> Optional[date]:
        if val is None or pd.isna(val):
            return None
        if isinstance(val, datetime):
            return val.date()
        if type(val) is date:
            return val
        return pd.Timestamp(val).date()

    def _fila_es_lunes_semana_cal_actual(si, hoy_ref: date) -> bool:
        """Misma semana calendario que hoy aunque semana_fin en BD no cubra hoy (TZ/legacy)."""
        d = _as_date_only_ctx(si)
        if d is None:
            return False
        cal_ini, _ = _bounds_semana_cal_reto(hoy_ref)
        return d == cal_ini

    def _es_semana_en_curso(si, sf, hoy_ref: date) -> bool:
        return _semana_incluye_hoy(si, sf, hoy_ref) or _fila_es_lunes_semana_cal_actual(
            si, hoy_ref,
        )

    hoy = date.today()

    # --- Timeline ---
    st.subheader("Evolución semanal del % de odio")

    # Promedio y umbral solo sobre semanas cerradas (la semana en curso es parcial y no debería mover la línea base).
    mask_cerrada = ~df_chart.apply(
        lambda r: _es_semana_en_curso(r["semana_inicio"], r["semana_fin"], hoy),
        axis=1,
    )
    df_cerradas = df_chart[mask_cerrada]
    if not df_cerradas.empty:
        avg_pct = float(df_cerradas["pct_odio"].mean())
    else:
        avg_pct = float(df_chart["pct_odio"].mean()) if not df_chart.empty else 0
    spike_threshold = avg_pct * 1.5

    def _alerta_spike_segun_cierre(row) -> bool:
        """Rojo si es_spike en BD, o si pct_odio > umbral (congelado o dinámico) y volumen >= MIN_MSGS_CHART."""
        try:
            if bool(row.get("es_spike")):
                return True
        except Exception:
            pass
        pct_val = row.get("pct_odio")
        tot_val = row.get("total_mensajes")
        try:
            pct = float(pct_val)
            tot = int(tot_val)
        except (TypeError, ValueError):
            return False
        if tot < MIN_MSGS_CHART:
            return False
        um = row.get("umbral_spike_pct")
        if um is not None and not (isinstance(um, float) and pd.isna(um)):
            try:
                return pct >= float(um)
            except (TypeError, ValueError):
                pass
        return pct >= spike_threshold

    st.markdown(
        f"Las **líneas horizontales** muestran el **contexto vigente** al cargar la página: promedio "
        f"**{avg_pct:.1f}%** y umbral **{spike_threshold:.1f}%** (= 1,5 × ese promedio) sobre **semanas ya cerradas** "
        f"del gráfico (≥**{MIN_MSGS_CHART}** mensajes cada una, **sin** la semana en curso). "
        "Ese umbral **cambia** si el histórico evoluciona (p. ej. un promedio de 3,6% implica umbral 5,4%). "
        "Las **barras rojas** siguen el criterio **congelado al cierre** guardado en la base de datos. "
        "La **tabla** bajo el gráfico resume **% de odio** y **alerta** por semana; si el pipeline archiva "
        "umbral y promedio de referencia (análisis posteriores a esa mejora), esas columnas **aparecen solas** en la tabla."
    )

    fecha_ini_med = (
        FECHA_INICIO_MEDICION.date()
        if hasattr(FECHA_INICIO_MEDICION, "date")
        else pd.Timestamp(FECHA_INICIO_MEDICION).date()
    )
    cal_ini, cal_fin = _bounds_semana_cal_reto(hoy)
    cubre_hoy_bd = df.apply(
        lambda r: _es_semana_en_curso(r["semana_inicio"], r["semana_fin"], hoy),
        axis=1,
    ).any()
    cubre_hoy_grafico = df_chart.apply(
        lambda r: _es_semana_en_curso(r["semana_inicio"], r["semana_fin"], hoy),
        axis=1,
    ).any()
    gap_sin_barra = cal_ini >= fecha_ini_med and not cubre_hoy_bd

    if gap_sin_barra:
        ultimo_fin = df["semana_fin"].max()
        if pd.isna(ultimo_fin):
            ultimo_fin_s = "—"
        else:
            ultimo_fin_s = pd.Timestamp(ultimo_fin).strftime("%d/%m/%Y")
        st.warning(
            f"**No hay fila en la base para la semana en curso** "
            f"({cal_ini.strftime('%d/%m/%Y')}–{cal_fin.strftime('%d/%m/%Y')}). "
            f"La última semana en `analisis_semanal` termina el **{ultimo_fin_s}**; **ningún registro incluye hoy**, "
            f"así que **no puede pintarse la barra amarilla** ni el cierre con {_ui_label('LLM')} de esta semana. "
            f"Ejecutá **`analisis_contexto_semanal.py`** (automatización diaria) para generar la semana actual y, "
            f"si aplica, cerrar la anterior con alerta y resumen."
        )
    elif cal_ini >= fecha_ini_med and cubre_hoy_bd and not cubre_hoy_grafico:
        st.info(
            f"La semana en curso ({cal_ini.strftime('%d/%m/%Y')}–{cal_fin.strftime('%d/%m/%Y')}) tiene "
            f"menos de **{MIN_MSGS_CHART}** mensajes; este gráfico solo muestra semanas con ese mínimo, "
            "así que **no verás barra amarilla** aquí hasta que haya volumen suficiente."
        )

    colors = []
    text_labels = []
    for _, row in df_chart.iterrows():
        es_actual = _es_semana_en_curso(
            row["semana_inicio"], row["semana_fin"], hoy,
        )
        if es_actual:
            colors.append(COLORS["current_week"])
        elif _alerta_spike_segun_cierre(row):
            colors.append(COLORS["danger"])
        else:
            colors.append(COLORS["accent"])
        show_pct = (
            es_actual or _alerta_spike_segun_cierre(row) or row["pct_odio"] >= avg_pct
        )
        text_labels.append(f"{row['pct_odio']}%" if show_pct else "")

    fig_timeline = go.Figure()
    fig_timeline.add_trace(go.Bar(
        x=df_chart["semana_label"],
        y=df_chart["pct_odio"],
        marker_color=colors,
        text=text_labels,
        textposition="outside",
        textfont=dict(size=11),
        hovertemplate=(
            "<b>Semana %{x}</b><br>"
            "% Odio: %{y:.1f}%<br>"
            "Total: %{customdata[0]:,} mensajes<br>"
            "Odio: %{customdata[1]:,} mensajes<extra></extra>"
        ),
        customdata=df_chart[["total_mensajes", "total_odio"]].values,
    ))
    fig_timeline.add_hline(
        y=avg_pct, line_dash="dash", line_color=COLORS["muted"],
        annotation_text=f"Promedio: {avg_pct:.1f}%",
        annotation_position="top left",
    )
    fig_timeline.add_hline(
        y=spike_threshold, line_dash="dot", line_color=COLORS["danger"],
        annotation_text=f"Umbral alerta: >={spike_threshold:.1f}%",
        annotation_position="top left",
        annotation=dict(
            font=dict(size=11, color=COLORS["danger"]),
            bgcolor="white",
            borderpad=3,
            yshift=8,
        ),
    )
    fig_timeline.update_layout(
        height=420,
        xaxis_title="",
        yaxis_title="% Odio",
        showlegend=False,
        xaxis=dict(tickangle=-45, tickfont=dict(size=10)),
        margin=dict(b=80),
    )
    if gap_sin_barra:
        fig_timeline.add_annotation(
            xref="paper",
            yref="paper",
            x=0.98,
            y=0.97,
            xanchor="right",
            yanchor="top",
            text="Semana en curso — datos pendientes de cierre",
            showarrow=False,
            bgcolor="rgba(254, 249, 195, 0.95)",
            bordercolor=COLORS["current_week"],
            borderwidth=1,
            font=dict(size=11, color="#1a1a1a"),
        )
    st.plotly_chart(fig_timeline, use_container_width=True, theme=None, key="ctx_timeline")

    with st.expander("ℹ️ Cómo leer este gráfico"):
        st.markdown(
            f"""
- 🔴 **Rojo**: semana con alerta (% odio ≥ umbral y ≥300 mensajes)
- 🔵 **Azul**: semana normal
- 🟡 **Amarillo**: semana en curso (parcial)
- **Líneas**: promedio ({avg_pct:.1f}%) y umbral de alerta ({spike_threshold:.1f}%) vigentes al cargar la página
- Solo se muestran semanas con ≥{MIN_MSGS_CHART} mensajes
            """
        )

    _tbl = df_chart.sort_values("semana_inicio").copy()
    _tiene_umbral_archivado = bool(
        _tbl["umbral_spike_pct"].notna().any()
        or _tbl["promedio_referencia_pct"].notna().any()
    )

    st.subheader(
        "Umbral y referencia por semana (congelados al cierre)"
        if _tiene_umbral_archivado
        else "Resumen por semana"
    )

    def _fmt_pct_cell(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        try:
            return f"{float(v):.1f}%"
        except (TypeError, ValueError):
            return "—"

    def _fmt_n_base(v) -> str:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return "—"
        try:
            return str(int(v))
        except (TypeError, ValueError):
            return "—"

    _tab_cols: Dict[str, Any] = {
        "Semana": _tbl["semana_label"],
        "% odio": _tbl["pct_odio"].map(lambda x: f"{x}%"),
    }
    if _tiene_umbral_archivado:
        _tab_cols["Promedio ref. (al cierre)"] = _tbl["promedio_referencia_pct"].map(_fmt_pct_cell)
        _tab_cols["Umbral 1,5× (al cierre)"] = _tbl["umbral_spike_pct"].map(_fmt_pct_cell)
        _tab_cols["Semanas en base"] = _tbl["n_semanas_base"].map(_fmt_n_base)
    _tab_cols["Alerta"] = _tbl.apply(
        lambda r: "Sí" if _alerta_spike_segun_cierre(r) else "No",
        axis=1,
    )

    def _estilo_alerta(val):
        if val == "Sí":
            return "background-color: #FEE2E2; color: #991B1B; font-weight: 600"
        return "color: #6B7280"

    st.dataframe(
        pd.DataFrame(_tab_cols).style.map(_estilo_alerta, subset=["Alerta"]),
        use_container_width=True,
        hide_index=True,
        key="ctx_umbral_por_semana",
    )

    st.markdown("---")

    # --- Week selector ---
    st.subheader("Detalle semanal")

    @st.fragment
    def _render_detalle_semanal():

        df_selectable = df[df["total_mensajes"] >= MIN_MSGS_CHART].copy()
        inicio_semana_actual = hoy - timedelta(days=hoy.weekday())
        df_selectable = df_selectable[
            df_selectable["semana_inicio"] < pd.Timestamp(inicio_semana_actual)
        ]
        if df_selectable.empty:
            st.info("No hay semanas con suficientes datos para mostrar detalle.")
            return

        week_options = []
        for _, row in df_selectable.sort_values("semana_inicio", ascending=False).iterrows():
            spike_mark = " ⚠️ ALERTA" if _alerta_spike_segun_cierre(row) else ""
            label = (
                f"{row['semana_inicio'].strftime('%d/%m/%Y')} — "
                f"{row['semana_fin'].strftime('%d/%m/%Y')}"
                f" | {int(row['total_mensajes']):,} msgs"
                f" | {row['pct_odio']}% odio{spike_mark}"
            )
            week_options.append((label, row["semana_inicio"]))

        selected_label = st.selectbox(
            "Seleccionar semana",
            [w[0] for w in week_options],
            index=0,
            key="ctx_week_sel",
        )
        selected_start = dict(week_options)[selected_label]
        row = df[df["semana_inicio"] == selected_start].iloc[0]

        # --- KPIs ---
        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Total mensajes", f"{int(row['total_mensajes']):,}")
        k2.metric("Mensajes de odio", f"{int(row['total_odio']):,}")
        k3.metric("% Odio", f"{row['pct_odio']}%")
        alerta_label = "Sí ⚠️" if bool(row.get("es_spike")) else "No"
        k4.metric("Alerta", alerta_label)

        st.markdown("---")

        # --- Context summary ---
        if row.get("resumen_contexto"):
            st.subheader("Resumen contextual")
            analisis_dt = row.get("analisis_date")
            if analisis_dt is not None and not (isinstance(analisis_dt, float) and pd.isna(analisis_dt)):
                analisis_s = pd.Timestamp(analisis_dt).strftime("%d/%m/%Y %H:%M")
            else:
                analisis_s = "—"
            st.caption(
                f"Texto generado por el pipeline semanal y guardado en la base de datos "
                f"(última actualización: **{analisis_s}**). "
                "Si regeneraste el análisis y no ves cambios, usá **Recargar resumen**."
            )
            rc1, _ = st.columns([1, 4])
            with rc1:
                if st.button("Recargar resumen", key="ctx_reload_resumen"):
                    load_analisis_semanal.clear()
                    st.rerun()
            st.info(_resumen_contextual_para_ui(row))

        ev_txt = _eventos_relacionados_para_ui(row) if row.get("eventos_relacionados") else ""
        if ev_txt:
            st.subheader("Eventos relacionados")
            st.markdown(ev_txt)

        st.markdown("---")

        # --- Categories & Targets side by side ---
        col_cat, col_tgt = st.columns(2)

        with col_cat:
            st.subheader("Categorías de odio (resumen de la semana)")
            cats = _parse_json_col(row.get("categorias"))
            if cats:
                cat_df = pd.DataFrame([
                    {"Categoría": CATEGORIAS_DISPLAY.get(k, k), "Mensajes": v}
                    for k, v in cats.items()
                ]).sort_values("Mensajes", ascending=False)
                fig_cat = px.bar(
                    cat_df, x="Mensajes", y="Categoría", orientation="h",
                    color="Mensajes", color_continuous_scale="Reds",
                )
                fig_cat.update_layout(height=300, showlegend=False, yaxis=dict(autorange="reversed"))
                _apply_horizontal_bar_labels(fig_cat)
                st.plotly_chart(fig_cat, use_container_width=True, theme=None, key="ctx_cats")
            else:
                st.info("Sin datos de categorías.")
            if not _is_viewer():
                st.caption(
                    "Este bloque resume la **semana** elegida. Para ver **texto de mensajes** "
                    "anonimizados clasificados por el LLM (muestra aleatoria), usá el menú lateral "
                    "**Categorías de odio (LLM)**."
                )

        with col_tgt:
            st.subheader("Colectivos atacados")
            targets = _parse_json_col(row.get("targets"))
            if targets:
                top_targets = dict(list(targets.items())[:10])
                tgt_df = pd.DataFrame([
                    {"Target": k, "Menciones": v}
                    for k, v in top_targets.items()
                ]).sort_values("Menciones", ascending=False)
                fig_tgt = px.bar(
                    tgt_df, x="Menciones", y="Target", orientation="h",
                    color="Menciones", color_continuous_scale="Oranges",
                )
                fig_tgt.update_layout(height=300, showlegend=False, yaxis=dict(autorange="reversed"))
                _apply_horizontal_bar_labels(fig_tgt)
                st.plotly_chart(fig_tgt, use_container_width=True, theme=None, key="ctx_targets")
            else:
                st.info("Sin datos de targets.")

        st.markdown("---")

        # --- Topics & Intensity ---
        col_tem, col_int = st.columns(2)

        with col_tem:
            st.subheader("Temas detectados")
            temas = _parse_json_col(row.get("temas"))
            if temas:
                top_temas = dict(list(temas.items())[:10])
                tema_df = pd.DataFrame([
                    {"Tema": k, "Menciones": v}
                    for k, v in top_temas.items()
                ]).sort_values("Menciones", ascending=False)
                fig_tema = px.bar(
                    tema_df, x="Menciones", y="Tema", orientation="h",
                    color="Menciones", color_continuous_scale="Blues",
                )
                fig_tema.update_layout(height=300, showlegend=False, yaxis=dict(autorange="reversed"))
                _apply_horizontal_bar_labels(fig_tema)
                st.plotly_chart(fig_tema, use_container_width=True, theme=None, key="ctx_temas")
            else:
                st.info("Sin datos de temas.")

        with col_int:
            st.subheader("Intensidad del odio")
            intensidad = _parse_json_col(row.get("intensidad"))
            if intensidad:
                int_labels = {"1": "Leve (ironía, burla)", "2": "Ofensivo (insultos)", "3": "Hostil (incitación)"}
                int_df = pd.DataFrame([
                    {"Nivel": int_labels.get(k, k), "Mensajes": v}
                    for k, v in intensidad.items() if v > 0
                ])
                if not int_df.empty:
                    fig_int = px.pie(
                        int_df, names="Nivel", values="Mensajes",
                        color="Nivel",
                        color_discrete_map={
                            "Leve (ironía, burla)": "#F4D03F",
                            "Ofensivo (insultos)": "#E67E22",
                            "Hostil (incitación)": "#C0392B",
                        },
                    )
                    fig_int.update_traces(
                        textinfo="percent+label",
                        textfont_size=12,
                        marker=dict(line=dict(color="#FFFFFF", width=2)),
                    )
                    fig_int.update_layout(height=300)
                    st.plotly_chart(fig_int, use_container_width=True, theme=None, key="ctx_intensidad")
                else:
                    st.info("Sin datos de intensidad.")
            else:
                st.info("Sin datos de intensidad.")

        if _is_viewer():
            st.caption("📅 Monitorización activa: lunes y jueves.")

        # --- Peak day ---
        if row.get("dia_pico"):
            st.markdown("---")
            st.caption(
                f"📅 **Día pico de la semana**: {row['dia_pico']} — "
                f"{int(row['dia_pico_odio'])} mensajes de odio ({row['dia_pico_pct']}%)"
            )

        csv_items = [
            ("semanal_historico", df),
        ]
        if "cat_df" in locals() and isinstance(cat_df, pd.DataFrame):
            csv_items.append(("detalle_categorias", cat_df))
        if "tgt_df" in locals() and isinstance(tgt_df, pd.DataFrame):
            csv_items.append(("detalle_targets", tgt_df))
        if "tema_df" in locals() and isinstance(tema_df, pd.DataFrame):
            csv_items.append(("detalle_temas", tema_df))
        if "int_df" in locals() and isinstance(int_df, pd.DataFrame):
            csv_items.append(("detalle_intensidad", int_df))

        render_section_exports(
            section_key="analisis_contextual",
            section_title="Análisis contextual semanal",
            csv_items=csv_items,
            fig_items=[
                {"title": "Evolución semanal % odio", "fig": fig_timeline, "kind": "plotly"},
                {"title": "Categorías de odio", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
                {"title": "Colectivos atacados", "fig": fig_tgt if "fig_tgt" in locals() else None, "kind": "plotly"},
                {"title": "Temas detectados", "fig": fig_tema if "fig_tema" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad del odio", "fig": fig_int if "fig_int" in locals() else None, "kind": "plotly"},
            ],
        )

    _render_detalle_semanal()
