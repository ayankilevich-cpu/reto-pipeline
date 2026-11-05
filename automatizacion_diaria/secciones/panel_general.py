"""Sección «Panel general» del dashboard (KPIs + estado del pipeline)."""
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

from components.constants import (
    CATEGORIAS_LABELS,
    CAT_COLOR_MAP,
    EXCLUDED_SOURCE_MEDIA,
    INTENSITY_COLORS,
    PLATFORM_DISPLAY,
    SEMANTIC_COLORS,
    _expand_platforms,
    platform_label,
)
from components.ui import (
    _apply_horizontal_bar_labels,
    _render_pg_kpi_grid,
    _render_section_header,
    _role_can_access_raw,
    _ui_label,
)
from components.exports import render_section_exports
from components.aviso_criterio import render_aviso_cambio_criterio
from components.db_helpers import load_filter_options, _pooled_conn


@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def load_kpis(
    access_raw: bool,
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> dict:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None

    with _pooled_conn() as conn:
        cur = conn.cursor()

        conds_p, params_p = [], []
        if platforms:
            conds_p.append("platform IN %s"); params_p.append(tuple(platforms))
        if medios:
            conds_p.append("source_media IN %s"); params_p.append(tuple(medios))
        wp = f"WHERE {' AND '.join(conds_p)}" if conds_p else ""
        wpc = f"WHERE is_candidate = TRUE" + (f" AND {' AND '.join(conds_p)}" if conds_p else "")

        if access_raw:
            conds_r, params_r = [], []
            if platforms:
                conds_r.append("platform IN %s")
                params_r.append(tuple(platforms))
            wr = f"WHERE {' AND '.join(conds_r)}" if conds_r else ""
            cur.execute(f"SELECT count(*) FROM raw.mensajes {wr}", params_r)
            total_raw = cur.fetchone()[0]
        else:
            cur.execute(f"SELECT count(*) FROM processed.mensajes {wp}", params_p)
            total_raw = cur.fetchone()[0]

        cur.execute(f"SELECT count(*) FROM processed.mensajes {wpc}", params_p)
        total_candidatos = cur.fetchone()[0]

        # scores
        q_scores = """
            SELECT count(*) FILTER (WHERE s.pred_odio = 1), AVG(s.proba_odio)
            FROM processed.scores s
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_s, params_s = [], []
        if platforms:
            conds_s.append("pm.platform IN %s"); params_s.append(tuple(platforms))
        if medios:
            conds_s.append("pm.source_media IN %s"); params_s.append(tuple(medios))
        ws = f"WHERE {' AND '.join(conds_s)}" if conds_s else ""
        cur.execute(f"{q_scores} {ws}", params_s)
        row = cur.fetchone()
        total_odio_baseline = row[0] or 0
        score_promedio = row[1] or 0

        # etiquetas_llm
        q_llm = """
            SELECT count(DISTINCT e.message_uuid),
                   count(DISTINCT e.message_uuid) FILTER (WHERE e.clasificacion_principal = 'ODIO')
            FROM processed.etiquetas_llm e
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_l, params_l = [], []
        if platforms:
            conds_l.append("pm.platform IN %s"); params_l.append(tuple(platforms))
        if medios:
            conds_l.append("pm.source_media IN %s"); params_l.append(tuple(medios))
        wl = f"WHERE {' AND '.join(conds_l)}" if conds_l else ""
        cur.execute(f"{q_llm} {wl}", params_l)
        row2 = cur.fetchone()
        total_etiquetados_llm = row2[0] or 0
        total_odio_llm = row2[1] or 0

        # medios count (solo medios reales con >= 100 mensajes)
        _excl_params = [tuple(EXCLUDED_SOURCE_MEDIA)]
        _excl_cond = " AND source_media NOT IN %s"
        cur.execute(
            "SELECT count(*) FROM ("
            "  SELECT source_media FROM processed.mensajes "
            "  WHERE source_media IS NOT NULL AND source_media != ''"
            + _excl_cond
            + (f" AND platform IN %s" if platforms else "")
            + "  GROUP BY source_media HAVING COUNT(*) >= 100"
            ") sub",
            _excl_params + ([tuple(platforms)] if platforms else []),
        )
        total_medios = cur.fetchone()[0]

        # gold validados (odio confirmado por humano)
        q_gold = """
            SELECT count(*),
                   count(*) FILTER (WHERE g.y_odio_bin = 1)
            FROM processed.gold_dataset g
            JOIN processed.mensajes pm USING (message_uuid)
        """
        conds_g, params_g = [], []
        if platforms:
            conds_g.append("pm.platform IN %s"); params_g.append(tuple(platforms))
        if medios:
            conds_g.append("pm.source_media IN %s"); params_g.append(tuple(medios))
        wg = f"WHERE {' AND '.join(conds_g)}" if conds_g else ""
        cur.execute(f"{q_gold} {wg}", params_g)
        row_g = cur.fetchone()
        total_gold = row_g[0] or 0
        total_gold_odio = row_g[1] or 0

        # Registros nuevos hoy
        tbl = "raw.mensajes" if access_raw else "processed.mensajes"
        q_hoy = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE"
        q_hoy_f = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE AND platform IN %s"
        if platforms:
            cur.execute(q_hoy_f, [tuple(platforms)])
        else:
            cur.execute(q_hoy)
        row_hoy = cur.fetchone()
        nuevos_x_hoy = row_hoy[0] or 0
        nuevos_yt_hoy = row_hoy[1] or 0

        # Registros nuevos ayer (CURRENT_DATE - 1)
        q_ayer = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE - 1"
        q_ayer_f = f"SELECT count(*) FILTER (WHERE platform IN ('x', 'twitter')), count(*) FILTER (WHERE platform = 'youtube') FROM {tbl} WHERE ingested_at::date = CURRENT_DATE - 1 AND platform IN %s"
        if platforms:
            cur.execute(q_ayer_f, [tuple(platforms)])
        else:
            cur.execute(q_ayer)
        row_ayer = cur.fetchone()
        nuevos_x_ayer = row_ayer[0] or 0
        nuevos_yt_ayer = row_ayer[1] or 0

        cur.close()

    return {
        "total_raw": total_raw,
        "total_candidatos": total_candidatos,
        "total_odio_baseline": total_odio_baseline,
        "total_odio_llm": total_odio_llm,
        "total_etiquetados_llm": total_etiquetados_llm,
        "score_promedio": score_promedio,
        "total_medios": total_medios,
        "total_gold": total_gold,
        "total_gold_odio": total_gold_odio,
        "nuevos_x_hoy": nuevos_x_hoy,
        "nuevos_yt_hoy": nuevos_yt_hoy,
        "nuevos_x_ayer": nuevos_x_ayer,
        "nuevos_yt_ayer": nuevos_yt_ayer,
    }


# ============================================================
# EMBUDO DE DETECCIÓN
# ============================================================
_COLOR_ETAPA = "#1B3A6B"
_COLOR_ODIO_CONF = "#A32D2D"
_COLOR_ODIO_IA = "#F09595"
_COLOR_VALIDADOS = "#2F855A"
_MIN_MUESTRA_PRECISION = 100

# Etiqueta LLM más reciente por mensaje (blindaje ante múltiples llm_version)
_SQL_LLM_ULTIMA = """
    SELECT DISTINCT ON (message_uuid)
           message_uuid, clasificacion_principal, categoria_odio_pred, intensidad_pred
    FROM processed.etiquetas_llm
    ORDER BY message_uuid, etiquetado_date DESC NULLS LAST
"""


def _fmt_int(n) -> str:
    return f"{int(n):,}".replace(",", ".")


def _fmt_pct_es(v: float) -> str:
    return f"{v:.1f}%".replace(".", ",")


@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def load_embudo(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> pd.DataFrame:
    """Embudo por plataforma sobre processed.mensajes (misma base para todos los roles)."""
    platforms_l = _expand_platforms(list(platforms) if platforms else None)
    medios_l = list(medios) if medios else None
    conds, params = [], []
    if platforms_l:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms_l))
    if medios_l:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios_l))
    where = f"WHERE {' AND '.join(conds)}" if conds else ""

    with _pooled_conn() as conn:
        df = pd.read_sql(f"""
            WITH e AS ({_SQL_LLM_ULTIMA})
            SELECT
                pm.platform,
                COUNT(*)                                                  AS procesados,
                COUNT(*) FILTER (WHERE pm.is_candidate)                   AS candidatos,
                COUNT(*) FILTER (WHERE g.message_uuid IS NOT NULL
                                    OR e.message_uuid IS NOT NULL)        AS clasificados,
                COUNT(*) FILTER (WHERE g.y_odio_final = 'Odio')           AS odio_confirmado,
                COUNT(*) FILTER (WHERE g.message_uuid IS NULL
                                   AND e.clasificacion_principal = 'ODIO') AS odio_ia,
                COUNT(*) FILTER (WHERE g.message_uuid IS NOT NULL)        AS validados,
                COUNT(*) FILTER (WHERE e.clasificacion_principal = 'ODIO'
                                   AND g.y_odio_final IS NOT NULL)        AS ia_odio_revisados,
                COUNT(*) FILTER (WHERE e.clasificacion_principal = 'ODIO'
                                   AND g.y_odio_final = 'Odio')           AS ia_odio_confirmados
            FROM processed.mensajes pm
            LEFT JOIN processed.gold_dataset g USING (message_uuid)
            LEFT JOIN e USING (message_uuid)
            {where}
            GROUP BY pm.platform
        """, conn, params=params)

    if not df.empty:
        df["plataforma"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
    return df


def _render_embudo(df_emb: pd.DataFrame):
    cols = ["procesados", "candidatos", "clasificados", "odio_confirmado", "odio_ia",
            "validados", "ia_odio_revisados", "ia_odio_confirmados"]
    t = {c: int(df_emb[c].sum()) for c in cols}
    base = t["procesados"] or 1
    odio_total = t["odio_confirmado"] + t["odio_ia"]

    etapas = [
        "Mensajes procesados",
        "Candidatos (filtro de términos)",
        "Clasificados (IA + revisión humana)",
        "Odio identificado",
        "Validados por revisión humana",
    ]
    valores = [t["procesados"], t["candidatos"], t["clasificados"], odio_total, t["validados"]]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=etapas, x=[t["procesados"], t["candidatos"], t["clasificados"], 0, 0],
        orientation="h", marker_color=_COLOR_ETAPA, showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Bar(
        y=etapas, x=[0, 0, 0, t["odio_confirmado"], 0], orientation="h",
        marker_color=_COLOR_ODIO_CONF, hoverinfo="skip",
        name=f"Odio confirmado por revisión humana ({_fmt_int(t['odio_confirmado'])})",
    ))
    fig.add_trace(go.Bar(
        y=etapas, x=[0, 0, 0, t["odio_ia"], 0], orientation="h",
        marker_color=_COLOR_ODIO_IA, hoverinfo="skip",
        name=f"Odio según IA, sin validar ({_fmt_int(t['odio_ia'])})",
    ))
    fig.add_trace(go.Bar(
        y=etapas, x=[0, 0, 0, 0, t["validados"]], orientation="h",
        marker_color=_COLOR_VALIDADOS, showlegend=False, hoverinfo="skip",
    ))
    for etapa, v in zip(etapas, valores):
        fig.add_annotation(
            x=v, y=etapa, xanchor="left", xshift=6, showarrow=False,
            text=f"<b>{_fmt_int(v)}</b>  ·  {_fmt_pct_es(v / base * 100)}",
            font=dict(size=13),
        )
    fig.update_layout(
        barmode="stack", height=360, bargap=0.35,
        title="Del total procesado al odio identificado"
              "<br><sup>% sobre los mensajes procesados</sup>",
        legend=dict(orientation="h", yanchor="top", y=-0.05, x=0, xanchor="left"),
        margin=dict(l=10, r=20, t=60, b=40),
    )
    fig.update_xaxes(range=[0, base * 1.25], showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(autorange="reversed", automargin=True, tickfont=dict(size=12))
    st.plotly_chart(fig, use_container_width=True, theme=None)

    nota = (
        "La IA se aplica a una selección priorizada de candidatos: las cifras de odio son un "
        "mínimo identificado, no una estimación de la proporción de odio en el total de conversaciones."
    )
    if t["ia_odio_revisados"] >= _MIN_MUESTRA_PRECISION:
        prec = t["ia_odio_confirmados"] / t["ia_odio_revisados"] * 100
        nota += (
            f" En la muestra revisada por personas ({_fmt_int(t['ia_odio_revisados'])} mensajes), "
            f"el {prec:.0f}% de lo marcado como odio por la IA fue confirmado."
        )
    st.caption(nota)
    return fig


def _render_odio_por_plataforma(df_emb: pd.DataFrame):
    dfp = (
        df_emb.groupby("plataforma", as_index=False)[["procesados", "odio_confirmado", "odio_ia"]]
        .sum()
    )
    dfp = dfp[dfp["procesados"] > 0].copy()
    if dfp.empty:
        st.info("Sin datos por plataforma.")
        return None
    dfp["pct_conf"] = dfp["odio_confirmado"] / dfp["procesados"] * 100
    dfp["pct_ia"] = dfp["odio_ia"] / dfp["procesados"] * 100
    dfp["pct_total"] = dfp["pct_conf"] + dfp["pct_ia"]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        x=dfp["plataforma"], y=dfp["pct_conf"], name="Confirmado por revisión humana",
        marker_color=_COLOR_ODIO_CONF,
        customdata=dfp[["odio_confirmado", "procesados"]].values,
        hovertemplate="%{x}: %{y:.1f}% (%{customdata[0]:,} de %{customdata[1]:,})<extra>Confirmado</extra>",
    ))
    fig.add_trace(go.Bar(
        x=dfp["plataforma"], y=dfp["pct_ia"], name="Según IA, sin validar",
        marker_color=_COLOR_ODIO_IA,
        customdata=dfp[["odio_ia", "procesados"]].values,
        hovertemplate="%{x}: %{y:.1f}% (%{customdata[0]:,} de %{customdata[1]:,})<extra>Solo IA</extra>",
    ))
    for _, r in dfp.iterrows():
        fig.add_annotation(
            x=r["plataforma"], y=r["pct_total"], yshift=12, showarrow=False,
            text=f"<b>{_fmt_pct_es(r['pct_total'])}</b>",
        )
    fig.update_layout(
        barmode="stack", height=380,
        title="Odio identificado por plataforma"
              "<br><sup>% sobre los mensajes procesados de cada plataforma</sup>",
        legend=dict(orientation="h", yanchor="top", y=-0.12, x=0.5, xanchor="center"),
        margin=dict(l=10, r=10, t=70, b=40),
    )
    fig.update_yaxes(ticksuffix="%", title_text="", range=[0, float(dfp["pct_total"].max()) * 1.25])
    fig.update_xaxes(title_text="")
    st.plotly_chart(fig, use_container_width=True, theme=None)
    return fig


@st.cache_data(ttl=60)
def load_last_pipeline_health_summary(pipeline_name: str = "reto_pipeline_diario") -> dict:
    """
    Lee la última corrida cloud registrada en processed.pipeline_health.

    Devuelve un resumen por plataforma (x / youtube) para usar como
    fuente principal del banner de estado en operación cloud-first.
    """
    try:
        with _pooled_conn() as conn:
            last_run_df = pd.read_sql(
                """
                SELECT run_id, run_at
                FROM processed.pipeline_health
                WHERE pipeline_name = %s
                ORDER BY run_at DESC
                LIMIT 1
                """,
                conn,
                params=(pipeline_name,),
            )
            if last_run_df.empty:
                return {"exists": False}

            run_id = str(last_run_df.iloc[0]["run_id"] or "")
            run_at = last_run_df.iloc[0]["run_at"]

            details_df = pd.read_sql(
                """
                SELECT
                    platform,
                    last_ingested_at,
                    rows_new_window,
                    stagnated,
                    critical_stage_ok,
                    failed_stages,
                    warnings,
                    errors
                FROM processed.pipeline_health
                WHERE pipeline_name = %s
                  AND run_id = %s
                ORDER BY platform ASC
                """,
                conn,
                params=(pipeline_name, run_id),
            )
    except Exception:
        return {"exists": False}

    if details_df.empty:
        return {"exists": False}

    def _safe_text_cell(val) -> str:
        if val is None:
            return ""
        try:
            if pd.isna(val):
                return ""
        except Exception:
            pass
        if isinstance(val, (list, tuple, set)):
            parts = [str(x).strip() for x in val if str(x).strip()]
            return ", ".join(parts)
        return str(val).strip()

    platforms: dict[str, dict] = {}
    for _, row in details_df.iterrows():
        p = str(row.get("platform") or "").strip().lower()
        if not p:
            continue
        platforms[p] = {
            "platform": p,
            "last_ingested_at": row.get("last_ingested_at"),
            "rows_new_window": int(row["rows_new_window"]) if row.get("rows_new_window") is not None else 0,
            "stagnated": bool(row["stagnated"]) if row.get("stagnated") is not None else False,
            "critical_stage_ok": bool(row["critical_stage_ok"]) if row.get("critical_stage_ok") is not None else False,
            "failed_stages": _safe_text_cell(row.get("failed_stages")),
            "warnings": _safe_text_cell(row.get("warnings")),
            "errors": _safe_text_cell(row.get("errors")),
        }

    has_critical_error = any(not p["critical_stage_ok"] for p in platforms.values())
    any_stagnated = any(p["stagnated"] for p in platforms.values())
    has_errors_text = any(bool(p["errors"]) for p in platforms.values())
    has_warnings_text = any(bool(p["warnings"]) for p in platforms.values())

    return {
        "exists": True,
        "source": "pipeline_health",
        "pipeline_name": pipeline_name,
        "run_id": run_id,
        "run_at": run_at,
        "platforms": platforms,
        "has_critical_error": has_critical_error,
        "any_stagnated": any_stagnated,
        "has_errors_text": has_errors_text,
        "has_warnings_text": has_warnings_text,
    }


@st.cache_data(ttl=60)
def load_last_pipeline_run_legacy(pipeline_name: str = "reto_x_diario") -> dict:
    """
    Lee la última corrida registrada en processed.pipeline_runs.

    Sirve para mostrar en la app que la actualización diaria se ejecutó
    aunque no haya habido datos nuevos (changes_detected = False).
    """
    try:
        with _pooled_conn() as conn:
            df = pd.read_sql(
                """
                SELECT
                    started_at,
                    finished_at,
                    status,
                    changes_detected,
                    ok_count,
                    fail_count,
                    triggered_by,
                    detail
                FROM processed.pipeline_runs
                WHERE pipeline_name = %s
                ORDER BY started_at DESC
                LIMIT 1
                """,
                conn,
                params=(pipeline_name,),
            )
    except Exception:
        return {"exists": False}

    if df.empty:
        return {"exists": False}

    row = df.iloc[0]
    return {
        "exists": True,
        "started_at": row["started_at"],
        "finished_at": row["finished_at"],
        "status": row["status"],
        "changes_detected": bool(row["changes_detected"]) if row["changes_detected"] is not None else False,
        "ok_count": int(row["ok_count"]) if row["ok_count"] is not None else 0,
        "fail_count": int(row["fail_count"]) if row["fail_count"] is not None else 0,
        "triggered_by": row["triggered_by"] or "",
        "detail": row["detail"] or "",
    }


def resolve_pipeline_banner_state(
    health_pipeline_name: str = "reto_pipeline_diario",
    legacy_pipeline_name: str = "reto_x_diario",
    desalineacion_horas: int = 24,
) -> dict:
    """
    Resuelve el estado operativo del banner con prioridad cloud-first:

    1) Fuente principal: processed.pipeline_health
    2) Fallback: processed.pipeline_runs (legacy)
    3) Señal de desalineación cuando legacy quedó viejo/en error pero cloud ya corrió
    """
    health = load_last_pipeline_health_summary(pipeline_name=health_pipeline_name)
    legacy = load_last_pipeline_run_legacy(pipeline_name=legacy_pipeline_name)

    if health.get("exists"):
        run_at = health.get("run_at")
        run_ts = pd.Timestamp(run_at) if run_at is not None else None
        platforms = health.get("platforms") or {}

        expected_platforms = {"x", "youtube"}
        missing_platforms = sorted(expected_platforms - set(platforms.keys()))
        has_platform_gap = len(missing_platforms) > 0

        has_critical_error = bool(health.get("has_critical_error"))
        any_stagnated = bool(health.get("any_stagnated"))
        total_rows_loaded = sum(int(p.get("rows_new_window") or 0) for p in platforms.values())

        # Criterio único: ¿se cargaron mensajes o no?
        # - has_critical_error: alguna plataforma NO cargó mensajes por un
        #   fallo real de ejecución (ver healthcheck_pipeline.py).
        # - Si no hubo error y se cargó al menos un mensaje -> éxito.
        # - Si no hubo error y no se cargó nada -> informativo (sin novedades).
        if has_critical_error:
            severity = "error"
        elif total_rows_loaded > 0:
            severity = "success"
        else:
            severity = "info"

        issues = []
        for p in sorted(platforms.keys()):
            p_info = platforms[p]
            if not p_info.get("critical_stage_ok", True):
                fail_txt = p_info.get("failed_stages") or "etapas críticas"
                issues.append(f"{p}: fallo crítico ({fail_txt})")
            if p_info.get("stagnated", False):
                issues.append(f"{p}: estancado")
            if p_info.get("errors"):
                issues.append(f"{p}: {p_info['errors']}")
        if has_platform_gap:
            issues.append(f"Plataformas faltantes en healthcheck: {', '.join(missing_platforms)}")

        # Señal de desalineación con legacy (informativa)
        desalineado = False
        desalineado_msg = ""
        if legacy.get("exists"):
            legacy_ts_raw = legacy.get("started_at")
            legacy_ts = pd.Timestamp(legacy_ts_raw) if legacy_ts_raw is not None else None
            legacy_status = (legacy.get("status") or "").lower()
            if run_ts is not None and legacy_ts is not None:
                delta_horas = (run_ts - legacy_ts).total_seconds() / 3600.0
                if delta_horas > desalineacion_horas and legacy_status in {"error", "partial"}:
                    desalineado = True
                    desalineado_msg = (
                        "Se detecta desalineación: pipeline_runs (legacy) quedó más antiguo/en error, "
                        "pero pipeline_health (cloud) tiene corrida más reciente."
                    )

        return {
            "exists": True,
            "source": "pipeline_health",
            "severity": severity,
            "run_at": run_ts,
            "run_id": health.get("run_id") or "",
            "platforms": platforms,
            "issues": issues,
            "has_critical_error": has_critical_error,
            "total_rows_loaded": total_rows_loaded,
            "any_stagnated": any_stagnated,
            "has_platform_gap": has_platform_gap,
            "missing_platforms": missing_platforms,
            "desalineado": desalineado,
            "desalineado_msg": desalineado_msg,
            "legacy_fallback_used": False,
        }

    if legacy.get("exists"):
        status = (legacy.get("status") or "").lower()
        if status == "error":
            severity = "error"
        elif status == "partial":
            severity = "warning"
        elif status == "ok":
            severity = "success" if legacy.get("changes_detected") else "info"
        else:
            severity = "info"

        return {
            "exists": True,
            "source": "pipeline_runs_legacy",
            "severity": severity,
            "run_at": legacy.get("started_at"),
            "run_id": "",
            "platforms": {},
            "issues": [],
            "has_critical_error": status == "error",
            "total_rows_loaded": None,
            "any_stagnated": False,
            "has_platform_gap": False,
            "missing_platforms": [],
            "desalineado": False,
            "desalineado_msg": "",
            "legacy_fallback_used": True,
            "legacy_info": legacy,
        }

    return {
        "exists": False,
        "source": "none",
        "severity": "info",
        "run_at": None,
        "run_id": "",
        "platforms": {},
        "issues": [],
        "has_critical_error": False,
        "any_stagnated": False,
        "has_platform_gap": False,
        "missing_platforms": [],
        "desalineado": False,
        "desalineado_msg": "",
        "legacy_fallback_used": False,
    }


def render_pipeline_status_banner(
    health_pipeline_name: str = "reto_pipeline_diario",
    legacy_pipeline_name: str = "reto_x_diario",
) -> None:
    """
    Banner operativo cloud-first:
    - Prioriza processed.pipeline_health (GitHub Actions).
    - Usa fallback en processed.pipeline_runs (legacy) si no hay health.
    - Señala desalineación entre fuentes cuando corresponda.
    """
    state = resolve_pipeline_banner_state(
        health_pipeline_name=health_pipeline_name,
        legacy_pipeline_name=legacy_pipeline_name,
    )
    if not state.get("exists"):
        st.info("Aún no hay registros operativos del pipeline (ni cloud ni fallback legacy).")
        return

    run_ts_raw = state.get("run_at")
    try:
        run_ts = pd.Timestamp(run_ts_raw) if run_ts_raw is not None else None
    except Exception:
        run_ts = None
    fecha_txt = run_ts.strftime("%d/%m/%Y %H:%M") if run_ts is not None else "—"

    severity = state.get("severity", "info")
    source = state.get("source")
    source_lbl = "GitHub Actions" if source == "pipeline_health" else "fallback legacy"
    icon = {"success": "✅", "error": "❌", "warning": "⚠️"}.get(severity, "ℹ️")

    total_rows_loaded = state.get("total_rows_loaded")
    if source == "pipeline_health" and total_rows_loaded is not None:
        if severity == "error":
            detalle = "no se pudieron cargar mensajes (falló la ejecución)"
        elif total_rows_loaded > 0:
            detalle = f"{total_rows_loaded:,} mensajes nuevos cargados"
        else:
            detalle = "sin mensajes nuevos (no había novedades)"
        msg = f"{icon} Última actualización: {fecha_txt} ({source_lbl}) — {detalle}"
    else:
        msg = f"{icon} Última actualización: {fecha_txt} ({source_lbl})"

    if severity == "error":
        st.error(msg)
    elif severity == "warning":
        st.warning(msg)
    elif severity == "success":
        st.success(msg)
    else:
        st.info(msg)

    LEGACY_PIPELINE_RUNS_THRESHOLD_DAYS = 7
    if state.get("desalineado") and st.session_state.get("user_role") == "admin":
        legacy = load_last_pipeline_run_legacy(pipeline_name=legacy_pipeline_name)
        legacy_reciente = False
        if legacy.get("exists"):
            legacy_ts_raw = legacy.get("started_at")
            if legacy_ts_raw is not None:
                try:
                    legacy_ts = pd.Timestamp(legacy_ts_raw)
                    now = (
                        pd.Timestamp.now(tz=legacy_ts.tzinfo)
                        if legacy_ts.tzinfo is not None
                        else pd.Timestamp.now()
                    )
                    age_days = (now - legacy_ts).total_seconds() / 86400.0
                    legacy_reciente = age_days < LEGACY_PIPELINE_RUNS_THRESHOLD_DAYS
                except Exception:
                    legacy_reciente = False
        if legacy_reciente:
            st.caption("⚠️ Desalineación detectada: pipeline_runs legacy más antiguo/en error que pipeline_health cloud.")


# ============================================================
# SECTIONS
# ============================================================
@st.cache_data(ttl=3600)
def load_gold_stats() -> dict:
    with _pooled_conn() as conn:
        row = pd.read_sql(f"""
            WITH llm_comparison AS (
                SELECT
                    g.message_uuid,
                    UPPER(REPLACE(g.y_odio_final, ' ', '_'))
                        != UPPER(e.clasificacion_principal)                          AS corrigio_odio,
                    g.y_categoria_final IS DISTINCT FROM e.categoria_odio_pred
                        AND g.y_categoria_final IS NOT NULL                          AS corrigio_categoria,
                    g.y_intensidad_final IS DISTINCT FROM NULLIF(e.intensidad_pred,'')::smallint
                        AND g.y_intensidad_final IS NOT NULL                        AS corrigio_intensidad
                FROM processed.gold_dataset g
                JOIN ({_SQL_LLM_ULTIMA}) e USING (message_uuid)
                WHERE g.label_source = 'llm_validated'
                  AND g.y_odio_bin IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM processed.gold_dataset)     AS total_gold,
                COUNT(*)                                          AS total_llm,
                COUNT(*) FILTER (WHERE corrigio_odio)             AS n_corrigio_odio,
                COUNT(*) FILTER (WHERE corrigio_categoria)        AS n_corrigio_categoria,
                COUNT(*) FILTER (WHERE corrigio_intensidad
                    AND corrigio_intensidad IS NOT NULL)          AS n_corrigio_intensidad,
                COUNT(*) FILTER (WHERE corrigio_intensidad
                    IS NOT NULL)                                  AS total_con_intensidad,
                (SELECT MAX(ingested_at) FROM processed.gold_dataset) AS fecha_validacion
            FROM llm_comparison
        """, conn).iloc[0]

    total_gold = int(row["total_gold"] or 0)
    total_llm  = int(row["total_llm"]  or 0)
    total_int  = int(row["total_con_intensidad"] or 0)

    return {
        "total_gold":              total_gold,
        "total_llm":               total_llm,
        "pct_concordancia_llm":    (1 - row["n_corrigio_odio"] / total_llm) * 100 if total_llm else None,
        "pct_corrigio_odio":       (row["n_corrigio_odio"]      / total_llm) * 100 if total_llm else None,
        "pct_corrigio_categoria":  (row["n_corrigio_categoria"]  / total_llm) * 100 if total_llm else None,
        "pct_corrigio_intensidad": (row["n_corrigio_intensidad"] / total_int) * 100 if total_int else None,
        "total_con_intensidad":    total_int,
        "fecha_validacion":        row["fecha_validacion"],
    }


@st.cache_data(ttl=3600)
def load_matriz_confusion() -> pd.DataFrame:
    with _pooled_conn() as conn:
        return pd.read_sql(f"""
        WITH e AS ({_SQL_LLM_ULTIMA})
            SELECT
                CASE e.clasificacion_principal
                    WHEN 'ODIO' THEN 'Odio'
                    WHEN 'NO_ODIO' THEN 'No Odio'
                    WHEN 'DUDOSO' THEN 'Dudoso'
                    ELSE e.clasificacion_principal
                END              AS llm,
                g.y_odio_final   AS humano,
                COUNT(*)         AS n
            FROM processed.gold_dataset g
            JOIN e USING (message_uuid)
            WHERE g.y_odio_final IS NOT NULL
            GROUP BY 1, 2
        """, conn)


def _render_gold_dataset_card() -> None:
    st.markdown("---")
    st.subheader("📋 Gold Dataset")
    try:
        g = load_gold_stats()
    except Exception:
        st.warning("Gold dataset no disponible")
        return
    if not g or g["total_gold"] == 0:
        st.warning("Gold dataset no disponible")
        return

    fecha_str = (
        pd.Timestamp(g["fecha_validacion"]).strftime("%d/%m/%Y")
        if g["fecha_validacion"] is not None else "—"
    )

    c1, c2, c3 = st.columns(3)
    c1.metric("Total mensajes gold", f"{g['total_gold']:,}")
    c2.metric("Etiquetados con LLM", f"{g['total_llm']:,}")
    c3.metric("Última validación", fecha_str)

    st.markdown(
        f"**Calidad del etiquetado LLM** *(sobre los {g['total_llm']:,} mensajes llm_validated)*"
    )

    c4, c5, c6, c7 = st.columns(4)
    def fmt_pct(v):
        return f"{v:.1f}%" if v is not None else "—"

    c4.metric("Concordancia LLM",     fmt_pct(g["pct_concordancia_llm"]))
    c5.metric("Corrección odio",      fmt_pct(g["pct_corrigio_odio"]))
    c6.metric("Corrección categoría", fmt_pct(g["pct_corrigio_categoria"]))
    c7.metric("Corrección intensidad",fmt_pct(g["pct_corrigio_intensidad"]))

    st.caption(
        f"Correcciones calculadas sobre {g['total_llm']:,} mensajes llm_validated · "
        f"Corrección de intensidad sobre {g['total_con_intensidad']:,} con intensidad registrada"
    )

    st.markdown("**Matriz de confusión: LLM vs revisión humana**")
    try:
        dfm = load_matriz_confusion()
    except Exception:
        dfm = pd.DataFrame()
    if dfm.empty:
        st.info("Sin mensajes con etiqueta LLM y revisión humana.")
        return
    orden = ["Odio", "No Odio", "Dudoso"]
    piv = (
        dfm.pivot_table(index="llm", columns="humano", values="n", aggfunc="sum", fill_value=0)
        .reindex(index=orden, columns=orden, fill_value=0)
    )
    total_m = int(piv.values.sum())
    diag = int(sum(piv.loc[c, c] for c in orden if c in piv.index and c in piv.columns))
    tp = int(piv.loc["Odio", "Odio"]) if "Odio" in piv.index and "Odio" in piv.columns else 0
    llm_odio = int(piv.loc["Odio"].sum()) if "Odio" in piv.index else 0
    hum_odio = int(piv["Odio"].sum()) if "Odio" in piv.columns else 0
    m1, m2, m3 = st.columns(3)
    m1.metric("Concordancia global", fmt_pct(diag / total_m * 100 if total_m else None))
    m2.metric("Precisión LLM en odio", fmt_pct(tp / llm_odio * 100 if llm_odio else None))
    m3.metric("Recall LLM en odio", fmt_pct(tp / hum_odio * 100 if hum_odio else None))
    piv.index.name = "LLM ↓ / Humano →"
    piv["Total"] = piv.sum(axis=1)
    st.dataframe(piv, use_container_width=True)
    st.caption(
        f"Sobre {total_m:,} mensajes con etiqueta LLM y revisión humana (todo el Gold, no solo "
        "llm_validated). Precisión: de lo que el LLM marcó como odio, cuánto confirmó la revisión "
        "humana. Recall: del odio confirmado por humanos, cuánto había detectado el LLM."
    )


def render_panel_general():
    _render_section_header(
        "Panel general",
        "Indicadores clave del proyecto ReTo · visión consolidada de volumen, "
        "clasificaciones y validación humana.",
    )
    render_aviso_cambio_criterio()

    if st.session_state.get("user_role") != "viewer":
        render_pipeline_status_banner()

    _access_raw = _role_can_access_raw()
    opts = load_filter_options(_access_raw)

    fc1, fc2 = st.columns(2)
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="pg_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="pg_med",
        placeholder="Todos los medios",
    )

    kpis = load_kpis(
        _access_raw,
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
    )

    mensajes_totales = kpis["total_raw"]
    candidatos_odio = kpis["total_candidatos"]
    etiquetados_llm = kpis["total_etiquetados_llm"]
    medios_monitorizados = kpis["total_medios"]
    mensajes_validados = kpis["total_gold"]
    mensajes_odio = kpis["total_gold_odio"]
    nuevos_x_hoy = kpis["nuevos_x_hoy"]
    nuevos_yt_hoy = kpis["nuevos_yt_hoy"]
    nuevos_x_ayer = kpis["nuevos_x_ayer"]
    nuevos_yt_ayer = kpis["nuevos_yt_ayer"]

    label_raw = "Mensajes totales (raw)" if _access_raw else "Mensajes procesados"
    label_llm = "Etiquetados por IA" if not _access_raw else "Etiquetados por LLM"

    _render_pg_kpi_grid([
        (label_raw, f"{mensajes_totales:,}", ""),
        ("Candidatos a odio", f"{candidatos_odio:,}", ""),
        (label_llm, f"{etiquetados_llm:,}", ""),
        ("Mensajes validados", f"{mensajes_validados:,}", f"{mensajes_odio:,} odio"),
        ("Medios monitorizados", f"{medios_monitorizados:,}", ""),
    ])
    st.markdown(
        '<div class="pg-kpi-section-label">Actividad reciente</div>',
        unsafe_allow_html=True,
    )
    _render_pg_kpi_grid([
        ("Nuevos X hoy", f"{nuevos_x_hoy:,}", ""),
        ("Nuevos YouTube hoy", f"{nuevos_yt_hoy:,}", ""),
        ("Nuevos X ayer", f"{nuevos_x_ayer:,}", ""),
        ("Nuevos YouTube ayer", f"{nuevos_yt_ayer:,}", ""),
    ], secondary=True)

    st.markdown("---")

    _plat_t = tuple(sel_platforms) if sel_platforms else None
    _med_t = tuple(sel_medios) if sel_medios else None

    # --- 1. Embudo de detección (base: mensajes procesados) ---
    fig_embudo = fig_plat = fig_int = fig_cat = fig_avg = None
    df_emb = load_embudo(platforms=_plat_t, medios=_med_t)
    if df_emb.empty or int(df_emb["procesados"].sum()) == 0:
        st.info("No hay mensajes procesados para los filtros seleccionados.")
    else:
        fig_embudo = _render_embudo(df_emb)

    st.markdown("---")

    # --- Datos combinados Gold + LLM (solo para gráficos condicionales a odio) ---
    df_comb = _load_panel_combined(platforms=_plat_t, medios=_med_t)

    if df_comb.empty:
        st.info(
            _ui_label("No hay datos clasificados (Gold o LLM) para los filtros seleccionados.")
        )
    else:
        df_odio = df_comb[df_comb["odio_label"] == "Odio"].copy()
        # '' (cadena vacía) pasa .notna() → normalizamos a NA para no generar barra sin nombre
        df_odio["categoria"] = (
            df_odio["categoria"].astype("string").str.strip().replace("", pd.NA)
        )
        n_odio = len(df_odio)
        sub_odio = f"<br><sup>Entre mensajes clasificados como odio (n = {_fmt_int(n_odio)})</sup>"

        if st.session_state.get("user_role", "admin") == "admin":
            n_gold_odio = int((df_odio["fuente"] == "Gold").sum())
            n_llm_odio = int((df_odio["fuente"] == "LLM").sum())
            st.caption(
                f"Intensidad y categoría basadas en **{_fmt_int(n_odio)}** mensajes de odio: "
                f"**{_fmt_int(n_gold_odio)}** validados por humanos (Gold) · "
                f"**{_fmt_int(n_llm_odio)}** etiquetados por IA"
            )

        col_g1, col_g2 = st.columns(2)

        # 2. Odio identificado por plataforma (% sobre procesados de cada plataforma)
        with col_g1:
            if not df_emb.empty:
                fig_plat = _render_odio_por_plataforma(df_emb)

        # 3. Distribución de intensidad
        with col_g2:
            df_int = df_odio[df_odio["intensidad"].notna()].copy()
            if not df_int.empty:
                df_int["intensidad"] = df_int["intensidad"].astype(int)
                int_data = df_int["intensidad"].value_counts().sort_index().reset_index()
                int_data.columns = ["Intensidad", "Cantidad"]
                int_data["Intensidad"] = int_data["Intensidad"].astype(str)
                int_data["Porcentaje"] = int_data["Cantidad"] / int_data["Cantidad"].sum() * 100
                int_data["pct_txt"] = int_data["Porcentaje"].map(
                    lambda v: f"{v:.1f}%".replace(".", ",")
                )
                fig_int = px.bar(
                    int_data, x="Intensidad", y="Porcentaje",
                    color="Intensidad",
                    color_discrete_map=INTENSITY_COLORS,
                    title="Distribución de intensidad" + sub_odio,
                    text="pct_txt",
                    custom_data=["Cantidad"],
                )
                fig_int.update_traces(
                    textposition="outside", cliponaxis=False,
                    hovertemplate="Intensidad %{x}: %{y:.1f}%<br>%{customdata[0]:,} mensajes<extra></extra>",
                )
                fig_int.update_layout(height=380, showlegend=False, margin=dict(t=70), bargap=0.15)
                fig_int.update_xaxes(type="category", categoryorder="array",
                                     categoryarray=["1", "2", "3"])
                fig_int.update_yaxes(
                    range=[0, float(int_data["Porcentaje"].max()) * 1.15],
                    ticksuffix="%", title_text="% de mensajes de odio",
                )
                st.plotly_chart(
                    fig_int,
                    use_container_width=True,
                    theme=None,
                    config={
                        "displayModeBar": "hover",
                        "displaylogo": False,
                        "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
                    },
                )
            else:
                st.info("Sin datos de intensidad.")

        col_g3, col_g4 = st.columns(2)

        # 4. Distribución por categoría
        with col_g3:
            df_cat = df_odio[df_odio["categoria"].notna()].copy()
            if not df_cat.empty:
                df_cat["categoria_label"] = df_cat["categoria"].map(
                    CATEGORIAS_LABELS
                ).fillna(df_cat["categoria"])
                cat_data = df_cat["categoria_label"].value_counts().reset_index()
                cat_data.columns = ["Categoría", "Cantidad"]
                df_categoria = cat_data.sort_values("Cantidad", ascending=True)
                fig_cat = px.bar(
                    df_categoria, x="Cantidad", y="Categoría", orientation="h",
                    color="Categoría",
                    color_discrete_map=CAT_COLOR_MAP,
                    title="Distribución por categoría de odio" + sub_odio,
                    text_auto=True,
                )
                fig_cat.update_layout(
                    height=380, showlegend=False,
                    yaxis=dict(autorange="reversed"),
                )
                _apply_horizontal_bar_labels(fig_cat)
                fig_cat.update_yaxes(title_text="", automargin=True, tickfont=dict(size=11))
                fig_cat.update_layout(margin=dict(l=10, r=20, t=70, b=40))
                st.plotly_chart(fig_cat, use_container_width=True, theme=None)
            else:
                st.info("Sin datos de categoría.")

        # 5. Intensidad promedio por categoría
        with col_g4:
            df_cat_int = df_odio[
                df_odio["categoria"].notna() & df_odio["intensidad"].notna()
            ].copy()
            if not df_cat_int.empty:
                df_cat_int["intensidad"] = df_cat_int["intensidad"].astype(float)
                df_cat_int["categoria_label"] = df_cat_int["categoria"].map(
                    CATEGORIAS_LABELS
                ).fillna(df_cat_int["categoria"])
                avg_int = (
                    df_cat_int.groupby("categoria_label")["intensidad"]
                    .mean().round(2).sort_values(ascending=False)
                    .reset_index()
                )
                avg_int.columns = ["Categoría", "Intensidad promedio"]
                fig_avg = px.bar(
                    avg_int, x="Intensidad promedio", y="Categoría", orientation="h",
                    color="Intensidad promedio",
                    color_continuous_scale=[[0, "#FBD38D"], [0.5, "#F59E0B"], [1, "#C0392B"]],
                    title="Intensidad promedio por categoría" + sub_odio,
                    text_auto=".2f",
                )
                fig_avg.update_layout(
                    height=380,
                    yaxis=dict(autorange="reversed"),
                    coloraxis_showscale=False,
                )
                _apply_horizontal_bar_labels(fig_avg)
                fig_avg.update_yaxes(title_text="", automargin=True, tickfont=dict(size=11))
                fig_avg.update_xaxes(range=[0, 3], dtick=0.5,
                                     title_text="1 = baja · 3 = alta")
                fig_avg.update_layout(margin=dict(l=10, r=60, t=70, b=40))
                st.plotly_chart(fig_avg, use_container_width=True, theme=None)

        render_section_exports(
            section_key="panel_general",
            section_title="Panel general",
            csv_items=[
                ("embudo", df_emb),
                ("datos_combinados", df_comb),
                ("kpis", pd.DataFrame([kpis])),
            ],
            fig_items=[
                {"title": "Embudo de detección", "fig": fig_embudo, "kind": "plotly"},
                {"title": "Odio identificado por plataforma", "fig": fig_plat, "kind": "plotly"},
                {"title": "Intensidad del odio", "fig": fig_int, "kind": "plotly"},
                {"title": "Categorías de odio", "fig": fig_cat, "kind": "plotly"},
                {"title": "Intensidad promedio por categoría", "fig": fig_avg, "kind": "plotly"},
            ],
        )

    # Tarjeta de rendimiento del modelo: solo admin (no viewer ni editor)
    if st.session_state.get("user_role") == "admin":
        _render_gold_dataset_card()


@st.cache_data(ttl=300, show_spinner="Cargando datos…")
def _load_panel_combined(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> pd.DataFrame:
    """Carga datos combinados Gold + LLM para gráficos del panel general.

    Gold tiene prioridad: si un mensaje está en gold Y en LLM, se usa gold.
    """
    platforms_l = _expand_platforms(list(platforms) if platforms else None)
    medios_l = list(medios) if medios else None

    conds = [
        "(g.message_uuid IS NOT NULL OR e.message_uuid IS NOT NULL)",
    ]
    params: list = []
    if platforms_l:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms_l))
    if medios_l:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios_l))

    where = " AND ".join(conds)

    with _pooled_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                pm.platform,
                COALESCE(
                    g.y_odio_final,
                    CASE
                        WHEN e.clasificacion_principal = 'ODIO' THEN 'Odio'
                        WHEN e.clasificacion_principal = 'DUDOSO' THEN 'Dudoso'
                        WHEN e.clasificacion_principal IS NOT NULL THEN 'No Odio'
                    END
                ) AS odio_label,
                COALESCE(
                    g.y_categoria_final,
                    CASE WHEN e.clasificacion_principal = 'ODIO'
                         THEN e.categoria_odio_pred END
                ) AS categoria,
                COALESCE(
                    g.y_intensidad_final::text,
                    CASE WHEN e.clasificacion_principal = 'ODIO'
                         THEN e.intensidad_pred END
                ) AS intensidad,
                CASE WHEN g.message_uuid IS NOT NULL THEN 'Gold'
                     ELSE 'LLM' END AS fuente
            FROM processed.mensajes pm
            LEFT JOIN processed.gold_dataset g USING (message_uuid)
            LEFT JOIN ({_SQL_LLM_ULTIMA}) e USING (message_uuid)
            WHERE {where}
        """, conn, params=params)

    if not df.empty:
        df["plataforma"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
        df["intensidad"] = pd.to_numeric(df["intensidad"], errors="coerce")

    return df
