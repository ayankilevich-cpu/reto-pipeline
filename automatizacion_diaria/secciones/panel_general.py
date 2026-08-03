"""Sección «Panel general» del dashboard (KPIs + estado del pipeline)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List, Optional, Tuple
import html
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
    EXCLUDED_SOURCE_MEDIA,
    INTENSITY_COLORS,
    PLATFORM_DISPLAY,
    SEMANTIC_COLORS,
    _expand_platforms,
    platform_label,
)
from components.ui import (
    _apply_horizontal_bar_labels,
    _render_section_header,
    _role_can_access_raw,
    _ui_label,
)
from components.exports import render_section_exports
from components.db_helpers import load_filter_options


def _render_pg_kpi_grid(
    cards: List[Tuple[str, str, str]],
    *,
    secondary: bool = False,
) -> None:
    """Renderiza KPIs del panel general como grid responsive de tarjetas HTML/CSS."""
    cards_html = []
    card_style = ' style="opacity:0.75;"' if secondary else ""
    for label, value, delta in cards:
        d = (
            f'<div class="pg-kpi-delta">{html.escape(delta)}</div>'
            if delta
            else ""
        )
        cards_html.append(
            f'<div class="pg-kpi-card"{card_style}>'
            f'<div class="pg-kpi-label">{html.escape(label)}</div>'
            f'<div class="pg-kpi-value">{html.escape(value)}</div>'
            f"{d}"
            "</div>"
        )
    st.markdown(
        f'<div class="pg-kpi-grid">{"".join(cards_html)}</div>',
        unsafe_allow_html=True,
    )


@st.cache_data(ttl=300)
def load_kpis(
    access_raw: bool,
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
) -> dict:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None

    with get_conn() as conn:
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
            SELECT count(*),
                   count(*) FILTER (WHERE e.clasificacion_principal = 'ODIO')
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


@st.cache_data(ttl=60)
def load_last_pipeline_health_summary(pipeline_name: str = "reto_pipeline_diario") -> dict:
    """
    Lee la última corrida cloud registrada en processed.pipeline_health.

    Devuelve un resumen por plataforma (x / youtube) para usar como
    fuente principal del banner de estado en operación cloud-first.
    """
    try:
        with get_conn() as conn:
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
        with get_conn() as conn:
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
        has_errors_text = bool(health.get("has_errors_text"))
        has_warnings_text = bool(health.get("has_warnings_text"))

        if has_critical_error:
            severity = "error"
        elif any_stagnated or has_platform_gap:
            severity = "warning"
        elif has_errors_text:
            severity = "warning"
        elif has_warnings_text:
            severity = "info"
        else:
            severity = "success"

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
    icon = "✅" if severity == "success" else ("❌" if severity == "error" else "⚠️")
    msg = f"{icon} Última actualización: {fecha_txt} ({source_lbl})"

    if severity == "error":
        st.error(msg)
    elif severity in {"warning", "info"}:
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
    with get_conn() as conn:
        row = pd.read_sql("""
            WITH llm_comparison AS (
                SELECT
                    g.message_uuid,
                    UPPER(g.y_odio_final) != UPPER(e.clasificacion_principal)        AS corrigio_odio,
                    g.y_categoria_final IS DISTINCT FROM e.categoria_odio_pred
                        AND g.y_categoria_final IS NOT NULL                          AS corrigio_categoria,
                    g.y_intensidad_final IS DISTINCT FROM NULLIF(e.intensidad_pred,'')::smallint
                        AND g.y_intensidad_final IS NOT NULL                        AS corrigio_intensidad
                FROM processed.gold_dataset g
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE g.label_source = 'llm_validated'
                  AND g.y_odio_bin IS NOT NULL
            )
            SELECT
                (SELECT COUNT(*) FROM processed.gold_dataset
                 WHERE y_odio_bin IS NOT NULL)                    AS total_gold,
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

    st.markdown("**Calidad del etiquetado LLM** *(sobre los 733 mensajes llm_validated)*")

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


def render_panel_general():
    _render_section_header(
        "Panel general",
        "Indicadores clave del proyecto ReTo · visión consolidada de volumen, "
        "clasificaciones y validación humana.",
    )

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

    # --- Cargar datos combinados Gold + LLM para gráficos ---
    df_comb = _load_panel_combined(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
    )

    if df_comb.empty:
        st.info(
            _ui_label("No hay datos clasificados (Gold o LLM) para los filtros seleccionados.")
        )
    else:
        # Cuadro resumen de fuentes
        total_msgs = len(df_comb)
        n_gold = (df_comb["fuente"] == "Gold").sum()
        n_llm = (df_comb["fuente"] == "LLM").sum()
        if st.session_state.get("user_role", "admin") == "admin":
            st.caption(
                f"Visualizaciones basadas en **{total_msgs:,}** mensajes clasificados: "
                f"**{n_gold:,}** validados por humanos (Gold) · "
                f"**{n_llm:,}** etiquetados por LLM"
            )

        # 1. Torta: Odio vs No Odio vs Dudoso (paleta semántica unificada)
        pie_data = df_comb["odio_label"].value_counts().reset_index()
        pie_data.columns = ["Clasificación", "Cantidad"]

        col_g1, col_g2 = st.columns(2)

        with col_g1:
            fig_pie = px.pie(
                pie_data, names="Clasificación", values="Cantidad",
                color="Clasificación", color_discrete_map=SEMANTIC_COLORS,
                hole=0.5, title="Distribución Odio vs No Odio",
            )
            fig_pie.update_traces(
                textinfo="percent",
                textposition="inside",
                textfont_size=14,
                textfont_color="white",
                marker=dict(line=dict(color="#FFFFFF", width=2)),
            )
            fig_pie.update_layout(
                height=380,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=-0.15, x=0.5, xanchor="center"),
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # 2. Barras: Odio por plataforma (semánticos + coherente con donut)
        with col_g2:
            plat_data = (
                df_comb.groupby(["plataforma", "odio_label"])
                .size().reset_index(name="Cantidad")
            )
            fig_plat = px.bar(
                plat_data, x="plataforma", y="Cantidad", color="odio_label",
                color_discrete_map=SEMANTIC_COLORS, barmode="group",
                labels={"plataforma": "Plataforma", "odio_label": "Clasificación"},
                title="Distribución de odio por plataforma",
            )
            fig_plat.update_layout(height=380)
            st.plotly_chart(fig_plat, use_container_width=True)

        df_odio = df_comb[df_comb["odio_label"] == "Odio"].copy()

        col_g3, col_g4 = st.columns(2)

        # 3. Distribución de intensidad (paleta coherente)
        with col_g3:
            df_int = df_odio[df_odio["intensidad"].notna()].copy()
            if not df_int.empty:
                df_int["intensidad"] = df_int["intensidad"].astype(int)
                int_data = df_int["intensidad"].value_counts().sort_index().reset_index()
                int_data.columns = ["Intensidad", "Cantidad"]
                int_data["Intensidad"] = int_data["Intensidad"].astype(str)
                fig_int = px.bar(
                    int_data, x="Intensidad", y="Cantidad",
                    color="Intensidad",
                    color_discrete_map=INTENSITY_COLORS,
                    title="Distribución de intensidad",
                    text_auto=True,
                )
                fig_int.update_layout(height=380, showlegend=False)
                st.plotly_chart(
                    fig_int,
                    use_container_width=True,
                    config={
                        "displayModeBar": "hover",
                        "displaylogo": False,
                        "modeBarButtonsToRemove": ["select2d", "lasso2d", "autoScale2d"],
                    },
                )
            else:
                st.info("Sin datos de intensidad.")

        # 4. Distribución de categoría (paleta fija por categoría)
        with col_g4:
            df_cat = df_odio[df_odio["categoria"].notna()].copy()
            if not df_cat.empty:
                df_cat["categoria_label"] = df_cat["categoria"].map(
                    CATEGORIAS_LABELS
                ).fillna(df_cat["categoria"])
                cat_data = (
                    df_cat["categoria_label"].value_counts()
                    .reset_index()
                )
                cat_data.columns = ["Categoría", "Cantidad"]
                df_categoria = cat_data.sort_values("Cantidad", ascending=True)
                fig_cat = px.bar(
                    df_categoria, x="Cantidad", y="Categoría", orientation="h",
                    color="Categoría",
                    color_discrete_map=CAT_COLOR_MAP,
                    title="Distribución por categoría de odio",
                    text_auto=True,
                )
                fig_cat.update_layout(
                    height=380, showlegend=False,
                    yaxis=dict(autorange="reversed"),
                )
                _apply_horizontal_bar_labels(fig_cat)
                st.plotly_chart(fig_cat, use_container_width=True)
            else:
                st.info("Sin datos de categoría.")

        # 5. Intensidad promedio por categoría
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
                title="Intensidad promedio por categoría de odio",
                text_auto=".2f",
            )
            fig_avg.update_layout(
                height=380, yaxis=dict(autorange="reversed"),
                coloraxis_colorbar=dict(title="Intensidad"),
            )
            _apply_horizontal_bar_labels(fig_avg)
            st.plotly_chart(fig_avg, use_container_width=True)

        render_section_exports(
            section_key="panel_general",
            section_title="Panel general",
            csv_items=[
                ("datos_combinados", df_comb),
                ("kpis", pd.DataFrame([kpis])),
            ],
            fig_items=[
                {"title": "Distribución odio/no odio", "fig": fig_pie if "fig_pie" in locals() else None, "kind": "plotly"},
                {"title": "Distribución por plataforma", "fig": fig_plat if "fig_plat" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad del odio", "fig": fig_int if "fig_int" in locals() else None, "kind": "plotly"},
                {"title": "Categorías de odio", "fig": fig_cat if "fig_cat" in locals() else None, "kind": "plotly"},
                {"title": "Intensidad promedio por categoría", "fig": fig_avg if "fig_avg" in locals() else None, "kind": "plotly"},
            ],
        )

    # Tarjeta de rendimiento del modelo: solo admin (no viewer ni editor)
    if st.session_state.get("user_role") == "admin":
        _render_gold_dataset_card()


@st.cache_data(ttl=300)
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

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT
                pm.platform,
                COALESCE(
                    g.y_odio_final,
                    CASE
                        WHEN e.clasificacion_principal = 'ODIO' THEN 'Odio'
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
            LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
            WHERE {where}
        """, conn, params=params)

    if not df.empty:
        df["plataforma"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
        df["intensidad"] = pd.to_numeric(df["intensidad"], errors="coerce")

    return df
