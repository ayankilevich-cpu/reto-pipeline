"""Sección «Panel general» del dashboard (KPIs + estado del pipeline)."""
from __future__ import annotations

import html
import sys
from pathlib import Path
from typing import Optional, Tuple
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import (
    EXCLUDED_SOURCE_MEDIA,
    PLATFORM_DISPLAY,
    SEMANTIC_COLORS,
    _expand_platforms,
    platform_label,
)
from components.ui import (
    _render_section_header,
    _role_can_access_raw,
    _ui_label,
)
from components.exports import render_descargas_menu
from components.aviso_criterio import render_aviso_cambio_criterio
from components.chart_style import (
    NAVY, NEUTRAL, encabezado_grafico, fmt_int, fmt_pct, kpi_tarjetas, linea_actividad,
    nota_fuente, tarjeta,
)
from components.db_helpers import load_filter_options, _pooled_conn, fecha_fin_clasificacion_yt, load_youtube_recorrido


@st.cache_data(ttl=86400, show_spinner="Cargando datos…", max_entries=64)
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
_COLOR_ETAPA = "#8AA4BF"     # etapas de contexto en azul acero
_COLOR_ODIO_CONF = "#A32D2D"
_COLOR_ODIO_IA = "#F09595"
_COLOR_VALIDADOS = NAVY      # validación humana en azul ReTo
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


@st.cache_data(ttl=86400, show_spinner="Cargando datos…", max_entries=64)
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


_RR_CSS = (
    "<style>"
    ".rr-flow{display:grid;grid-template-columns:repeat(5,minmax(0,1fr));margin:18px 0 6px}"
    ".rr-step{position:relative;padding:14px 16px 14px 18px;border-left:3px solid #8AA4BF}"
    ".rr-step+.rr-step{margin-left:26px}"
    ".rr-step+.rr-step::before{content:'';position:absolute;left:-29px;top:30px;width:20px;height:2px;background:#C3CFDC}"
    ".rr-step+.rr-step::after{content:'';position:absolute;left:-14px;top:26px;border:5px solid transparent;border-left:7px solid #C3CFDC}"
    ".rr-l{font-size:13px;color:#5B6675;min-height:2.6em;line-height:1.3}"
    ".rr-v{font-size:28px;font-weight:700;letter-spacing:-.02em;margin-top:4px;white-space:nowrap;color:#14202E;font-variant-numeric:tabular-nums}"
    ".rr-p{font-size:14px;color:#5B6675}"
    ".rr-hate{border-left-color:#A32D2D;background:#FDF5F4;border-radius:0 10px 10px 0}"
    ".rr-hate .rr-v{color:#A32D2D}"
    ".rr-val{border-left-color:#1F4E79}.rr-val .rr-v{color:#1F4E79}"
    ".rr-split{display:flex;height:6px;border-radius:3px;overflow:hidden;margin:8px 0 6px;background:#F09595}"
    ".rr-split b{background:#A32D2D;display:block}"
    ".rr-splitl{font-size:12px;color:#6B4A4A;line-height:1.45}"
    "@media(max-width:1100px){.rr-flow{grid-template-columns:minmax(0,1fr)}"
    ".rr-step+.rr-step{margin-left:0;margin-top:10px}"
    ".rr-step+.rr-step::before,.rr-step+.rr-step::after{display:none}}"
    "</style>"
)


def _recorrido_html(pasos, base: int, conf: int, ia: int) -> str:
    """Genera el recorrido HTML en una sola línea para que Streamlit lo interprete como HTML."""
    partes = []
    for lab, val, cls in pasos:
        extra = ""
        if cls == "hate":
            w = (conf / val * 100) if val else 0
            extra = (
                f"<div class='rr-split'><b style='width:{w:.1f}%'></b></div>"
                f"<div class='rr-splitl'>{fmt_int(conf)} confirmados<br>"
                f"{fmt_int(ia)} según IA, sin validar</div>"
            )
        partes.append(
            f"<div class='rr-step{(' rr-' + cls) if cls else ''}'>"
            f"<div class='rr-l'>{html.escape(lab)}</div>"
            f"<div class='rr-v'>{fmt_int(val)}</div>"
            f"<div class='rr-p'>{fmt_pct(val / base * 100)}</div>{extra}</div>"
        )
    return _RR_CSS + "<div class='rr-flow'>" + "".join(partes) + "</div>"


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
            text=f"<b>{fmt_int(v)}</b>  ·  {fmt_pct(v / base * 100)}",
            font=dict(size=13, color="#1A202C"),
        )
    fig.update_layout(
        barmode="stack", height=330, bargap=0.35,
        legend=dict(orientation="h", yanchor="top", y=-0.05, x=0, xanchor="left"),
        margin=dict(l=10, r=20, t=10, b=40),
    )
    fig.update_xaxes(range=[0, base * 1.25], showticklabels=False, showgrid=False, zeroline=False)
    fig.update_yaxes(autorange="reversed", automargin=True, tickfont=dict(size=12), ticksuffix="  ")
    encabezado_grafico(
        f"El odio identificado supone el {fmt_pct(odio_total / base * 100)} "
        "de los mensajes procesados",
        "Recorrido de los mensajes por cada etapa del sistema. Porcentajes sobre el total procesado.",
    )
    st.markdown(
        _recorrido_html(
            [
                ("Mensajes procesados", t["procesados"], ""),
                ("Candidatos (filtro de términos)", t["candidatos"], ""),
                ("Clasificados (IA + revisión humana)", t["clasificados"], ""),
                ("Odio identificado", odio_total, "hate"),
                ("Validados por revisión humana", t["validados"], "val"),
            ],
            base, t["odio_confirmado"], t["odio_ia"],
        ),
        unsafe_allow_html=True,
    )

    nota = (
        "La IA se aplica a una selección priorizada de candidatos: las cifras de odio son un "
        "mínimo identificado, no una estimación del odio en el total de conversaciones."
    )
    if t["ia_odio_revisados"] >= _MIN_MUESTRA_PRECISION:
        prec = t["ia_odio_confirmados"] / t["ia_odio_revisados"] * 100
        nota += (
            f" En la muestra revisada por personas ({fmt_int(t['ia_odio_revisados'])} mensajes), "
            f"el {prec:.0f} % de lo marcado como odio por la IA fue confirmado."
        )
    st.markdown(
        "<p style='font-size:13.5px;color:#5B6675;max-width:78ch;margin:12px 0 4px;"
        f"border-top:1px solid #EEF1F5;padding-top:12px'>{html.escape(nota)}</p>",
        unsafe_allow_html=True,
    )
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
    dfp = dfp.sort_values("pct_total", ascending=True)

    fig = go.Figure()
    for col_pct, col_n, nombre, color in (
        ("pct_conf", "odio_confirmado", "Confirmado", _COLOR_ODIO_CONF),
        ("pct_ia", "odio_ia", "Según IA", _COLOR_ODIO_IA),
    ):
        fig.add_trace(go.Bar(
            orientation="h", y=dfp["plataforma"], x=dfp[col_pct], name=nombre,
            marker_color=color,
            customdata=[[fmt_pct(p), fmt_int(a), fmt_int(b)]
                        for p, a, b in zip(dfp[col_pct], dfp[col_n], dfp["procesados"])],
            hovertemplate=(f"<b>%{{y}}</b><br>{nombre}: %{{customdata[0]}}<br>"
                           "%{customdata[1]} de %{customdata[2]} mensajes<extra></extra>"),
        ))
    for _, r in dfp.iterrows():
        fig.add_annotation(
            x=r["pct_total"], y=r["plataforma"], xanchor="left", xshift=10, showarrow=False,
            text=f"<b>{fmt_pct(r['pct_total'])}</b>", font=dict(size=16, color="#14202E"),
        )
    fig.update_layout(
        barmode="stack", height=60 * len(dfp) + 50, bargap=0.5, showlegend=False,
        margin=dict(l=8, r=80, t=6, b=6),
        xaxis=dict(visible=False, range=[0, float(dfp["pct_total"].max()) * 1.15], fixedrange=True),
        yaxis=dict(showgrid=False, fixedrange=True, ticksuffix="   ",
                   tickfont=dict(size=15, color="#14202E")),
    )
    _top = dfp.iloc[-1]
    if len(dfp) > 1:
        _tit = (f"{_top['plataforma']} registra la mayor proporción de odio identificado "
                f"({fmt_pct(_top['pct_total'])})")
    else:
        _tit = f"Odio identificado en {_top['plataforma']}: {fmt_pct(_top['pct_total'])}"
    encabezado_grafico(_tit, "% de odio identificado sobre los mensajes procesados de cada plataforma.")
    st.plotly_chart(fig, use_container_width=True, theme=None)
    st.markdown(
        "<div style='display:flex;gap:18px;flex-wrap:wrap;font-size:13px;color:#5B6675;margin:6px 0 4px'>"
        f"<span><span style='display:inline-block;width:11px;height:11px;border-radius:3px;"
        f"background:{_COLOR_ODIO_CONF};margin-right:6px;vertical-align:-1px'></span>"
        "Confirmado por revisión humana</span>"
        f"<span><span style='display:inline-block;width:11px;height:11px;border-radius:3px;"
        f"background:{_COLOR_ODIO_IA};margin-right:6px;vertical-align:-1px'></span>"
        "Según IA, sin validar</span></div>",
        unsafe_allow_html=True,
    )
    if dfp["plataforma"].astype(str).str.lower().str.contains("youtube").any():
        try:
            _yt = load_youtube_recorrido()
            _pct_an = fmt_pct(_yt["analizados"] / _yt["total"] * 100) if _yt["total"] else "—"
            st.markdown(
                "<p style='font-size:13px;color:#7A8699;margin:8px 0 2px;max-width:80ch'>"
                f"En YouTube la IA no clasifica odio desde el {fecha_fin_clasificacion_yt(_yt)}, y su filtro "
                f"de relevancia ha analizado el {_pct_an} de los comentarios. Su porcentaje suma el odio "
                "confirmado por personas y la clasificación anterior a esa fecha, por lo que no es "
                "comparable con X.</p>",
                unsafe_allow_html=True,
            )
        except Exception:
            pass
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
@st.cache_data(ttl=86400, max_entries=64)
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


@st.cache_data(ttl=86400, max_entries=64)
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
    """Rendimiento de la IA frente a la revisión humana (solo admin)."""
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

    def _etiqueta(txt: str) -> None:
        st.markdown(
            f"<p style='font-size:13px;font-weight:600;color:#5B6675;margin:18px 0 8px'>{html.escape(txt)}</p>",
            unsafe_allow_html=True,
        )

    with tarjeta():
        encabezado_grafico(
            "Rendimiento de la IA frente a la revisión humana",
            "Solo visible para administración. Gold Dataset: mensajes validados por personas.",
        )

        _etiqueta("Gold Dataset")
        kpi_tarjetas([
            ("Mensajes validados (Gold)", fmt_int(g["total_gold"]), ""),
            ("Con etiqueta previa de IA", fmt_int(g["total_llm"]), ""),
            ("Última validación", fecha_str, ""),
        ])

        _etiqueta(f"Calidad del etiquetado de la IA · sobre {fmt_int(g['total_llm'])} mensajes revisados")
        kpi_tarjetas([
            ("Concordancia IA–humano", fmt_pct(g["pct_concordancia_llm"]), ""),
            ("Corrección de odio", fmt_pct(g["pct_corrigio_odio"]), ""),
            ("Corrección de categoría", fmt_pct(g["pct_corrigio_categoria"]), ""),
            ("Corrección de intensidad", fmt_pct(g["pct_corrigio_intensidad"]), ""),
        ], destacar_primero=False)
        st.markdown(
            "<p style='font-size:13px;color:#7A8699;margin:8px 0 0'>"
            f"Corrección de intensidad sobre {fmt_int(g['total_con_intensidad'])} mensajes con intensidad registrada.</p>",
            unsafe_allow_html=True,
        )

        try:
            dfm = load_matriz_confusion()
        except Exception:
            dfm = pd.DataFrame()
        if dfm.empty:
            st.info("Sin mensajes con etiqueta de IA y revisión humana.")
            return

        orden = ["Odio", "No Odio", "Dudoso"]
        piv = (
            dfm.pivot_table(index="llm", columns="humano", values="n", aggfunc="sum", fill_value=0)
            .reindex(index=orden, columns=orden, fill_value=0)
        )
        total_m = int(piv.values.sum())
        diag = int(sum(piv.loc[c, c] for c in orden))
        tp = int(piv.loc["Odio", "Odio"])
        llm_odio = int(piv.loc["Odio"].sum())
        hum_odio = int(piv["Odio"].sum())

        _etiqueta(f"Matriz de confusión · {fmt_int(total_m)} mensajes con etiqueta de IA y revisión humana")
        kpi_tarjetas([
            ("Concordancia global", fmt_pct(diag / total_m * 100 if total_m else None), ""),
            ("Precisión de la IA en odio", fmt_pct(tp / llm_odio * 100 if llm_odio else None), ""),
            ("Recall de la IA en odio", fmt_pct(tp / hum_odio * 100 if hum_odio else None), ""),
        ], destacar_primero=False)

        _max = max(int(piv.values.max()), 1)
        _th = "text-align:right;padding:8px 14px;font-size:13px;color:#5B6675;font-weight:600"
        cabecera = (
            "<tr><th style='text-align:left;padding:8px 14px;font-size:12px;color:#7A8699;font-weight:500'>"
            "IA ↓ · Humano →</th>"
            + "".join(f"<th style='{_th}'>{c}</th>" for c in orden)
            + f"<th style='{_th}'>Total</th></tr>"
        )
        filas = []
        for r in orden:
            celdas = []
            for c in orden:
                v = int(piv.loc[r, c])
                a = 0.08 + 0.55 * v / _max
                bg = f"rgba(31,78,121,{a:.2f})" if r == c else f"rgba(163,45,45,{a:.2f})"
                fg = "#FFFFFF" if a > 0.4 else "#14202E"
                celdas.append(
                    f"<td style='background:{bg};color:{fg};text-align:right;padding:10px 14px;"
                    f"font-weight:600;border-radius:6px'>{fmt_int(v)}</td>"
                )
            filas.append(
                f"<tr><th style='text-align:left;padding:10px 14px;font-weight:600;color:#14202E'>{r}</th>"
                + "".join(celdas)
                + f"<td style='text-align:right;padding:10px 14px;color:#5B6675'>{fmt_int(int(piv.loc[r].sum()))}</td></tr>"
            )
        st.markdown(
            "<table style='border-collapse:separate;border-spacing:4px;font-size:14.5px;"
            "font-variant-numeric:tabular-nums;margin-top:14px'>"
            + cabecera + "".join(filas) + "</table>",
            unsafe_allow_html=True,
        )
        st.markdown(
            "<p style='font-size:13px;color:#7A8699;margin:10px 0 4px;max-width:90ch'>"
            "Azul: coincidencias (diagonal); rojo: discrepancias. Precisión: de lo que la IA marcó como odio, "
            "cuánto confirmó la revisión humana. Recall: del odio confirmado por personas, cuánto había "
            "detectado la IA. Incluye todo el Gold, no solo los mensajes revisados tras la IA.</p>",
            unsafe_allow_html=True,
        )


def render_panel_general():
    _col_tit, _col_dl = st.columns([6, 1])
    with _col_tit:
        _render_section_header(
            "Panel general",
            "Indicadores clave del proyecto ReTo: volumen analizado, clasificación y validación humana.",
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

    kpi_tarjetas([
        (label_raw, fmt_int(mensajes_totales), ""),
        ("Candidatos a odio", fmt_int(candidatos_odio), ""),
        (label_llm, fmt_int(etiquetados_llm), ""),
        ("Mensajes validados", fmt_int(mensajes_validados),
         f"{fmt_int(mensajes_odio)} confirmados como odio"),
        ("Medios monitorizados", fmt_int(medios_monitorizados), ""),
    ])
    _act = (f"Ayer se incorporaron {fmt_int(nuevos_x_ayer)} mensajes de X y "
            f"{fmt_int(nuevos_yt_ayer)} de YouTube.")
    if (nuevos_x_hoy or 0) + (nuevos_yt_hoy or 0) > 0:
        _act += (f" Hoy, por ahora, {fmt_int(nuevos_x_hoy)} de X y "
                 f"{fmt_int(nuevos_yt_hoy)} de YouTube.")
    linea_actividad(_act + " Datos actualizados cada día.")

    _plat_t = tuple(sel_platforms) if sel_platforms else None
    _med_t = tuple(sel_medios) if sel_medios else None

    # --- 1. Embudo de detección (base: mensajes procesados) ---
    fig_embudo = fig_plat = fig_int = None
    df_emb = load_embudo(platforms=_plat_t, medios=_med_t)
    if df_emb.empty or int(df_emb["procesados"].sum()) == 0:
        st.info("No hay mensajes procesados para los filtros seleccionados.")
    else:
        with tarjeta():
            fig_embudo = _render_embudo(df_emb)

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

        if st.session_state.get("user_role", "admin") == "admin":
            n_gold_odio = int((df_odio["fuente"] == "Gold").sum())
            n_llm_odio = int((df_odio["fuente"] == "LLM").sum())
            st.caption(
                f"Intensidad basada en **{_fmt_int(n_odio)}** mensajes de odio: "
                f"**{_fmt_int(n_gold_odio)}** validados por humanos (Gold) · "
                f"**{_fmt_int(n_llm_odio)}** etiquetados por IA"
            )

        col_g1, col_g2 = st.columns(2)

        # 2. Odio identificado por plataforma (% sobre procesados de cada plataforma)
        with col_g1:
            if not df_emb.empty:
                with tarjeta():
                    fig_plat = _render_odio_por_plataforma(df_emb)

        # 3. Distribución de intensidad (barra única al 100 %)
        with col_g2:
            df_int = df_odio[df_odio["intensidad"].notna()].copy()
            if not df_int.empty:
                df_int["intensidad"] = df_int["intensidad"].astype(int).astype(str)
                int_data = (
                    df_int["intensidad"].value_counts()
                    .reindex(["1", "2", "3"], fill_value=0)
                    .rename_axis("Intensidad").reset_index(name="Cantidad")
                )
                _n_tot = int(int_data["Cantidad"].sum())
                int_data["Porcentaje"] = int_data["Cantidad"] / (_n_tot or 1) * 100
                _INT_LABEL = {"1": "Baja (1)", "2": "Media (2)", "3": "Alta (3)"}
                _INT_COLOR = {"1": "#F4C7C3", "2": "#E0726A", "3": "#A32D2D"}
                _pct_alta = float(int_data.loc[int_data["Intensidad"] == "3", "Porcentaje"].sum())
                _media_int = (
                    sum(int(k) * int(c) for k, c in zip(int_data["Intensidad"], int_data["Cantidad"])) / _n_tot
                    if _n_tot else 0.0
                )

                fig_int = go.Figure()
                for _, r in int_data.iterrows():
                    k = r["Intensidad"]
                    fig_int.add_trace(go.Bar(
                        orientation="h", y=[""], x=[r["Porcentaje"]], name=_INT_LABEL[k],
                        marker_color=_INT_COLOR[k],
                        customdata=[[fmt_int(r["Cantidad"]), fmt_pct(r["Porcentaje"])]],
                        hovertemplate=(f"Nivel {_INT_LABEL[k].lower()}: %{{customdata[1]}}"
                                       "<br>%{customdata[0]} mensajes<extra></extra>"),
                    ))
                fig_int.update_layout(
                    barmode="stack", height=60, showlegend=False,
                    margin=dict(l=0, r=0, t=4, b=4),
                    xaxis=dict(visible=False, range=[0, 100], fixedrange=True),
                    yaxis=dict(visible=False, fixedrange=True),
                )
                _filas = "".join(
                    "<div style='display:grid;grid-template-columns:18px 96px 72px 1fr;"
                    "align-items:center;font-size:14.5px;color:#14202E'>"
                    f"<span style='width:11px;height:11px;border-radius:3px;background:{_INT_COLOR[k]};"
                    "display:inline-block'></span>"
                    f"<span>{_INT_LABEL[k]}</span>"
                    f"<span style='font-weight:700;text-align:right;padding-right:16px'>{fmt_pct(p)}</span>"
                    f"<span style='font-size:13px;color:#7A8699'>{fmt_int(c)} mensajes</span></div>"
                    for k, p, c in zip(int_data["Intensidad"], int_data["Porcentaje"], int_data["Cantidad"])
                )
                _media_txt = f"{_media_int:.2f}".replace(".", ",")
                with tarjeta():
                    encabezado_grafico(
                        f"El {fmt_pct(_pct_alta)} del odio identificado es de intensidad alta",
                        f"Entre los {fmt_int(n_odio)} mensajes clasificados como odio.",
                    )
                    st.plotly_chart(fig_int, use_container_width=True, theme=None)
                    st.markdown(
                        "<div style='display:grid;gap:10px;margin:6px 0 0'>" + _filas + "</div>"
                        "<div style='display:flex;align-items:baseline;gap:8px;margin:16px 0 6px;"
                        "padding-top:14px;border-top:1px solid #EEF1F5;font-size:14px;color:#5B6675'>"
                        f"<strong style='font-size:22px;color:#14202E'>{_media_txt}</strong>"
                        "<span>intensidad promedio en una escala de 1 (baja) a 3 (alta)</span></div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Sin datos de intensidad.")

        nota_fuente()
        with _col_dl:
            render_descargas_menu(
                section_key="panel_general",
                section_title="Panel general",
                csv_items=[
                    ("embudo", df_emb),
                    ("datos_combinados", df_comb),
                    ("kpis", pd.DataFrame([kpis])),
                ],
                fig_items=[
                    {"title": "Recorrido de los mensajes", "fig": fig_embudo, "kind": "plotly"},
                    {"title": "Odio identificado por plataforma", "fig": fig_plat, "kind": "plotly"},
                    {"title": "Intensidad del odio", "fig": fig_int, "kind": "plotly"},
                ],
            )

    # Tarjeta de rendimiento del modelo: solo admin (no viewer ni editor)
    if st.session_state.get("user_role") == "admin":
        _render_gold_dataset_card()


@st.cache_data(ttl=86400, show_spinner="Cargando datos…", max_entries=64)
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
