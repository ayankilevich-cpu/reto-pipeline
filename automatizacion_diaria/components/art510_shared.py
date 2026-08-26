"""Renderers compartidos de validación humana Art. 510 (Anotación + Art. 510)."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import List
import html
import pandas as pd
import plotly.express as px
import streamlit as st
import streamlit.components.v1 as components

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import APARTADO_LABELS, ART510_COLORS, COLORS
from components.db_helpers import load_art510_validaciones_humanas


def _art510_escape(s) -> str:
    return html.escape(str(s) if s is not None and not (isinstance(s, float) and pd.isna(s)) else "")


def _render_art510_validacion_hscroll(cards_inner_html: str) -> None:
    """Carril horizontal con tarjetas (sin scroll vertical del listado principal)."""
    components.html(
        f"""
        <style>
        .art510-hscroll-wrap {{
            overflow-x: auto;
            overflow-y: hidden;
            -webkit-overflow-scrolling: touch;
            width: 100%;
            padding: 4px 2px 12px 2px;
        }}
        .art510-hscroll-inner {{
            display: flex;
            flex-direction: row;
            gap: 14px;
            align-items: stretch;
        }}
        .art510-card {{
            flex: 0 0 min(340px, 88vw);
            max-width: 360px;
            min-width: 260px;
            border: 1px solid #cbd5e0;
            border-radius: 10px;
            padding: 12px;
            background: #f8fafc;
            font-family: system-ui, -apple-system, sans-serif;
            font-size: 13px;
            color: #1a202c;
            line-height: 1.35;
        }}
        .art510-card blockquote {{
            margin: 0 0 10px 0;
            padding: 8px 10px;
            background: #fff;
            border-left: 3px solid #2b6cb0;
            font-size: 12px;
            white-space: pre-wrap;
            word-break: break-word;
            max-height: 220px;
            overflow-y: auto;
        }}
        .art510-card .meta {{ margin: 4px 0; font-size: 12px; }}
        .art510-card details {{ margin-top: 8px; font-size: 11px; color: #4a5568; }}
        .art510-badge {{
            display: inline-block;
            padding: 2px 8px;
            border-radius: 6px;
            font-size: 11px;
            font-weight: 600;
            margin-bottom: 6px;
            background: #e2e8f0;
        }}
        </style>
        <div class="art510-hscroll-wrap"><div class="art510-hscroll-inner">
        {cards_inner_html}
        </div></div>
        """,
        height=540,
        scrolling=False,
    )


def _render_art510_validacion_humana(summary: dict):
    """Sub-sección: resultado de validaciones humanas Art. 510."""
    total_val = summary.get("total_validados", 0)
    if total_val == 0:
        return

    st.markdown("---")
    st.markdown("### Validación humana")
    st.markdown(
        "Resultado de la revisión manual por expertos de los mensajes "
        "pre-seleccionados por el LLM como potenciales delitos Art. 510."
    )

    confirmados = summary.get("total_confirmados", 0)
    rechazados = summary.get("total_rechazados", 0)
    tasa_precision = (confirmados / total_val * 100) if total_val else 0

    v1, v2, v3, v4 = st.columns(4)
    v1.metric("Revisados por humano", f"{total_val}")
    v2.metric("Confirmados como delito", f"{confirmados}")
    v3.metric("Rechazados", f"{rechazados}")
    v4.metric("Precisión del LLM", f"{tasa_precision:.0f}%")

    df_vh = load_art510_validaciones_humanas()
    if df_vh.empty:
        return

    _dec_lab = {
        "confirmado": "Confirmado",
        "rechazado": "Rechazado",
        "corregido": "Corregido",
    }
    st.markdown("#### Resumen visual")
    g1, g2 = st.columns(2)
    with g1:
        vc = df_vh["validacion_humana"].value_counts().reset_index()
        vc.columns = ["decision", "Cantidad"]
        vc["Decisión"] = vc["decision"].map(lambda x: _dec_lab.get(str(x), str(x)))
        fig_d = px.pie(
            vc, names="Decisión", values="Cantidad",
            title="Distribución de decisiones humanas",
            hole=0.35,
            color="Decisión",
            color_discrete_map={
                "Confirmado": COLORS["danger"],
                "Rechazado": COLORS["muted"],
                "Corregido": COLORS["warning"],
            },
        )
        fig_d.update_layout(height=360)
        st.plotly_chart(fig_d, use_container_width=True, theme=None, key="art510_vh_pie_decisiones")
    with g2:
        df_ap = df_vh[df_vh["validacion_humana"].isin(["confirmado", "corregido"])].copy()
        df_ap = df_ap[df_ap["apartado_510_final"].notna() & (df_ap["apartado_510_final"].astype(str) != "")]
        if df_ap.empty:
            st.info("Sin apartado humano registrado para confirmados/corregidos.")
        else:
            df_ap["Apartado"] = df_ap["apartado_510_final"].map(
                lambda x: APARTADO_LABELS.get(x, x) if pd.notna(x) else "—"
            )
            ap_c = df_ap["Apartado"].value_counts().reset_index()
            ap_c.columns = ["Apartado", "Cantidad"]
            fig_ap = px.bar(
                ap_c, x="Apartado", y="Cantidad",
                title="Apartado Art. 510.1 (decisión humana)",
                color="Apartado",
                color_discrete_map={
                    APARTADO_LABELS["1a"]: ART510_COLORS["1a"],
                    APARTADO_LABELS["1b"]: ART510_COLORS["1b"],
                    APARTADO_LABELS["1c"]: ART510_COLORS["1c"],
                },
            )
            fig_ap.update_layout(height=360, showlegend=False)
            st.plotly_chart(fig_ap, use_container_width=True, theme=None, key="art510_vh_bar_apartado")

    st.markdown("---")
    tab_conf, tab_rech, tab_all = st.tabs([
        f"Confirmados ({confirmados})",
        f"Rechazados ({rechazados})",
        "Todos",
    ])

    with tab_conf:
        df_c = df_vh[df_vh["validacion_humana"] == "confirmado"]
        if df_c.empty:
            st.info("No hay mensajes confirmados como delito.")
        else:
            st.caption("Desplazá horizontalmente para ver todas las tarjetas.")
            parts_conf: List[str] = []
            for _, r in df_c.iterrows():
                cond = r.get("conducta_final")
                com = r.get("comentario")
                just = r.get("llm_justificacion")
                extra_cond = (
                    f'<div class="meta"><b>Conducta:</b> {_art510_escape(cond)}</div>'
                    if cond and str(cond).strip() else ""
                )
                extra_com = (
                    f'<div class="meta"><b>Comentario:</b> {_art510_escape(com)}</div>'
                    if com and str(com).strip() else ""
                )
                extra_just = (
                    f"<p>Justificación LLM: {_art510_escape(just)}</p>"
                    if just and str(just).strip() else ""
                )
                parts_conf.append(
                    "<div class=\"art510-card\">"
                    f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                    f"<div class=\"meta\"><b>Apartado:</b> {_art510_escape(r.get('apartado_510_final')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Grupo protegido:</b> {_art510_escape(r.get('grupo_protegido_final')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                    f"{extra_cond}{extra_com}"
                    "<details><summary>Comparar con evaluación LLM</summary>"
                    f"<p>Apartado LLM: <code>{_art510_escape(r.get('llm_apartado')) or '—'}</code></p>"
                    f"<p>Grupo LLM: <code>{_art510_escape(r.get('llm_grupo')) or '—'}</code></p>"
                    f"<p>Confianza LLM: <code>{_art510_escape(r.get('llm_confianza')) or '—'}</code></p>"
                    f"{extra_just}"
                    "</details></div>"
                )
            _render_art510_validacion_hscroll("".join(parts_conf))

    with tab_rech:
        df_r = df_vh[df_vh["validacion_humana"] == "rechazado"]
        if df_r.empty:
            st.info("No hay mensajes rechazados.")
        else:
            st.caption(
                "Estos mensajes fueron evaluados como potencial delito por el LLM "
                "pero descartados por el experto humano. Desplazá horizontalmente para ver todas las tarjetas."
            )
            parts_rech: List[str] = []
            for _, r in df_r.iterrows():
                parts_rech.append(
                    "<div class=\"art510-card\">"
                    f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                    f"<div class=\"meta\"><b>Confianza LLM:</b> {_art510_escape(r.get('llm_confianza')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Grupo (LLM):</b> {_art510_escape(r.get('llm_grupo')) or '—'}</div>"
                    f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                    "</div>"
                )
            _render_art510_validacion_hscroll("".join(parts_rech))

    with tab_all:
        st.caption(
            "Vista completa: desplazá horizontalmente. El texto largo de cada mensaje se puede "
            "revisar con scroll **dentro** de la tarjeta."
        )
        _dec_badge = {
            "confirmado": "Confirmado",
            "rechazado": "Rechazado",
            "corregido": "Corregido",
        }
        parts_all: List[str] = []
        for _, r in df_vh.iterrows():
            vh = str(r.get("validacion_humana") or "")
            badge = _dec_badge.get(vh, _art510_escape(vh) or "—")
            cond = r.get("conducta_final")
            com = r.get("comentario")
            extra_cond = (
                f'<div class="meta"><b>Conducta (humano):</b> {_art510_escape(cond)}</div>'
                if cond and str(cond).strip() else ""
            )
            extra_com = (
                f'<div class="meta"><b>Comentario:</b> {_art510_escape(com)}</div>'
                if com and str(com).strip() else ""
            )
            parts_all.append(
                "<div class=\"art510-card\">"
                f'<span class="art510-badge">{_art510_escape(badge)}</span>'
                f"<blockquote>{_art510_escape(r.get('content_original'))}</blockquote>"
                f"<div class=\"meta\"><b>Apartado (humano):</b> {_art510_escape(r.get('apartado_510_final')) or '—'}</div>"
                f"<div class=\"meta\"><b>Grupo (humano):</b> {_art510_escape(r.get('grupo_protegido_final')) or '—'}</div>"
                f"{extra_cond}{extra_com}"
                f"<div class=\"meta\"><b>Confianza LLM:</b> {_art510_escape(r.get('llm_confianza')) or '—'}</div>"
                f"<div class=\"meta\"><b>Revisor:</b> {_art510_escape(r.get('annotator_id')) or '—'}</div>"
                "</div>"
            )
        _render_art510_validacion_hscroll("".join(parts_all))
