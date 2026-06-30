"""Helpers de UI compartidos entre secciones del dashboard RETO."""
import html

import streamlit as st


def _render_section_header(title: str, subtitle_html: str = "") -> None:
    """Cabecera de sección unificada (barra lateral, tipografía global). `subtitle_html` es HTML fijo en código."""
    sub = (
        f'<div class="subtitle">{subtitle_html}</div>'
        if (subtitle_html and subtitle_html.strip())
        else ""
    )
    st.markdown(
        f'<div class="reto-section-header"><h1>{html.escape(title)}</h1>{sub}</div>',
        unsafe_allow_html=True,
    )


def _apply_horizontal_bar_labels(fig):
    """Etiquetas fuera de barras cortas en gráficos horizontales."""
    fig.update_traces(
        textposition="outside",
        cliponaxis=False,
        textfont_size=11,
    )
    fig.update_layout(margin=dict(r=70))
    return fig


def _role_can_access_raw() -> bool:
    """Solo admin/editor consultan schema raw; viewer y HF público usan processed."""
    return st.session_state.get("user_role") in ("admin", "editor")


def _is_viewer() -> bool:
    return st.session_state.get("user_role") == "viewer"


def _ui_label(text: str) -> str:
    """Texto visible al usuario: el perfil viewer ve IA en lugar de LLM."""
    if not _is_viewer():
        return text
    if "Categorías de odio (LLM)" in text:
        text = text.replace("Categorías de odio (LLM)", "Categorías de odio por IA")
    return text.replace("LLM", "IA")
