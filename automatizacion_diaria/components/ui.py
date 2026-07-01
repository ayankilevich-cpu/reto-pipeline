"""Helpers de UI compartidos entre secciones del dashboard RETO."""
import html
from pathlib import Path
from typing import Optional

import streamlit as st

_AUTO_DIR = Path(__file__).resolve().parent.parent  # automatizacion_diaria/
# Raíz del paquete ReTo (logo y carpeta logos/ están ahí, no dentro de automatizacion_diaria/)
_RETO_ROOT = _AUTO_DIR.parent


def _reto_asset_file(*parts: str) -> Optional[Path]:
    """Resuelve logo u otro asset: mismo dir del script o raíz ReTo (Streamlit Cloud / distintos entrypoints)."""
    for base in (_AUTO_DIR, _RETO_ROOT):
        p = base.joinpath(*parts)
        if p.is_file():
            return p
    return None


def _require_role(*allowed_roles: str, section: str = "esta sección") -> bool:
    """Guard de acceso: detiene el renderer si el rol no está autorizado.
    Devuelve True si el acceso está permitido, False si no."""
    role = st.session_state.get("user_role")
    if role not in allowed_roles:
        st.error(f"No tenés permisos para acceder a {section}.")
        st.info("Si creés que es un error, iniciá sesión con las credenciales correctas.")
        st.stop()
        return False
    return True


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
