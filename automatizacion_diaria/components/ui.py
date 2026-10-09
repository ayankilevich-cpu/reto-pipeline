"""Helpers de UI compartidos entre secciones del dashboard RETO."""
import base64
import html
import re
from io import BytesIO
from pathlib import Path
from typing import List, Optional, Tuple

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


@st.cache_data(show_spinner=False)
def _logo_data_uri(path_str: str, max_width: int = 480) -> str:
    """Logo como data URI (redimensionado y cacheado).

    st.image sirve las imágenes desde la memoria del servidor (/media/...);
    tras reiniciar la app esas URL dejan de existir y las pestañas abiertas
    (p. ej. el iframe de ciedes.es) muestran el logo roto. El data URI viaja
    dentro de la página y no depende del servidor.
    """
    from PIL import Image

    with Image.open(path_str) as im:
        im.load()
        if im.width > max_width:
            im = im.resize((max_width, round(im.height * max_width / im.width)), Image.LANCZOS)
        buf = BytesIO()
        im.save(buf, format="PNG", optimize=True)
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode("ascii")


def _logo_img_html(path: Path, alt: str, width_css: str = "100%") -> str:
    """<img> con el logo incrustado (data URI)."""
    return (
        f'<img src="{_logo_data_uri(str(path))}" alt="{html.escape(alt)}" '
        f'style="width:{width_css};max-width:100%;height:auto;display:block;margin:0 auto;">'
    )


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


def _render_pg_kpi_grid(
    cards: List[Tuple[str, str, str]],
    *,
    secondary: bool = False,
) -> None:
    """Renderiza KPIs institucionales como grid responsive de tarjetas navy."""
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
        f'<div class="pg-kpi-grid" style="--pg-cols:{len(cards)};">'
        f'{"".join(cards_html)}</div>',
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


def _anonimizar_texto_mensaje(texto: str) -> str:
    """Elimina menciones (@usuario) y URLs del texto del mensaje."""
    texto = re.sub(r'@\S+', '', texto)
    texto = re.sub(r'http\S+', '', texto)
    texto = re.sub(r'\s+', ' ', texto).strip()
    return texto
