"""Estilo institucional de gráficos (opción B): helpers editoriales reutilizables."""
from __future__ import annotations

import html
from typing import Iterable, List, Optional

import streamlit as st

INK = "#14202E"
MUTED = "#5B6675"
SUBTLE = "#7A8699"
NEUTRAL = "#C9D1DB"       # barras sin destacar
ACCENT = "#C0392B"        # rojo ReTo: solo para lo que importa
NAVY = "#1F4E79"          # referencias (medias, umbrales)
FUENTE_RETO = "Fuente: ReTo · Fundación CIEDES."


def fmt_int(n) -> str:
    """Entero con punto de miles (siempre, también 4 cifras)."""
    try:
        return f"{int(n):,}".replace(",", ".")
    except (TypeError, ValueError):
        return "—"


def fmt_pct(v, dec: int = 1) -> str:
    try:
        return f"{float(v):.{dec}f}".replace(".", ",") + " %"
    except (TypeError, ValueError):
        return "—"


def etiqueta_doble(principal: str, detalle: str) -> str:
    """Tick de eje en dos líneas: nombre en negrita y un dato secundario en gris."""
    return (
        f"<b>{html.escape(str(principal))}</b><br>"
        f"<span style='color:{SUBTLE};font-size:11px'>{html.escape(str(detalle))}</span>"
    )


def colores_destacados(claves: Iterable, destacar: Iterable,
                       color: str = ACCENT, base: str = NEUTRAL) -> List[str]:
    destacar = set(destacar)
    return [color if k in destacar else base for k in claves]


def encabezado_grafico(titulo: str, subtitulo: Optional[str] = None) -> None:
    """Título-conclusión + subtítulo encima del gráfico (HTML: hace salto de línea solo)."""
    sub = (
        f"<p style='margin:2px 0 6px;font-size:14px;color:{MUTED};max-width:75ch'>"
        f"{html.escape(subtitulo)}</p>" if subtitulo else ""
    )
    st.markdown(
        f"<div style='margin-top:6px'>"
        f"<p style='margin:0;font-size:19px;font-weight:600;line-height:1.3;color:{INK}'>"
        f"{html.escape(titulo)}</p>{sub}</div>",
        unsafe_allow_html=True,
    )


def nota_fuente(texto: str = FUENTE_RETO) -> None:
    st.markdown(
        f"<p style='margin:-4px 0 12px;font-size:12px;color:{SUBTLE};"
        f"border-top:1px solid #EEF1F5;padding-top:6px'>{html.escape(texto)}</p>",
        unsafe_allow_html=True,
    )
