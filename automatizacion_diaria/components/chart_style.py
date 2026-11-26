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
        f"<p style='margin:2px 0 4px;font-size:13px;color:{MUTED}'>"
        f"{html.escape(subtitulo)}</p>" if subtitulo else ""
    )
    st.markdown(
        f"<div style='margin-top:6px'>"
        f"<p style='margin:0;font-size:18px;font-weight:600;line-height:1.3;color:{INK}'>"
        f"{html.escape(titulo)}</p>{sub}</div>",
        unsafe_allow_html=True,
    )


def nota_fuente(texto: str = FUENTE_RETO) -> None:
    st.markdown(
        f"<p style='margin:-4px 0 12px;font-size:12px;color:{SUBTLE}'>"
        f"{html.escape(texto)}</p>",
        unsafe_allow_html=True,
    )


def tarjeta():
    """Contenedor con borde institucional (card)."""
    return st.container(border=True)


def kpi_tarjetas(items, destacar_primero: bool = True) -> None:
    """KPIs en tarjetas blancas. items: lista de (etiqueta, valor_f, detalle_o_vacío)."""
    cards = []
    for i, (lab, val, det) in enumerate(items):
        hi = destacar_primero and i == 0
        det_html = (
            f"<div style='font-size:13px;color:{ACCENT};margin-top:2px;font-weight:500'>"
            f"{html.escape(det)}</div>" if det else ""
        )
        cards.append(
            f"<div style='border:1px solid {'#C9D8E8' if hi else '#E3E8EF'};"
            f"background:{'#F7FAFD' if hi else '#FFFFFF'};border-radius:12px;padding:16px 18px'>"
            f"<div style='font-size:13px;color:{MUTED}'>{html.escape(lab)}</div>"
            f"<div style='font-size:30px;font-weight:700;letter-spacing:-0.02em;margin-top:4px;"
            f"color:{NAVY if hi else INK};font-variant-numeric:tabular-nums;white-space:nowrap'>"
            f"{html.escape(val)}</div>{det_html}</div>"
        )
    st.markdown(
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(170px,1fr));gap:14px'>"
        + "".join(cards) + "</div>",
        unsafe_allow_html=True,
    )


def linea_actividad(texto: str) -> None:
    """Frase de actividad reciente con punto verde."""
    st.markdown(
        f"<p style='display:flex;align-items:center;gap:8px;font-size:14px;color:{MUTED};margin:12px 0 26px'>"
        "<span style='width:8px;height:8px;border-radius:50%;background:#2F855A;flex:none;"
        "display:inline-block'></span>"
        f"<span>{html.escape(texto)}</span></p>",
        unsafe_allow_html=True,
    )
