"""Helpers de exportación CSV / PDF por sección del dashboard RETO."""
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.backends.backend_pdf import PdfPages

try:
    import plotly.io as pio
except Exception:  # pragma: no cover
    pio = None


# ============================================================
# EXPORT HELPERS (CSV / PDF por sección)
# ============================================================
def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    """Serializa un DataFrame a CSV UTF-8 (con BOM para Excel)."""
    return df.to_csv(index=False).encode("utf-8-sig")


def plotly_fig_to_png_bytes(fig) -> Tuple[Optional[bytes], Optional[str]]:
    """
    Convierte una figura Plotly a PNG en memoria.
    Requiere kaleido; si no está disponible devuelve error legible.
    """
    if fig is None:
        return None, "Figura Plotly vacía."
    if pio is None:
        return None, "No se pudo importar plotly.io."
    try:
        png = pio.to_image(fig, format="png", width=1400, height=900, scale=2)
        return png, None
    except Exception as e:
        msg = str(e)
        if "kaleido" in msg.lower() or "chrome" in msg.lower():
            return None, "Exportación Plotly->PNG requiere kaleido."
        return None, f"No se pudo convertir figura Plotly: {type(e).__name__}."


def matplotlib_fig_to_png_bytes(fig) -> Tuple[Optional[bytes], Optional[str]]:
    """Convierte una figura Matplotlib a PNG en memoria."""
    if fig is None:
        return None, "Figura Matplotlib vacía."
    buf = BytesIO()
    try:
        fig.savefig(buf, format="png", dpi=160, bbox_inches="tight")
        buf.seek(0)
        return buf.read(), None
    except Exception as e:
        return None, f"No se pudo convertir figura Matplotlib: {type(e).__name__}."
    finally:
        buf.close()


def build_section_pdf_bytes(
    section_title: str,
    fig_items: List[Dict[str, Any]],
) -> Tuple[Optional[bytes], List[str]]:
    """
    Construye un único PDF de sección con todas las figuras recibidas.

    fig_items: lista de dicts con claves:
      - title: título de página
      - fig: objeto figura
      - kind: "plotly" o "matplotlib"
    """
    errors: List[str] = []
    images: List[Tuple[str, bytes]] = []

    for item in fig_items:
        title = item.get("title", "Gráfico")
        fig = item.get("fig")
        kind = item.get("kind", "plotly")

        if kind == "matplotlib":
            png, err = matplotlib_fig_to_png_bytes(fig)
        else:
            png, err = plotly_fig_to_png_bytes(fig)

        if err:
            errors.append(f"{title}: {err}")
            continue
        if png:
            images.append((title, png))

    if not images:
        return None, errors

    pdf_buf = BytesIO()
    with PdfPages(pdf_buf) as pdf:
        fig_cover = plt.figure(figsize=(11.69, 8.27))
        ax_cover = fig_cover.add_subplot(111)
        ax_cover.axis("off")
        ax_cover.text(0.5, 0.62, "RETO — Exportación de sección", ha="center", va="center", fontsize=20, weight="bold")
        ax_cover.text(0.5, 0.50, section_title, ha="center", va="center", fontsize=16)
        ax_cover.text(0.5, 0.40, datetime.now().strftime("%Y-%m-%d %H:%M"), ha="center", va="center", fontsize=11)
        pdf.savefig(fig_cover, bbox_inches="tight")
        plt.close(fig_cover)

        for title, png in images:
            fig_page = plt.figure(figsize=(11.69, 8.27))
            ax = fig_page.add_subplot(111)
            ax.axis("off")
            ax.set_title(title, fontsize=13, pad=12)
            arr = mpimg.imread(BytesIO(png), format="png")
            ax.imshow(arr)
            pdf.savefig(fig_page, bbox_inches="tight")
            plt.close(fig_page)

    pdf_buf.seek(0)
    return pdf_buf.read(), errors


def render_section_exports(
    section_key: str,
    section_title: str,
    csv_items: List[Tuple[str, pd.DataFrame]],
    fig_items: List[Dict[str, Any]],
) -> None:
    """
    Renderiza botones de descarga CSV y PDF para una sección.
    """
    clean_csv_items: List[Tuple[str, pd.DataFrame]] = []
    for name, df in csv_items:
        if isinstance(df, pd.DataFrame) and not df.empty:
            clean_csv_items.append((name, df))

    clean_fig_items: List[Dict[str, Any]] = [f for f in fig_items if f.get("fig") is not None]

    if not clean_csv_items and not clean_fig_items:
        return

    st.markdown("---")
    st.markdown(
        '<div class="reto-download-panel"><div class="reto-download-panel-title">Descargas</div>',
        unsafe_allow_html=True,
    )

    if clean_csv_items:
        for idx, (name, df) in enumerate(clean_csv_items):
            st.download_button(
                label=f"⬇ Descargar CSV — {name}",
                data=df_to_csv_bytes(df),
                file_name=f"reto_{section_key}_{name}.csv",
                mime="text/csv",
                key=f"dl_csv_{section_key}_{idx}",
                use_container_width=True,
            )

    if clean_fig_items:
        pdf_bytes, pdf_errors = build_section_pdf_bytes(section_title, clean_fig_items)
        if pdf_bytes:
            st.download_button(
                label="⬇ Descargar PDF — gráficos de la sección",
                data=pdf_bytes,
                file_name=f"reto_{section_key}_graficos.pdf",
                mime="application/pdf",
                key=f"dl_pdf_{section_key}",
                use_container_width=True,
            )
        else:
            st.info("No se pudo generar el PDF de gráficos para esta sección.")

        if pdf_errors:
            st.caption("Avisos de exportación PDF: " + " | ".join(pdf_errors[:4]))

    st.markdown("</div>", unsafe_allow_html=True)
