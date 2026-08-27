"""Sección «Anotación y validación» del dashboard (editor + admin)."""
from __future__ import annotations

import sys
from pathlib import Path
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple
import re
from contextlib import contextmanager
import pandas as pd
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import get_conn
from components.constants import (
    APARTADO_LABELS,
    CATEGORIAS_LABELS,
    LABEL_SOURCE_LABELS,
    platform_label,
)
from components.ui import _render_section_header, _require_role
from components.db_helpers import (
    _load_vllm_yt_corrections,
    _public_medio_label,
    load_art510_candidates,
    load_art510_summary,
)
from components.art510_shared import _render_art510_validacion_humana
from components.validacion_shared import (
    _render_vllm_label_error_analysis,
    _render_vllm_yt_error_analysis,
)

# ============================================================
# CSS específico de los formularios de anotación / validación
# (4 subsecciones: odio · categoría · intensidad · humor)
# Inyección 1 vez por sesión; los selectores están scopeados con clases
# propias y con _ann_styled_box para no afectar otros widgets.
# ============================================================
_ANN_FORM_CSS = """
<style>
.ann-form-title {
    font-family: 'Inter', sans-serif;
    color: #1B3A6B;
    font-weight: 700;
    font-size: 1.05rem;
    margin: 0.25rem 0 0.15rem 0;
}
.ann-form-subtitle {
    color: #5A6675;
    font-size: 0.85rem;
    margin: 0 0 0.85rem 0;
}
.ann-step-header {
    color: #1B3A6B;
    font-weight: 600;
    font-size: 0.95rem;
    margin: 0.6rem 0 0.35rem 0;
    display: flex;
    align-items: center;
    gap: 0.45rem;
}
.ann-step-num {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 22px;
    height: 22px;
    background: #1B3A6B;
    color: white;
    border-radius: 50%;
    font-size: 0.78rem;
    font-weight: 700;
    flex-shrink: 0;
}
.ann-step-desc {
    color: #5A6675;
    font-size: 0.83rem;
    margin: 0 0 0.5rem 30px;
}
.ann-cond-banner {
    background: #EEF4FB;
    border-left: 4px solid #1B3A6B;
    border-radius: 6px;
    padding: 0.55rem 0.85rem;
    margin: 1rem 0 0.6rem 0;
    color: #1B3A6B;
    font-size: 0.82rem;
    font-weight: 500;
}
/* Encabezado de paso justo encima del bloque gris (fuera del container scopeado) */
.ann-step-header--standalone {
    margin: 0.35rem 0 0.65rem 0;
}
.ann-humor-hint {
    color: #5A6675;
    font-size: 0.82rem;
    margin: 0.15rem 0 0.35rem 30px;
}
</style>
"""


# CSS scopeado (vía _ann_styled_box) para los 3 sub-bloques visuales
_ANN_CHIPS_CSS = """
div[role="radiogroup"] {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.5rem;
    width: 100%;
    box-sizing: border-box;
}
div[role="radiogroup"] > label {
    min-width: 0;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    padding: 0.65rem 0.45rem;
    border-radius: 8px;
    border: 1.5px solid #CBD5E0;
    background: #FFFFFF;
    cursor: pointer;
    transition: all 0.15s ease;
    margin: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
}
div[role="radiogroup"] > label > div:first-child { display: none; }
div[role="radiogroup"] > label > div:not(:first-child) {
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
}
div[role="radiogroup"] > label p {
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    font-size: 0.85rem;
    margin: 0 auto;
    text-align: center;
    width: 100%;
    display: block;
    line-height: 1.25;
    word-break: break-word;
    hyphens: auto;
}
div[role="radiogroup"] > label:nth-child(1) { border-color: #C0392B; color: #C0392B; }
div[role="radiogroup"] > label:nth-child(2) { border-color: #2F855A; color: #2F855A; }
div[role="radiogroup"] > label:nth-child(3) { border-color: #B7791F; color: #B7791F; }
div[role="radiogroup"] > label:nth-child(1):hover { background: #FEE2E2; }
div[role="radiogroup"] > label:nth-child(2):hover { background: #DCFCE7; }
div[role="radiogroup"] > label:nth-child(3):hover { background: #FEF3C7; }
div[role="radiogroup"] > label:nth-child(1):has(input:checked) {
    background: #C0392B; border-color: #C0392B;
}
div[role="radiogroup"] > label:nth-child(2):has(input:checked) {
    background: #2F855A; border-color: #2F855A;
}
div[role="radiogroup"] > label:nth-child(3):has(input:checked) {
    background: #B7791F; border-color: #B7791F;
}
div[role="radiogroup"] > label:has(input:checked) p { color: #FFFFFF; }
"""


# Chips de intensidad (mismo patrón que odio/no/dudoso; colores leve→hostil)
_ANN_INTENSITY_CHIPS_CSS = """
div[role="radiogroup"] {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 0.5rem;
    width: 100%;
    box-sizing: border-box;
}
div[role="radiogroup"] > label {
    min-width: 0;
    width: 100%;
    max-width: 100%;
    box-sizing: border-box;
    padding: 0.7rem 0.4rem;
    border-radius: 8px;
    border: 1.5px solid #CBD5E0;
    background: #FFFFFF;
    cursor: pointer;
    transition: all 0.15s ease;
    margin: 0 !important;
    display: flex;
    align-items: center;
    justify-content: center;
    text-align: center;
}
div[role="radiogroup"] > label > div:first-child { display: none; }
div[role="radiogroup"] > label > div:not(:first-child) {
    width: 100%;
    display: flex;
    justify-content: center;
    align-items: center;
    text-align: center;
}
div[role="radiogroup"] > label p {
    font-weight: 600;
    text-transform: none;
    letter-spacing: 0.02em;
    font-size: 0.8rem;
    margin: 0 auto;
    text-align: center;
    width: 100%;
    display: block;
    line-height: 1.3;
    word-break: break-word;
    hyphens: auto;
}
div[role="radiogroup"] > label:nth-child(1) { border-color: #D97706; color: #B45309; }
div[role="radiogroup"] > label:nth-child(2) { border-color: #EA580C; color: #C2410C; }
div[role="radiogroup"] > label:nth-child(3) { border-color: #C0392B; color: #991B1B; }
div[role="radiogroup"] > label:nth-child(1):hover { background: #FFFBEB; }
div[role="radiogroup"] > label:nth-child(2):hover { background: #FFF7ED; }
div[role="radiogroup"] > label:nth-child(3):hover { background: #FEF2F2; }
div[role="radiogroup"] > label:nth-child(1):has(input:checked) {
    background: #D97706; border-color: #D97706;
}
div[role="radiogroup"] > label:nth-child(2):has(input:checked) {
    background: #EA580C; border-color: #EA580C;
}
div[role="radiogroup"] > label:nth-child(3):has(input:checked) {
    background: #C0392B; border-color: #C0392B;
}
div[role="radiogroup"] > label:has(input:checked) p { color: #FFFFFF; }
"""


_ANN_COND_CSS = """
{
    background: #F7FAFC;
    border-left: 4px solid #1B3A6B;
    border-radius: 0 8px 8px 0;
    padding: 0.85rem 1.1rem 0.75rem 1.1rem;
    margin: 0 0 0.5rem 0;
    overflow: visible !important;
    min-height: 0;
}
"""


_ANN_INTENSITY_RADIO_LABELS: Tuple[str, str, str] = (
    "1 — Leve",
    "2 — Ofensivo",
    "3 — Hostil",
)


_ANN_INTENSITY_LABEL_TO_INT: Dict[str, int] = {
    "1 — Leve": 1,
    "2 — Ofensivo": 2,
    "3 — Hostil": 3,
}


def _ann_intensity_radio_index(default_1_2_3: int) -> int:
    """Índice 0..2 para st.radio según intensidad por defecto (1, 2 o 3)."""
    try:
        d = int(default_1_2_3)
    except (TypeError, ValueError):
        d = 2
    d = max(1, min(3, d))
    return d - 1


def _ann_pick_sticky_row(
    queue: pd.DataFrame,
    state_key: str,
    id_col: str = "message_uuid",
) -> pd.Series:
    """Devuelve la fila "activa" de una cola y la fija en session_state.

    Evita que los reruns (p. ej. al cambiar el radio del paso 1 fuera del
    st.form) cambien el mensaje mostrado por culpa de queries con
    ORDER BY RANDOM() / df.sample(). El mensaje activo solo cambia tras
    Guardar/Saltar (la sección hace pop de `state_key`).
    """
    ids = queue[id_col].astype(str)
    current = st.session_state.get(state_key)
    if current is not None and current in set(ids):
        row = queue.loc[ids == current].iloc[0]
    else:
        row = queue.iloc[0]
        st.session_state[state_key] = str(row[id_col])
    return row


def _ann_get_or_load_queue(
    cache_key: str,
    loader: Callable[..., pd.DataFrame],
    cache_args: Tuple = (),
) -> pd.DataFrame:
    """Cachea la cola de mensajes en session_state.

    Sin esta caché, las queries con `df.sample(frac=1)` reordenan la cola en
    cada rerun (por ejemplo al cambiar el radio del paso 1 fuera del
    st.form) y el mensaje mostrado puede cambiar antes de que el anotador
    pulse Guardar. `cache_args` invalida la caché si cambia (p. ej. al
    cambiar el filtro de clasificación LLM).

    Tras Guardar/Saltar la fila se elimina en memoria con `_ann_queue_drop`
    (sin reconsultar la base). La cola se vuelve a pedir a Postgres solo
    cuando cambian los filtros (`cache_args`) o se limpia la caché a mano.
    """
    cached = st.session_state.get(cache_key)
    if cached is not None:
        saved_args, df = cached
        if saved_args == cache_args:
            return df
    df = loader(*cache_args)
    st.session_state[cache_key] = (cache_args, df)
    return df


def _ann_queue_drop(
    cache_key: str,
    id_value: str,
    id_col: str = "message_uuid",
) -> None:
    """Quita un mensaje de la cola en session_state sin reconsultar la base."""
    cached = st.session_state.get(cache_key)
    if cached is None:
        return
    saved_args, df = cached
    if df is None or getattr(df, "empty", True) or id_col not in df.columns:
        return
    mask = df[id_col].astype(str) != str(id_value)
    st.session_state[cache_key] = (saved_args, df.loc[mask].copy())


def _ann_rerun_fragment() -> None:
    """Re-ejecuta solo el fragmento activo (fallback a rerun de página)."""
    try:
        st.rerun(scope="fragment")
    except TypeError:
        st.rerun()


# ---------------------------------------------------------------------------
# KPIs en session_state (mismo patrón que _ann_queue_drop): se inicializan
# desde la base al cambiar periodo/filtros / Limpiar saltos, y se incrementan
# en memoria tras Guardar — sin load_* ni rerun de página en el camino crítico.
# ---------------------------------------------------------------------------
_KPI_ANN_YT = "_kpi_ann_yt"
_KPI_V510 = "_kpi_v510"
_KPI_VLLM_YT = "_kpi_vllm_yt"
_KPI_VLLM_X = "_kpi_vllm_x"
_KPI_SUPERVISION = "_kpi_supervision"
# "Anot. YT" = anotaciones hechas en la cola de odio YouTube (Odio + No Odio +
# Dudoso). El nombre anterior "YT Odio" confundía: parecía contar solo odio.
_SUPERVISION_COLS = ("Anot. YT", "Art.510", "LLM YT", "LLM X")


def _ann_kpi_invalidate(*keys: str) -> None:
    """Borra contadores en memoria para forzar recarga desde la base."""
    for key in keys:
        st.session_state.pop(key, None)
        st.session_state.pop(f"{key}_sig", None)


def _ann_kpi_ensure(key: str, sig: tuple, loader) -> dict:
    """Devuelve KPIs cacheados si la firma coincide; si no, carga desde la base."""
    if st.session_state.get(f"{key}_sig") == sig and key in st.session_state:
        return st.session_state[key]
    data = loader()
    st.session_state[key] = data
    st.session_state[f"{key}_sig"] = sig
    return data


def _ann_kpi_ensure_supervision(period: str) -> dict:
    return _ann_kpi_ensure(
        _KPI_SUPERVISION,
        (period,),
        lambda: _load_admin_annotation_supervision(period),
    )


def _ann_kpi_bump_supervision(subsection: str, annotator: str) -> None:
    """Incrementa el resumen compartido Anot. YT / Art.510 / LLM YT / LLM X."""
    data = st.session_state.get(_KPI_SUPERVISION)
    if not data or subsection not in _SUPERVISION_COLS:
        return
    summary = data.setdefault("summary", {})
    summary[subsection] = int(summary.get(subsection, 0) or 0) + 1

    df = data.get("by_annotator")
    if df is None or not isinstance(df, pd.DataFrame):
        df = pd.DataFrame(
            columns=["Anotador", *_SUPERVISION_COLS, "Total"]
        )
    else:
        df = df.copy()

    ann = str(annotator)
    if df.empty or "Anotador" not in df.columns:
        row = {c: 0 for c in _SUPERVISION_COLS}
        row["Anotador"] = ann
        row[subsection] = 1
        row["Total"] = 1
        data["by_annotator"] = pd.DataFrame([row])
        return

    mask = df["Anotador"].astype(str) == ann
    if not mask.any():
        row = {c: 0 for c in _SUPERVISION_COLS}
        row["Anotador"] = ann
        row[subsection] = 1
        row["Total"] = 1
        data["by_annotator"] = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
    else:
        idx = df.index[mask][0]
        for col in (*_SUPERVISION_COLS, "Total"):
            if col not in df.columns:
                df[col] = 0
        df.at[idx, subsection] = int(df.at[idx, subsection] or 0) + 1
        df.at[idx, "Total"] = int(df.at[idx, "Total"] or 0) + 1
        data["by_annotator"] = df.sort_values("Total", ascending=False)


def _ann_kpi_bump_yt(annotator: str) -> None:
    k = st.session_state.get(_KPI_ANN_YT)
    if not k:
        return
    k["total_anotados"] = int(k.get("total_anotados", 0) or 0) + 1
    k["pendientes"] = max(0, int(k.get("pendientes", 0) or 0) - 1)
    k["anotados_periodo"] = int(k.get("anotados_periodo", 0) or 0) + 1
    k["por_anotador"] = int(k.get("por_anotador", 0) or 0) + 1
    tot = int(k.get("total_relevantes", 0) or 0)
    k["pct_avance"] = (k["total_anotados"] / tot * 100) if tot else 0
    _ann_kpi_bump_supervision("Anot. YT", annotator)


def _ann_kpi_bump_v510(annotator: str) -> None:
    k = st.session_state.get(_KPI_V510)
    if not k:
        return
    k["pendientes"] = max(0, int(k.get("pendientes", 0) or 0) - 1)
    k["total_validados"] = int(k.get("total_validados", 0) or 0) + 1
    k["validados_periodo"] = int(k.get("validados_periodo", 0) or 0) + 1
    k["por_anotador"] = int(k.get("por_anotador", 0) or 0) + 1
    _ann_kpi_bump_supervision("Art.510", annotator)


def _ann_kpi_bump_vllm_yt(annotator: str) -> None:
    k = st.session_state.get(_KPI_VLLM_YT)
    if not k:
        return
    k["pendientes"] = max(0, int(k.get("pendientes", 0) or 0) - 1)
    k["total_validados"] = int(k.get("total_validados", 0) or 0) + 1
    k["validados_periodo"] = int(k.get("validados_periodo", 0) or 0) + 1
    k["por_anotador"] = int(k.get("por_anotador", 0) or 0) + 1
    tot = int(k.get("total_etiquetados_llm", 0) or 0)
    k["pct_avance"] = (k["total_validados"] / tot * 100) if tot else 0
    _ann_kpi_bump_supervision("LLM YT", annotator)


def _ann_kpi_bump_vllm_x(annotator: str) -> None:
    k = st.session_state.get(_KPI_VLLM_X)
    if not k:
        return
    k["pendientes"] = max(0, int(k.get("pendientes", 0) or 0) - 1)
    k["total_validados"] = int(k.get("total_validados", 0) or 0) + 1
    k["validados_periodo"] = int(k.get("validados_periodo", 0) or 0) + 1
    k["por_anotador"] = int(k.get("por_anotador", 0) or 0) + 1
    tot = int(k.get("total_etiquetados_llm", 0) or 0)
    k["pct_avance"] = (k["total_validados"] / tot * 100) if tot else 0
    _ann_kpi_bump_supervision("LLM X", annotator)


def _ann_render_supervision_from_state(period: str) -> None:
    """Tarjetas Anot. YT / Art.510 / LLM YT / LLM X + tabla por anotador (desde session_state)."""
    data = _ann_kpi_ensure_supervision(period)
    summary = data.get("summary", {})
    by_annotator = data.get("by_annotator", pd.DataFrame())

    c1, c2, c3, c4 = st.columns(4)
    c1.metric(
        "Anotaciones YT",
        f"{summary.get('Anot. YT', 0):,}",
        help="Mensajes anotados en la cola de odio YouTube (incluye Odio, No Odio y Dudoso).",
    )
    c2.metric("Art. 510", f"{summary.get('Art.510', 0):,}")
    c3.metric("LLM YouTube", f"{summary.get('LLM YT', 0):,}")
    c4.metric("LLM X", f"{summary.get('LLM X', 0):,}")

    st.markdown("**Detalle por anotador**")
    if by_annotator is not None and isinstance(by_annotator, pd.DataFrame) and not by_annotator.empty:
        st.dataframe(by_annotator, use_container_width=True, hide_index=True)
    else:
        st.info("No hay anotaciones registradas en el periodo seleccionado.")
    st.caption(
        "Los conteos son anotaciones hechas en el periodo (Odio + No Odio + Dudoso / "
        "equivalentes en cada cola), no solo las marcadas como odio. "
        "Se actualizan al Guardar en esta pestaña."
    )


def _ann_render_kpis_yt(annotator: str, period: str) -> dict:
    kpis = _ann_kpi_ensure(
        _KPI_ANN_YT,
        (annotator, period),
        lambda: _load_annotation_kpis(annotator, period),
    )
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Total relevantes (YT)", f"{kpis['total_relevantes']:,}")
    k2.metric("Anotados", f"{kpis['total_anotados']:,}")
    k3.metric("Pendientes", f"{kpis['pendientes']:,}")
    k4.metric("Anotados en el periodo", f"{kpis['anotados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(kpis["pct_avance"] / 100, text=f"Avance: {kpis['pct_avance']:.1f}%")
    return kpis


def _ann_render_kpis_v510(annotator: str, period: str) -> dict:
    kpis = _ann_kpi_ensure(
        _KPI_V510,
        (annotator, period),
        lambda: _load_v510_kpis(annotator, period),
    )
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Pendientes Art. 510", f"{kpis['pendientes']:,}")
    k2.metric("Total validados", f"{kpis['total_validados']:,}")
    k3.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k4.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    return kpis


def _ann_render_kpis_vllm_yt(
    annotator: str,
    clasif_filter: Optional[str],
    period: str,
    clasif_sel: str = "Todos",
) -> dict:
    kpis = _ann_kpi_ensure(
        _KPI_VLLM_YT,
        (annotator, clasif_filter, period),
        lambda: _load_vllm_yt_kpis(annotator, clasif_filter, period),
    )
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Etiquetados LLM (YT)", f"{kpis['total_etiquetados_llm']:,}")
    k2.metric("Validados", f"{kpis['total_validados']:,}")
    k3.metric(
        "Pendientes" + (f" ({clasif_sel})" if clasif_filter else ""),
        f"{kpis['pendientes']:,}",
    )
    k4.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(
        kpis["pct_avance"] / 100,
        text=f"Avance validación: {kpis['pct_avance']:.1f}%",
    )
    return kpis


def _ann_render_kpis_vllm_x(
    annotator: str,
    clasif_filter: Optional[str],
    categoria_filter: Optional[str],
    fd_str: Optional[str],
    fh_str: Optional[str],
    period: str,
    filter_suffix: str = "",
) -> dict:
    kpis = _ann_kpi_ensure(
        _KPI_VLLM_X,
        (annotator, clasif_filter, categoria_filter, fd_str, fh_str, period),
        lambda: _load_vllm_x_kpis(
            annotator, clasif_filter, categoria_filter, fd_str, fh_str, period,
        ),
    )
    k1, k2, k3, k4, k5 = st.columns(5)
    k1.metric("Etiquetados LLM (X)", f"{kpis['total_etiquetados_llm']:,}")
    k2.metric("Validados", f"{kpis['total_validados']:,}")
    k3.metric("Pendientes" + filter_suffix, f"{kpis['pendientes']:,}")
    k4.metric("Validados en el periodo", f"{kpis['validados_periodo']:,}")
    k5.metric(f"Por {annotator}", f"{kpis['por_anotador']:,}")
    st.progress(
        kpis["pct_avance"] / 100,
        text=f"Avance validación: {kpis['pct_avance']:.1f}%",
    )
    return kpis


_ANN_FOOTER_CSS = """
{
    background: #F7FAFC;
    border-top: 1px solid #E2E8F0;
    border-radius: 0 0 8px 8px;
    padding: 0.85rem 1rem 0.6rem 1rem;
    margin-top: 0.85rem;
}
button[kind="primaryFormSubmit"], button[kind="primary"] {
    background: #1B3A6B !important;
    border-color: #1B3A6B !important;
}
button[kind="primaryFormSubmit"]:hover, button[kind="primary"]:hover {
    background: #2C5282 !important;
    border-color: #2C5282 !important;
}
"""


def _inject_anotacion_form_css() -> None:
    """Inyecta el CSS específico de los formularios de anotación (1 vez por sesión)."""
    if st.session_state.get("_reto_ann_css_injected"):
        return
    st.markdown(_ANN_FORM_CSS, unsafe_allow_html=True)
    st.session_state["_reto_ann_css_injected"] = True


@contextmanager
def _ann_styled_box(key: str, css: str):
    """Context manager para scopear CSS a un bloque del form.

    `st.container(key=...)` etiqueta el bloque con la clase `st-key-<key>`, que
    es el gancho al que apunta el CSS inyectado acá. El margen negativo
    compensa el espacio que agrega el propio bloque de estilos.
    """
    class_name = re.sub(r"[^a-zA-Z0-9_-]", "-", key.strip())
    container = st.container(key=class_name)
    container.html(
        "<style>\n"
        f".st-key-{class_name} {css}\n"
        f".st-key-{class_name} > div:first-child {{ margin-bottom: -1rem; }}\n"
        "</style>"
    )
    with container:
        yield


CATEGORIA_NO_ODIO = "no_odio"


CATEGORIA_DUDOSO = "dudoso"


def _clasif_from_odio_flag(odio_flag: Optional[bool]) -> Optional[str]:
    if odio_flag is True:
        return "ODIO"
    if odio_flag is False:
        return "NO_ODIO"
    if odio_flag is None:
        return "DUDOSO"
    return None


def _categoria_odio_for_save(
    odio_flag: Optional[bool], categoria_odio: Optional[str]
) -> Optional[str]:
    """Valor explícito en validaciones_manuales según odio_flag."""
    if odio_flag is True:
        cat = (categoria_odio or "").strip()
        return cat if cat else None
    if odio_flag is False:
        return CATEGORIA_NO_ODIO
    if odio_flag is None:
        return CATEGORIA_DUDOSO
    return None


def _normalize_cat_for_coincide(cat: Optional[str], clasif: Optional[str]) -> str:
    c = (cat or "").strip().lower()
    clf = (clasif or "").strip().upper()
    if not c:
        if clf == "NO_ODIO":
            return CATEGORIA_NO_ODIO
        if clf == "DUDOSO":
            return CATEGORIA_DUDOSO
    return c


def _compute_coincide_con_llm(
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    llm_clasif: Optional[str],
    llm_categoria_pred: Optional[str],
    llm_intensidad_pred: Any,
) -> Optional[bool]:
    """
    Compara etiqueta humana (normalizada) con processed.etiquetas_llm.
    None si odio_flag no permite clasificación (no debería ocurrir tras normalizar).
    """
    human_clasif = _clasif_from_odio_flag(odio_flag)
    if human_clasif is None:
        return None

    llm_c = (llm_clasif or "").strip().upper()
    if llm_c != human_clasif:
        return False

    hum_cat = _normalize_cat_for_coincide(categoria_odio, human_clasif)
    llm_cat = _normalize_cat_for_coincide(llm_categoria_pred, llm_c)
    if hum_cat != llm_cat:
        return False

    if odio_flag is True:
        llm_i = str(llm_intensidad_pred or "").strip()
        return str(intensidad) == llm_i

    return True


def _fetch_llm_labels_for_uuid(message_uuid: str) -> tuple:
    """Lee predicción LLM para calcular coincide_con_llm al guardar."""
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT clasificacion_principal, categoria_odio_pred, intensidad_pred
            FROM processed.etiquetas_llm
            WHERE message_uuid = %s::uuid
            """,
            (message_uuid,),
        )
        row = cur.fetchone()
        cur.close()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


def _period_to_sql_date(period: str) -> str:
    """
    period in ('day', 'week', 'month')
    Devuelve la fecha mínima como string ISO para usar en queries.
    """
    today = date.today()
    if period == "week":
        return (today - timedelta(days=7)).isoformat()
    if period == "month":
        return (today - timedelta(days=30)).isoformat()
    return today.isoformat()


@st.cache_data(ttl=3600)
def _load_admin_annotation_supervision(period: str) -> dict:
    """Carga conteos de anotación por subsección y anotador para el panel admin/editor."""
    fecha_desde = _period_to_sql_date(period)
    empty_df = pd.DataFrame(
        columns=["Anotador", "Anot. YT", "Art.510", "LLM YT", "LLM X", "Total"]
    )

    queries = {
        "Anot. YT": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND g.label_source = 'human_explicit'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "LLM YT": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND g.label_source = 'llm_validated'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "LLM X": """
            SELECT vm.annotator_id, COUNT(*) AS n
            FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            JOIN processed.gold_dataset g USING (message_uuid)
            WHERE pm.platform IN ('x', 'twitter')
              AND g.label_source = 'llm_validated'
              AND vm.annotation_date >= %s
            GROUP BY vm.annotator_id
        """,
        "Art.510": """
            SELECT annotator_id, COUNT(*) AS n
            FROM processed.validacion_art510_humana
            WHERE annotation_date >= %s
            GROUP BY annotator_id
        """,
    }

    summary: Dict[str, int] = {}
    frames: Dict[str, pd.DataFrame] = {}

    try:
        with get_conn() as conn:
            for subsection, sql in queries.items():
                df = pd.read_sql(sql, conn, params=(fecha_desde,))
                if df.empty:
                    summary[subsection] = 0
                    continue
                ann_col = "annotator_id" if "annotator_id" in df.columns else df.columns[0]
                df = df.rename(columns={ann_col: "Anotador", "n": subsection})
                summary[subsection] = int(df[subsection].sum())
                frames[subsection] = df[["Anotador", subsection]]

        by_annotator = empty_df.copy()
        for subsection, df_sub in frames.items():
            if df_sub.empty:
                continue
            if by_annotator.empty:
                by_annotator = df_sub.copy()
            else:
                by_annotator = by_annotator.merge(
                    df_sub, on="Anotador", how="outer"
                )

        if not by_annotator.empty:
            for col in ("Anot. YT", "Art.510", "LLM YT", "LLM X"):
                if col not in by_annotator.columns:
                    by_annotator[col] = 0
            by_annotator = by_annotator.fillna(0)
            for col in ("Anot. YT", "Art.510", "LLM YT", "LLM X"):
                by_annotator[col] = by_annotator[col].astype(int)
            by_annotator["Total"] = (
                by_annotator["Anot. YT"]
                + by_annotator["Art.510"]
                + by_annotator["LLM YT"]
                + by_annotator["LLM X"]
            )
            by_annotator = by_annotator.sort_values("Total", ascending=False)
        else:
            by_annotator = empty_df

        return {"summary": summary, "by_annotator": by_annotator}
    except Exception:
        return {
            "summary": {k: 0 for k in queries},
            "by_annotator": empty_df,
        }


def _ann_yt_sql_filters(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> Tuple[str, list]:
    """Fragmento SQL + params para el filtro de fechas de la cola de anotación YT."""
    parts: List[str] = []
    params: list = []
    if fecha_desde:
        parts.append("AND pm.created_at >= %s::date")
        params.append(fecha_desde)
    if fecha_hasta:
        parts.append("AND pm.created_at < (%s::date + interval '1 day')")
        params.append(fecha_hasta)
    return " ".join(parts), params


def _load_annotation_queue(
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> pd.DataFrame:
    """Carga mensajes YouTube pendientes de anotación (sin cache)."""
    skipped = st.session_state.get("ann_skipped", set())
    filter_sql, params = _ann_yt_sql_filters(fecha_desde, fecha_hasta)

    with get_conn() as conn:
        df = pd.read_sql(f"""
            SELECT DISTINCT ON (pm.content_original)
                   pm.message_uuid, pm.content_original, pm.source_media,
                   pm.matched_terms, pm.relevante_score, pm.relevante_motivo,
                   pm.created_at, rm.tweet_id AS video_id
            FROM processed.mensajes pm
            LEFT JOIN raw.mensajes rm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND pm.relevante_llm = 'SI'
              AND pm.message_uuid NOT IN (
                  SELECT message_uuid FROM processed.validaciones_manuales
              )
              {filter_sql}
            ORDER BY pm.content_original, pm.relevante_score DESC NULLS LAST
        """, conn, params=params)
        df = df.sort_values("relevante_score", ascending=False).head(100)

    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


@st.cache_data(ttl=3600)
def _load_annotation_kpis(annotator_id: str, period: str = "day") -> dict:
    """Carga KPIs de progreso de anotación YouTube."""
    fecha_desde = _period_to_sql_date(period)
    with get_conn() as conn:
        cur = conn.cursor()

        cur.execute("""
            SELECT COUNT(*) FROM processed.mensajes pm
            WHERE pm.platform = 'youtube'
              AND pm.relevante_llm = 'SI'
        """)
        total_relevantes = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
        """)
        total_anotados = cur.fetchone()[0]

        pendientes = total_relevantes - total_anotados

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND vm.annotation_date >= %s
        """, (fecha_desde,))
        anotados_periodo = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM processed.validaciones_manuales vm
            JOIN processed.mensajes pm USING (message_uuid)
            WHERE pm.platform = 'youtube'
              AND vm.annotator_id = %s
        """, (annotator_id,))
        por_anotador = cur.fetchone()[0]

        cur.close()

    pct_avance = (total_anotados / total_relevantes * 100) if total_relevantes else 0

    return {
        "total_relevantes": total_relevantes,
        "pendientes": pendientes,
        "total_anotados": total_anotados,
        "anotados_periodo": anotados_periodo,
        "por_anotador": por_anotador,
        "pct_avance": pct_avance,
    }


def _stratified_split(target_ratio: float = 0.85) -> str:
    """Asigna split TRAIN/TEST de forma estratificada consultando el ratio actual en gold_dataset.

    Si el ratio actual de TRAIN < target_ratio → asigna TRAIN (para reequilibrar).
    Si ya está en target o más → asigna TEST.
    Con fallback a asignación aleatoria si la consulta falla.
    """
    import random
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT
                    SUM(CASE WHEN split = 'TRAIN' THEN 1 ELSE 0 END) AS n_train,
                    COUNT(*) AS n_total
                FROM processed.gold_dataset
            """)
            row = cur.fetchone()
            cur.close()
        if row and row[1] and row[1] > 0:
            current_train_ratio = (row[0] or 0) / row[1]
            return "TRAIN" if current_train_ratio < target_ratio else "TEST"
    except Exception:
        pass
    # Fallback aleatorio si la consulta falla
    return "TRAIN" if random.random() < target_ratio else "TEST"


def _save_annotation(
    message_uuid: str,
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    humor_flag: bool,
    annotator_id: str,
) -> bool:
    """Guarda la anotación en validaciones_manuales y gold_dataset."""
    import random

    if odio_flag is True:
        y_odio_final = "Odio"
        y_odio_bin = 1
    elif odio_flag is False:
        y_odio_final = "No Odio"
        y_odio_bin = 0
    else:
        y_odio_final = "Dudoso"
        y_odio_bin = None

    categoria_save = _categoria_odio_for_save(odio_flag, categoria_odio)
    y_categoria = categoria_save
    y_intensidad = intensidad if odio_flag else None
    split_val = _stratified_split(target_ratio=0.85)

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                (message_uuid, odio_flag, categoria_odio, intensidad,
                 humor_flag, annotator_id, annotation_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    odio_flag = EXCLUDED.odio_flag,
                    categoria_odio = EXCLUDED.categoria_odio,
                    intensidad = EXCLUDED.intensidad,
                    humor_flag = EXCLUDED.humor_flag,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date
            """, (
                message_uuid, odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(),
            ))

            cur.execute("""
                INSERT INTO processed.gold_dataset
                (message_uuid, y_odio_final, y_odio_bin, y_categoria_final,
                 y_intensidad_final, label_source, split)
                VALUES (%s, %s, %s, %s, %s, 'human_explicit', %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    y_odio_final = EXCLUDED.y_odio_final,
                    y_odio_bin = EXCLUDED.y_odio_bin,
                    y_categoria_final = EXCLUDED.y_categoria_final,
                    y_intensidad_final = EXCLUDED.y_intensidad_final,
                    label_source = EXCLUDED.label_source
            """, (
                message_uuid, y_odio_final, y_odio_bin,
                y_categoria, y_intensidad, split_val,
            ))

            # Anotar también duplicados con mismo contenido
            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                    (message_uuid, odio_flag, categoria_odio, intensidad,
                     humor_flag, annotator_id, annotation_date)
                SELECT pm2.message_uuid, %s, %s, %s, %s, %s, %s
                FROM processed.mensajes pm2
                WHERE pm2.content_original = (
                    SELECT content_original FROM processed.mensajes
                    WHERE message_uuid = %s
                )
                  AND pm2.message_uuid != %s
                  AND pm2.message_uuid NOT IN (
                      SELECT message_uuid
                      FROM processed.validaciones_manuales
                  )
                ON CONFLICT (message_uuid) DO NOTHING
            """, (
                odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(),
                message_uuid, message_uuid,
            ))

            cur.close()

        # Invalidar cache para que las vistas reflejen la anotación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando anotación: {e}")
        return False


def _load_v510_queue() -> pd.DataFrame:
    """Carga mensajes con potencial delito Art. 510 pendientes de validación humana."""
    skipped = st.session_state.get("v510_skipped", set())

    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT ea.message_uuid,
                       ea.label_source,
                       ea.apartado_510,
                       ea.grupo_protegido,
                       ea.conducta_detectada,
                       ea.justificacion,
                       ea.confianza,
                       pm.platform,
                       pm.content_original,
                       pm.source_media,
                       rm.tweet_id AS video_id
                FROM processed.evaluacion_art510 ea
                JOIN processed.mensajes pm USING (message_uuid)
                LEFT JOIN raw.mensajes rm USING (message_uuid)
                WHERE ea.es_potencial_delito = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM processed.validacion_art510_humana vh
                      WHERE vh.message_uuid = ea.message_uuid
                        AND vh.label_source = ea.label_source
                  )
                ORDER BY
                    CASE ea.confianza
                        WHEN 'alta' THEN 1
                        WHEN 'media' THEN 2
                        ELSE 3
                    END,
                    ea.evaluacion_date DESC
                LIMIT 200
            """, conn)
    except Exception:
        return pd.DataFrame()

    if skipped and not df.empty:
        keys = df["message_uuid"].astype(str) + "|" + df["label_source"].astype(str)
        df = df[~keys.isin(skipped)]

    return df


@st.cache_data(ttl=3600)
def _load_v510_kpis(annotator_id: str, period: str = "day") -> dict:
    """KPIs de progreso de validación Art. 510."""
    fecha_desde = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                SELECT COUNT(*) FROM processed.evaluacion_art510
                WHERE es_potencial_delito = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM processed.validacion_art510_humana vh
                      WHERE vh.message_uuid = evaluacion_art510.message_uuid
                        AND vh.label_source = evaluacion_art510.label_source
                  )
            """)
            pendientes = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM processed.validacion_art510_humana")
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validacion_art510_humana
                WHERE annotation_date >= %s
            """, (fecha_desde,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validacion_art510_humana
                WHERE annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        return {
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
        }
    except Exception:
        return {
            "pendientes": 0, "total_validados": 0,
            "validados_periodo": 0, "por_anotador": 0,
        }


def _save_v510_validation(
    message_uuid: str,
    label_source: str,
    validacion: str,
    apartado_final: Optional[str],
    grupo_final: Optional[str],
    conducta_final: Optional[str],
    comentario: Optional[str],
    annotator_id: str,
) -> bool:
    """Guarda la validación humana de Art. 510."""
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO processed.validacion_art510_humana
                (message_uuid, label_source, validacion_humana,
                 apartado_510_final, grupo_protegido_final, conducta_final,
                 comentario, annotator_id, annotation_date)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid, label_source) DO UPDATE SET
                    validacion_humana = EXCLUDED.validacion_humana,
                    apartado_510_final = EXCLUDED.apartado_510_final,
                    grupo_protegido_final = EXCLUDED.grupo_protegido_final,
                    conducta_final = EXCLUDED.conducta_final,
                    comentario = EXCLUDED.comentario,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date
            """, (
                message_uuid, label_source, validacion,
                apartado_final, grupo_final, conducta_final,
                comentario, annotator_id, date.today(),
            ))
            cur.close()
        # Invalidar cache para reflejar la validación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando validación Art. 510: {e}")
        return False


@st.fragment
def _fragment_anotacion_youtube(annotator: str, fd_str: Optional[str], fh_str: Optional[str]) -> None:
    """KPIs + mensaje + formulario: rerun acotado al fragment tras Guardar/Saltar."""
    _kpi_period = st.session_state.get("supervision_period", "day")
    if st.session_state.get("user_role") in ("admin", "editor"):
        _ann_render_supervision_from_state(_kpi_period)
        st.divider()
    _ann_render_kpis_yt(annotator, _kpi_period)
    st.divider()

    last_status = st.session_state.pop("_ann_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Anotación guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la anotación.")

    if "ann_skipped" not in st.session_state:
        st.session_state["ann_skipped"] = set()

    queue = _ann_get_or_load_queue(
        "_ann_yt_queue_cache",
        _load_annotation_queue,
        (fd_str, fh_str),
    )

    if queue.empty:
        if fd_str or fh_str:
            st.success("No hay mensajes pendientes de anotación en el rango de fechas seleccionado.")
            st.caption("Ampliá o quitá el filtro de fechas para ver el resto de la cola.")
        else:
            st.success("No hay mensajes pendientes de anotación.")
            st.caption(
                "Si esperabas mensajes, verifica que se haya ejecutado "
                "`filtrar_relevancia_youtube.py` para generar la cola de "
                "anotación (marca `relevante_llm = 'SI'` en los candidatos)."
            )
        if st.button("Limpiar saltos y recargar", key="ann_yt_limpiar_saltos"):
            st.session_state["ann_skipped"] = set()
            st.session_state.pop("_ann_yt_queue_cache", None)
            st.session_state.pop("_ann_yt_current_uuid", None)
            _ann_kpi_invalidate(_KPI_ANN_YT, _KPI_SUPERVISION)
            st.rerun()  # full: hay que volver a pedir la cola a la base
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "esos comentarios pueden volver a salir en la cola (muestra aleatoria)."
        )
        return

    msg = _ann_pick_sticky_row(queue, state_key="_ann_yt_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a anotar  ({queue.shape[0]} en cola)")

    col_msg, col_meta = st.columns([3, 1])
    with col_msg:
        st.markdown("**Texto del comentario:**")
        st.text_area(
            "contenido", value=str(msg["content_original"]),
            height=130, disabled=True, label_visibility="collapsed",
        )
    with col_meta:
        st.markdown(
            f"**Plataforma:** {platform_label(str(msg.get('platform') or ''))}"
        )
        _mp = _public_medio_label(msg.get("source_media"))
        if _mp:
            st.markdown(f"**Medio monitorizado:** {_mp}")
        video_id = msg.get("video_id")
        if video_id and pd.notna(video_id):
            yt_url = f"https://www.youtube.com/watch?v={video_id}"
            st.markdown(f"**Video:** [{video_id}]({yt_url})")
        terms = msg.get("matched_terms") or ""
        if terms and pd.notna(terms):
            st.markdown(f"**Términos:** `{terms}`")
        score = msg.get("relevante_score")
        if pd.notna(score):
            st.markdown(f"**Score relevancia:** {float(score):.2f}")
        motivo = msg.get("relevante_motivo")
        if motivo and pd.notna(motivo):
            st.markdown(f"**Motivo LLM:** _{motivo}_")

    st.divider()

    _inject_anotacion_form_css()
    fk = f"ann_yt_{msg_uuid}"

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Completa los siguientes 4 pasos y guarda para pasar al siguiente mensaje.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=None,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "Elegí **Odio**, **No Odio** o **Dudoso**; la pantalla se actualiza al cambiar la opción, "
            "pero **nada se guarda en la base** hasta que pulses **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al marcar No odio o Dudoso: solo se desbloquea la vista. "
            "Para registrar la clasificación y ver el **siguiente mensaje**, pulsá **Guardar y siguiente** abajo."
        )
    else:
        st.caption(
            "Completá los pasos 2 a 4 si corresponde y pulsá **Guardar y siguiente** para guardar."
        )

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=list(CATEGORIAS_LABELS.keys()),
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=None if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(2),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"
        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )
        ok = _save_annotation(
            message_uuid=msg_uuid,
            odio_flag=odio_flag,
            categoria_odio=_categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            intensidad=intensidad if es_odio else None,
            humor_flag=humor if es_odio else False,
            annotator_id=annotator,
        )
        if ok:
            st.session_state.get("ann_skipped", set()).discard(msg_uuid)
            _ann_queue_drop("_ann_yt_queue_cache", msg_uuid)
            _ann_kpi_bump_yt(annotator)
            st.session_state.pop("_ann_yt_current_uuid", None)
            st.session_state["_ann_last_status"] = ("ok", msg_uuid[:8])
        else:
            st.session_state["_ann_last_status"] = ("error", "")
        _ann_rerun_fragment()

    if skipped:
        st.session_state.setdefault("ann_skipped", set()).add(msg_uuid)
        _ann_queue_drop("_ann_yt_queue_cache", msg_uuid)
        st.session_state.pop("_ann_yt_current_uuid", None)
        _ann_rerun_fragment()


def _render_anotacion_youtube(annotator: str):
    """Contenido del tab de anotación YouTube (filtros fuera del fragment; KPIs dentro)."""

    col_fd, col_fh = st.columns(2)
    with col_fd:
        fecha_desde = st.date_input(
            "Fecha desde",
            value=None,
            key="ann_yt_fecha_desde",
        )
    with col_fh:
        fecha_hasta = st.date_input(
            "Fecha hasta",
            value=None,
            key="ann_yt_fecha_hasta",
        )

    fd_str = fecha_desde.isoformat() if fecha_desde else None
    fh_str = fecha_hasta.isoformat() if fecha_hasta else None

    if fd_str and fh_str and fd_str > fh_str:
        st.warning("La fecha **desde** no puede ser posterior a la fecha **hasta**.")
        st.divider()
        return

    _fragment_anotacion_youtube(annotator, fd_str, fh_str)


@st.fragment
def _fragment_validacion_art510(annotator: str) -> None:
    """KPIs + mensaje + formulario Art. 510 (rerun acotado al fragment)."""
    _kpi_period = st.session_state.get("supervision_period", "day")
    if st.session_state.get("user_role") in ("admin", "editor"):
        _ann_render_supervision_from_state(_kpi_period)
        st.divider()
    _ann_render_kpis_v510(annotator, _kpi_period)
    st.divider()

    last_status = st.session_state.pop("_v510_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación Art. 510 guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación Art. 510.")

    if "v510_skipped" not in st.session_state:
        st.session_state["v510_skipped"] = set()

    queue = _ann_get_or_load_queue("_v510_queue_cache", _load_v510_queue)

    if queue.empty:
        summary = load_art510_summary()
        if summary["total_evaluados"] == 0:
            st.info(
                "Aún no se ha ejecutado `evaluar_art510.py`. "
                "Una vez que se evalúen los mensajes de odio bajo el criterio del "
                "Art. 510.1, aparecerán aquí los que requieran validación humana."
            )
            df_preview = load_art510_candidates()
            if not df_preview.empty:
                st.caption(
                    f"Hay **{len(df_preview):,}** mensajes candidatos a evaluar "
                    f"(visibles en la sección *Análisis Art. 510*)."
                )
        else:
            st.success("No hay mensajes Art. 510 pendientes de validación.")
            if summary.get("total_validados", 0) > 0:
                _render_art510_validacion_humana(summary)
        if st.button("Limpiar saltos Art. 510 y recargar", key="v510_clear"):
            st.session_state["v510_skipped"] = set()
            st.session_state.pop("_v510_queue_cache", None)
            st.session_state.pop("_v510_current_id", None)
            _ann_kpi_invalidate(_KPI_V510, _KPI_SUPERVISION)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra los pares (mensaje + fuente de etiqueta) que pasaste con **Saltar**; "
            "si siguen sin validación en la base, volverán a mostrarse como pendientes."
        )
        return

    queue = queue.copy()
    queue["_v510_id"] = (
        queue["message_uuid"].astype(str) + "|" + queue["label_source"].astype(str)
    )
    # Persistir _v510_id en la caché para poder dropear por ese id
    cached = st.session_state.get("_v510_queue_cache")
    if cached is not None:
        st.session_state["_v510_queue_cache"] = (cached[0], queue)

    msg = _ann_pick_sticky_row(
        queue, state_key="_v510_current_id", id_col="_v510_id"
    )
    msg_uuid = str(msg["message_uuid"])
    msg_label_source = str(msg["label_source"])
    msg_key = f"{msg_uuid}|{msg_label_source}"

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    col_msg, col_eval = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del mensaje:**")
        st.text_area(
            "contenido_510", value=str(msg["content_original"]),
            height=150, disabled=True, label_visibility="collapsed",
        )
        plat_raw = str(msg.get("platform", ""))
        plat = platform_label(plat_raw)
        _mp = _public_medio_label(msg.get("source_media"))
        if _mp:
            st.caption(f"Plataforma: **{plat}** · Medio monitorizado: **{_mp}**")
        else:
            st.caption(f"Plataforma: **{plat}**")

    with col_eval:
        st.markdown("**Evaluación del LLM:**")
        ap = msg.get("apartado_510") or "—"
        ap_label = APARTADO_LABELS.get(ap, ap)
        st.markdown(f"**Apartado:** {ap_label}")
        st.markdown(f"**Grupo protegido:** {msg.get('grupo_protegido') or '—'}")
        st.markdown(f"**Conducta:** {msg.get('conducta_detectada') or '—'}")
        st.markdown(f"**Confianza:** {msg.get('confianza') or '—'}")
        st.markdown(f"**Fuente etiquetado:** {LABEL_SOURCE_LABELS.get(msg_label_source, msg_label_source)}")
        just = msg.get("justificacion") or ""
        if just:
            st.markdown(f"**Justificación:** _{just}_")

    st.divider()

    fk = "v510_" + re.sub(r"[^0-9a-zA-Z_-]", "_", msg_key)

    st.markdown("**Validación**")
    validacion = st.radio(
        "¿Es potencial delito Art. 510.1?",
        ["Confirmar", "Rechazar", "Corregir"],
        horizontal=True,
        index=None,
        key=f"{fk}_val",
        help="Confirmar: el LLM acertó. Rechazar: no es delito. Corregir: es delito pero con datos distintos.",
    )
    only_corregir = validacion == "Corregir"

    if validacion is None:
        st.caption(
            "Elegí **Confirmar**, **Rechazar** o **Corregir**; **nada se guarda** hasta pulsar "
            "**Guardar y siguiente** (o **Saltar**)."
        )
    else:
        st.caption(
            "La opción del radio **no guarda sola**: usá **Guardar y siguiente** abajo para registrar y pasar al siguiente."
        )

    with st.form(key=fk, clear_on_submit=False):
        st.markdown("---")
        if only_corregir:
            st.caption(
                "Completar apartado, grupo y conducta solo si mantienes **Corregir** "
                "(se usarán los valores del LLM si eliges **Confirmar**)."
            )
        else:
            st.caption(
                "Apartado, grupo protegido y conducta solo se editan si eliges **Corregir**; "
                "con **Confirmar** o **Rechazar** quedan deshabilitados."
            )

        apartado_opts = ["1a", "1b", "1c"]
        apartado_default = (
            apartado_opts.index(ap) if ap in apartado_opts else 0
        )
        apartado_sel = st.selectbox(
            "Apartado Art. 510.1",
            options=apartado_opts,
            format_func=lambda x: APARTADO_LABELS.get(x, x),
            index=apartado_default,
            key=f"{fk}_ap",
            disabled=not only_corregir,
        )

        grupo_sel = st.text_input(
            "Grupo protegido",
            value=msg.get("grupo_protegido") or "",
            key=f"{fk}_gp",
            help="Ej: raza, religión, orientación sexual, discapacidad...",
            disabled=not only_corregir,
        )

        conducta_sel = st.text_input(
            "Conducta detectada",
            value=msg.get("conducta_detectada") or "",
            key=f"{fk}_cond",
            disabled=not only_corregir,
        )

        comentario = st.text_area(
            "Comentario (opcional)",
            height=80,
            key=f"{fk}_comment",
        )

        st.markdown("---")
        col_save, col_skip = st.columns(2)
        submitted = col_save.form_submit_button(
            "Guardar y siguiente", type="primary", use_container_width=True,
        )
        skipped = col_skip.form_submit_button(
            "Saltar", use_container_width=True,
        )

    if submitted:
        if validacion is None:
            st.error("Selecciona una opción (Confirmar / Rechazar / Corregir).")
            return

        validacion_map = {
            "Confirmar": "confirmado",
            "Rechazar": "rechazado",
            "Corregir": "corregido",
        }

        if validacion == "Confirmar":
            ap_final = msg.get("apartado_510") or None
            gp_final = msg.get("grupo_protegido") or None
            cd_final = msg.get("conducta_detectada") or None
        elif validacion == "Corregir":
            ap_final = apartado_sel
            gp_final = grupo_sel.strip() or None
            cd_final = conducta_sel.strip() or None
        else:
            ap_final = None
            gp_final = None
            cd_final = None

        ok = _save_v510_validation(
            message_uuid=msg_uuid,
            label_source=msg_label_source,
            validacion=validacion_map[validacion],
            apartado_final=ap_final,
            grupo_final=gp_final,
            conducta_final=cd_final,
            comentario=comentario.strip() or None,
            annotator_id=annotator,
        )
        if ok:
            st.session_state.get("v510_skipped", set()).discard(msg_key)
            _ann_queue_drop("_v510_queue_cache", msg_key, id_col="_v510_id")
            _ann_kpi_bump_v510(annotator)
            st.session_state.pop("_v510_current_id", None)
            st.session_state["_v510_last_status"] = ("ok", msg_uuid[:8])
        else:
            st.session_state["_v510_last_status"] = ("error", "")
        _ann_rerun_fragment()

    if skipped:
        st.session_state.setdefault("v510_skipped", set()).add(msg_key)
        _ann_queue_drop("_v510_queue_cache", msg_key, id_col="_v510_id")
        st.session_state.pop("_v510_current_id", None)
        _ann_rerun_fragment()


def _render_validacion_art510(annotator: str):
    """Contenido del tab de validación Art. 510 (KPIs dentro del fragment)."""
    _fragment_validacion_art510(annotator)


def _load_vllm_yt_queue(clasif_filter: Optional[str] = None) -> pd.DataFrame:
    """Carga muestra aleatoria de mensajes YT con etiqueta LLM pendientes de validación humana."""
    try:
        with get_conn() as conn:
            clasif_cond = ""
            params: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params.append(clasif_filter)

            df = pd.read_sql(f"""
                SELECT DISTINCT ON (pm.content_original)
                       pm.message_uuid, pm.content_original, pm.source_media,
                       pm.created_at, rm.tweet_id AS video_id,
                       e.clasificacion_principal, e.categoria_odio_pred,
                       e.intensidad_pred, e.resumen_motivo
                FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                LEFT JOIN raw.mensajes rm USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
                ORDER BY pm.content_original, pm.created_at DESC
            """, conn, params=params)
    except Exception:
        return pd.DataFrame()

    df = df.sample(frac=1).head(100).reset_index(drop=True)

    skipped = st.session_state.get("vllm_yt_skipped", set())
    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


@st.cache_data(ttl=3600)
def _load_vllm_yt_kpis(
    annotator_id: str,
    clasif_filter: Optional[str] = None,
    period: str = "day",
) -> dict:
    """KPIs de validación de etiquetado LLM en YouTube."""
    fecha_desde = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            clasif_cond = ""
            params_pending: list = []
            if clasif_filter:
                clasif_cond = "AND e.clasificacion_principal = %s"
                params_pending.append(clasif_filter)

            cur.execute(f"""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {clasif_cond}
            """, params_pending)
            pendientes = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform = 'youtube'
            """)
            total_etiquetados_llm = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
            """)
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND vm.annotation_date >= %s
            """, (fecha_desde,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform = 'youtube'
                  AND vm.annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        pct = (total_validados / total_etiquetados_llm * 100) if total_etiquetados_llm else 0
        return {
            "total_etiquetados_llm": total_etiquetados_llm,
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
            "pct_avance": pct,
        }
    except Exception:
        return {
            "total_etiquetados_llm": 0, "pendientes": 0,
            "total_validados": 0, "validados_periodo": 0,
            "por_anotador": 0, "pct_avance": 0,
        }


def _vllm_x_sql_filters(
    clasif_filter: Optional[str] = None,
    categoria_filter: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> Tuple[str, list]:
    """Fragmento SQL + params para filtros de cola/KPIs validación LLM X."""
    parts: List[str] = []
    params: list = []
    if clasif_filter:
        parts.append("AND e.clasificacion_principal = %s")
        params.append(clasif_filter)
    if categoria_filter:
        parts.append("AND e.categoria_odio_pred = %s")
        params.append(categoria_filter)
    if fecha_desde:
        parts.append("AND pm.created_at >= %s::date")
        params.append(fecha_desde)
    if fecha_hasta:
        parts.append("AND pm.created_at < (%s::date + interval '1 day')")
        params.append(fecha_hasta)
    return " ".join(parts), params


def _load_vllm_x_queue(
    clasif_filter: Optional[str] = None,
    categoria_filter: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
) -> pd.DataFrame:
    """Cola de mensajes X/Twitter con etiqueta LLM pendientes de validación humana."""
    try:
        with get_conn() as conn:
            filter_sql, params = _vllm_x_sql_filters(
                clasif_filter, categoria_filter, fecha_desde, fecha_hasta,
            )

            df = pd.read_sql(f"""
                SELECT DISTINCT ON (pm.content_original)
                       pm.message_uuid, pm.content_original,
                       pm.created_at,
                       e.clasificacion_principal, e.categoria_odio_pred,
                       e.intensidad_pred, e.resumen_motivo
                FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {filter_sql}
                ORDER BY pm.content_original, pm.created_at DESC
            """, conn, params=params)
    except Exception:
        return pd.DataFrame()

    df = df.sample(frac=1).head(100).reset_index(drop=True)

    skipped = st.session_state.get("vllm_x_skipped", set())
    if skipped and not df.empty:
        df = df[~df["message_uuid"].astype(str).isin(skipped)]

    return df


@st.cache_data(ttl=3600)
def _load_vllm_x_kpis(
    annotator_id: str,
    clasif_filter: Optional[str] = None,
    categoria_filter: Optional[str] = None,
    fecha_desde: Optional[str] = None,
    fecha_hasta: Optional[str] = None,
    period: str = "day",
) -> dict:
    """KPIs de validación de etiquetado LLM en X."""
    fecha_desde_periodo = _period_to_sql_date(period)
    try:
        with get_conn() as conn:
            cur = conn.cursor()

            filter_sql, params_pending = _vllm_x_sql_filters(
                clasif_filter, categoria_filter, fecha_desde, fecha_hasta,
            )

            cur.execute(f"""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND pm.message_uuid NOT IN (
                      SELECT message_uuid FROM processed.validaciones_manuales
                  )
                  {filter_sql}
            """, params_pending)
            pendientes = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.etiquetas_llm e
                JOIN processed.mensajes pm USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
            """)
            total_etiquetados_llm = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
            """)
            total_validados = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND vm.annotation_date >= %s
            """, (fecha_desde_periodo,))
            validados_periodo = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*) FROM processed.validaciones_manuales vm
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                  AND vm.annotator_id = %s
            """, (annotator_id,))
            por_anotador = cur.fetchone()[0]

            cur.close()

        pct = (total_validados / total_etiquetados_llm * 100) if total_etiquetados_llm else 0
        return {
            "total_etiquetados_llm": total_etiquetados_llm,
            "pendientes": pendientes,
            "total_validados": total_validados,
            "validados_periodo": validados_periodo,
            "por_anotador": por_anotador,
            "pct_avance": pct,
        }
    except Exception:
        return {
            "total_etiquetados_llm": 0, "pendientes": 0,
            "total_validados": 0, "validados_periodo": 0,
            "por_anotador": 0, "pct_avance": 0,
        }


def _save_vllm_yt_validation(
    message_uuid: str,
    odio_flag: Optional[bool],
    categoria_odio: Optional[str],
    intensidad: Optional[int],
    humor_flag: bool,
    annotator_id: str,
    coincide_con_llm: Optional[bool] = None,
) -> bool:
    """Guarda validación de etiquetado LLM (YT/X) en validaciones_manuales y gold_dataset."""
    import random

    if odio_flag is True and not (categoria_odio or "").strip():
        st.error(
            "Registro anómalo: odio_flag=true sin categoría. "
            "Marcá de nuevo como Odio y elegí categoría, o revisá en BD."
        )
        return False

    categoria_save = _categoria_odio_for_save(odio_flag, categoria_odio)

    llm_clasif, llm_cat, llm_int = _fetch_llm_labels_for_uuid(message_uuid)
    coincide = _compute_coincide_con_llm(
        odio_flag,
        categoria_save,
        intensidad,
        llm_clasif,
        llm_cat,
        llm_int,
    )

    if odio_flag is True:
        y_odio_final = "Odio"
        y_odio_bin = 1
    elif odio_flag is False:
        y_odio_final = "No Odio"
        y_odio_bin = 0
    else:
        y_odio_final = "Dudoso"
        y_odio_bin = None

    y_categoria = categoria_save
    y_intensidad = intensidad if odio_flag else None
    split_val = _stratified_split(target_ratio=0.85)

    try:
        with get_conn() as conn:
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO processed.validaciones_manuales
                (message_uuid, odio_flag, categoria_odio, intensidad,
                 humor_flag, annotator_id, annotation_date, coincide_con_llm)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    odio_flag = EXCLUDED.odio_flag,
                    categoria_odio = EXCLUDED.categoria_odio,
                    intensidad = EXCLUDED.intensidad,
                    humor_flag = EXCLUDED.humor_flag,
                    annotator_id = EXCLUDED.annotator_id,
                    annotation_date = EXCLUDED.annotation_date,
                    coincide_con_llm = EXCLUDED.coincide_con_llm
            """, (
                message_uuid, odio_flag, categoria_save, intensidad,
                humor_flag, annotator_id, date.today(), coincide,
            ))

            cur.execute("""
                INSERT INTO processed.gold_dataset
                (message_uuid, y_odio_final, y_odio_bin, y_categoria_final,
                 y_intensidad_final, label_source, split)
                VALUES (%s, %s, %s, %s, %s, 'llm_validated', %s)
                ON CONFLICT (message_uuid) DO UPDATE SET
                    y_odio_final = EXCLUDED.y_odio_final,
                    y_odio_bin = EXCLUDED.y_odio_bin,
                    y_categoria_final = EXCLUDED.y_categoria_final,
                    y_intensidad_final = EXCLUDED.y_intensidad_final,
                    label_source = EXCLUDED.label_source
            """, (
                message_uuid, y_odio_final, y_odio_bin,
                y_categoria, y_intensidad, split_val,
            ))

            cur.close()

        # Invalidar cache para reflejar la validación inmediatamente
        st.cache_data.clear()
        return True
    except Exception as e:
        st.error(f"Error guardando validación LLM YT: {e}")
        return False


@st.cache_data(ttl=120)
def _load_vllm_x_corrections() -> pd.DataFrame:
    """Validaciones humanas de etiquetado LLM en X (twitter / x)."""
    try:
        with get_conn() as conn:
            df = pd.read_sql("""
                SELECT pm.message_uuid,
                       pm.content_original,
                       pm.source_media,
                       e.clasificacion_principal AS llm_clasif,
                       e.categoria_odio_pred     AS llm_categoria,
                       e.intensidad_pred         AS llm_intensidad,
                       e.resumen_motivo          AS llm_motivo,
                       CASE WHEN v.odio_flag = TRUE THEN 'ODIO'
                            WHEN v.odio_flag = FALSE THEN 'NO_ODIO'
                            ELSE 'DUDOSO' END    AS humano_clasif,
                       v.categoria_odio          AS humano_categoria,
                       v.intensidad              AS humano_intensidad,
                       v.humor_flag              AS humano_humor,
                       v.coincide_con_llm,
                       v.annotator_id,
                       v.annotation_date
                FROM processed.validaciones_manuales v
                JOIN processed.mensajes pm USING (message_uuid)
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE pm.platform IN ('x', 'twitter')
                ORDER BY v.annotation_date DESC
            """, conn)
    except Exception:
        return pd.DataFrame()
    return df


def _render_vllm_x_error_analysis() -> None:
    _render_vllm_label_error_analysis(
        _load_vllm_x_corrections, key_prefix="vllm_x", file_tag="x",
    )


def _render_validacion_llm_youtube(annotator: str):
    """Pestaña de validación del etiquetado LLM en YouTube."""

    # --- Filtro por clasificación LLM ---
    clasif_options = ["Todos", "ODIO", "NO_ODIO", "DUDOSO"]
    clasif_sel = st.selectbox(
        "Filtrar por predicción LLM",
        options=clasif_options,
        index=0,
        key="vllm_yt_clasif_filter",
    )
    clasif_filter = clasif_sel if clasif_sel != "Todos" else None

    # --- Panel de análisis de errores ---
    with st.expander("📊 Análisis de concordancia LLM vs Humano (YouTube)", expanded=False):
        _render_vllm_yt_error_analysis()

    st.divider()
    _fragment_validacion_llm_youtube(annotator, clasif_filter, clasif_sel)


@st.fragment
def _fragment_validacion_llm_youtube(
    annotator: str,
    clasif_filter: Optional[str],
    clasif_sel: str = "Todos",
) -> None:
    """KPIs + cola + formulario de validación LLM YouTube."""
    _kpi_period = st.session_state.get("supervision_period", "day")
    if st.session_state.get("user_role") in ("admin", "editor"):
        _ann_render_supervision_from_state(_kpi_period)
        st.divider()
    kpis = _ann_render_kpis_vllm_yt(annotator, clasif_filter, _kpi_period, clasif_sel)
    st.divider()

    last_status = st.session_state.pop("_vllm_yt_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación LLM guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación.")

    # --- Cola ---
    if "vllm_yt_skipped" not in st.session_state:
        st.session_state["vllm_yt_skipped"] = set()

    queue = _ann_get_or_load_queue(
        "_vllm_yt_queue_cache",
        _load_vllm_yt_queue,
        (clasif_filter,),
    )

    if queue.empty:
        if kpis["pendientes"] == 0 and kpis["total_etiquetados_llm"] > 0:
            st.success("Todos los mensajes con etiqueta LLM han sido validados.")
        elif kpis["total_etiquetados_llm"] == 0:
            st.info(
                "No hay mensajes YouTube etiquetados por el LLM. "
                "Ejecutá `etiquetar_completo_youtube_llm.py` primero."
            )
        else:
            st.info("No hay mensajes pendientes con el filtro seleccionado.")
        if st.button("Limpiar saltos y recargar", key="vllm_yt_clear"):
            st.session_state["vllm_yt_skipped"] = set()
            st.session_state.pop("_vllm_yt_queue_cache", None)
            st.session_state.pop("_vllm_yt_current_uuid", None)
            _ann_kpi_invalidate(_KPI_VLLM_YT, _KPI_SUPERVISION)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "pueden volver a aparecer en la cola de validación LLM (YouTube)."
        )
        return

    msg = _ann_pick_sticky_row(queue, state_key="_vllm_yt_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    # --- Contenido y predicción LLM ---
    col_msg, col_llm = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del comentario:**")
        st.text_area(
            "contenido_vllm", value=str(msg["content_original"]),
            height=140, disabled=True, label_visibility="collapsed",
        )
        plat_v = platform_label(str(msg.get("platform") or ""))
        _mp = _public_medio_label(msg.get("source_media"))
        video_id = msg.get("video_id")
        vid_link = ""
        if video_id and pd.notna(video_id):
            yt_url = f"https://www.youtube.com/watch?v={video_id}"
            vid_link = f" · [Video]({yt_url})"
        if _mp:
            st.caption(f"Plataforma: **{plat_v}** · Medio monitorizado: **{_mp}**{vid_link}")
        else:
            st.caption(f"Plataforma: **{plat_v}**{vid_link}")

    with col_llm:
        st.markdown("**Predicción del LLM:**")
        llm_clasif = msg.get("clasificacion_principal") or "—"
        llm_cat_raw = msg.get("categoria_odio_pred") or ""
        llm_cat = CATEGORIAS_LABELS.get(llm_cat_raw, llm_cat_raw) if llm_cat_raw else "—"
        llm_int = msg.get("intensidad_pred") or "—"
        llm_motivo = msg.get("resumen_motivo") or ""

        clasif_colors = {"ODIO": "🔴", "NO_ODIO": "🟢", "DUDOSO": "🟡"}
        st.markdown(f"**Clasificación:** {clasif_colors.get(llm_clasif, '')} {llm_clasif}")
        st.markdown(f"**Categoría:** {llm_cat}")
        int_labels = {"1": "1 — Leve", "2": "2 — Ofensivo", "3": "3 — Hostil"}
        st.markdown(f"**Intensidad:** {int_labels.get(str(llm_int), str(llm_int))}")
        if llm_motivo:
            st.markdown(f"**Motivo:** _{llm_motivo}_")

    st.divider()

    # --- Formulario (paso 1 fuera del st.form para deshabilitar 2–4 si no es Odio) ---
    _inject_anotacion_form_css()
    fk = f"vllm_yt_{msg_uuid}"

    llm_odio_idx = (
        {"ODIO": 0, "NO_ODIO": 1, "DUDOSO": 2}.get(llm_clasif)
    )
    llm_cat_idx = None
    cat_keys = list(CATEGORIAS_LABELS.keys())
    if llm_cat_raw in cat_keys:
        llm_cat_idx = cat_keys.index(llm_cat_raw)
    llm_int_val = int(llm_int) if str(llm_int) in {"1", "2", "3"} else 2

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Los campos vienen precargados con la predicción del LLM. Confirma o corrige y guarda.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=llm_odio_idx,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "La predicción del LLM viene precargada; podés cambiarla. "
            "**Nada se guarda** hasta pulsar **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al cambiar a No odio o Dudoso. "
            "Pulsá **Guardar y siguiente** abajo para registrar y pasar al siguiente mensaje."
        )
    else:
        st.caption("Ajustá los pasos 2 a 4 si hace falta y pulsá **Guardar y siguiente**.")

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=cat_keys,
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=llm_cat_idx if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(llm_int_val),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    # --- Procesar acciones ---
    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"

        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )

        ok = _save_vllm_yt_validation(
            message_uuid=msg_uuid,
            odio_flag=odio_flag,
            categoria_odio=_categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            intensidad=intensidad if es_odio else None,
            humor_flag=humor if es_odio else False,
            annotator_id=annotator,
        )
        if ok:
            _load_vllm_yt_corrections.clear()
            st.session_state.get("vllm_yt_skipped", set()).discard(msg_uuid)
            _ann_queue_drop("_vllm_yt_queue_cache", msg_uuid)
            _ann_kpi_bump_vllm_yt(annotator)
            st.session_state.pop("_vllm_yt_current_uuid", None)
            st.session_state["_vllm_yt_last_status"] = ("ok", msg_uuid[:8])
        else:
            st.session_state["_vllm_yt_last_status"] = ("error", "")
        _ann_rerun_fragment()

    if skipped:
        st.session_state.setdefault("vllm_yt_skipped", set()).add(msg_uuid)
        _ann_queue_drop("_vllm_yt_queue_cache", msg_uuid)
        st.session_state.pop("_vllm_yt_current_uuid", None)
        _ann_rerun_fragment()


def _render_validacion_llm_x(annotator: str):
    """Pestaña de validación del etiquetado LLM en X (Twitter)."""

    clasif_options = ["Todos", "ODIO", "NO_ODIO", "DUDOSO"]
    categoria_options = ["Todas", *list(CATEGORIAS_LABELS.keys())]

    col_clasif, col_cat = st.columns(2)
    with col_clasif:
        clasif_sel = st.selectbox(
            "Filtrar por predicción LLM",
            options=clasif_options,
            index=0,
            key="vllm_x_clasif_filter",
        )
    with col_cat:
        categoria_sel = st.selectbox(
            "Filtrar por categoría LLM",
            options=categoria_options,
            format_func=lambda x: (
                "Todas"
                if x == "Todas"
                else CATEGORIAS_LABELS.get(x, x)
            ),
            index=0,
            key="vllm_x_categoria_filter",
        )

    col_fd, col_fh = st.columns(2)
    with col_fd:
        fecha_desde = st.date_input(
            "Fecha desde",
            value=None,
            key="vllm_x_fecha_desde",
        )
    with col_fh:
        fecha_hasta = st.date_input(
            "Fecha hasta",
            value=None,
            key="vllm_x_fecha_hasta",
        )

    clasif_filter = clasif_sel if clasif_sel != "Todos" else None
    categoria_filter = categoria_sel if categoria_sel != "Todas" else None
    fd_str = fecha_desde.isoformat() if fecha_desde else None
    fh_str = fecha_hasta.isoformat() if fecha_hasta else None

    if fd_str and fh_str and fd_str > fh_str:
        st.warning("La fecha **desde** no puede ser posterior a la fecha **hasta**.")
        st.divider()
        return

    filter_parts: List[str] = []
    if clasif_filter:
        filter_parts.append(clasif_filter)
    if categoria_filter:
        filter_parts.append(CATEGORIAS_LABELS.get(categoria_filter, categoria_filter))
    if fd_str or fh_str:
        rango = f"{fd_str or '…'} → {fh_str or '…'}"
        filter_parts.append(rango)
    filter_suffix = f" ({', '.join(filter_parts)})" if filter_parts else ""

    with st.expander("📊 Análisis de concordancia LLM vs Humano (X / Twitter)", expanded=False):
        _render_vllm_x_error_analysis()

    st.divider()
    _fragment_validacion_llm_x(
        annotator, clasif_filter, categoria_filter, fd_str, fh_str, filter_suffix,
    )


@st.fragment
def _fragment_validacion_llm_x(
    annotator: str,
    clasif_filter: Optional[str],
    categoria_filter: Optional[str],
    fd_str: Optional[str],
    fh_str: Optional[str],
    filter_suffix: str = "",
) -> None:
    """KPIs + cola + formulario de validación LLM X."""
    _kpi_period = st.session_state.get("supervision_period", "day")
    if st.session_state.get("user_role") in ("admin", "editor"):
        _ann_render_supervision_from_state(_kpi_period)
        st.divider()
    kpis = _ann_render_kpis_vllm_x(
        annotator,
        clasif_filter,
        categoria_filter,
        fd_str,
        fh_str,
        _kpi_period,
        filter_suffix,
    )
    st.divider()

    last_status = st.session_state.pop("_vllm_x_last_status", None)
    if last_status:
        if last_status[0] == "ok":
            st.success(f"Validación LLM guardada ({last_status[1]}...)")
        else:
            st.error("Error al guardar la validación.")

    if "vllm_x_skipped" not in st.session_state:
        st.session_state["vllm_x_skipped"] = set()

    queue = _ann_get_or_load_queue(
        "_vllm_x_queue_cache",
        _load_vllm_x_queue,
        (clasif_filter, categoria_filter, fd_str, fh_str),
    )

    if queue.empty:
        if kpis["pendientes"] == 0 and kpis["total_etiquetados_llm"] > 0:
            st.success("Todos los mensajes con etiqueta LLM han sido validados.")
        elif kpis["total_etiquetados_llm"] == 0:
            st.info(
                "No hay mensajes X/Twitter etiquetados por el LLM. "
                "Ejecutá el pipeline de etiquetado LLM para X primero."
            )
        else:
            st.info("No hay mensajes pendientes con los filtros seleccionados.")
        if st.button("Limpiar saltos y recargar", key="vllm_x_clear"):
            st.session_state["vllm_x_skipped"] = set()
            st.session_state.pop("_vllm_x_queue_cache", None)
            st.session_state.pop("_vllm_x_current_uuid", None)
            _ann_kpi_invalidate(_KPI_VLLM_X, _KPI_SUPERVISION)
            st.rerun()
        st.caption(
            "**Limpiar saltos:** borra la memoria de mensajes que pasaste con **Saltar**; "
            "pueden volver a aparecer en la cola de validación LLM (X/Twitter)."
        )
        return

    msg = _ann_pick_sticky_row(queue, state_key="_vllm_x_current_uuid")
    msg_uuid = str(msg["message_uuid"])

    st.subheader(f"Mensaje a validar  ({queue.shape[0]} en cola)")

    col_msg, col_llm = st.columns([3, 2])
    with col_msg:
        st.markdown("**Texto del mensaje:**")
        st.text_area(
            "contenido_vllm_x", value=str(msg["content_original"]),
            height=140, disabled=True, label_visibility="collapsed",
        )

    with col_llm:
        st.markdown("**Predicción del LLM:**")
        llm_clasif = msg.get("clasificacion_principal") or "—"
        llm_cat_raw = msg.get("categoria_odio_pred") or ""
        llm_cat = CATEGORIAS_LABELS.get(llm_cat_raw, llm_cat_raw) if llm_cat_raw else "—"
        llm_int = msg.get("intensidad_pred") or "—"
        llm_motivo = msg.get("resumen_motivo") or ""

        clasif_colors = {"ODIO": "🔴", "NO_ODIO": "🟢", "DUDOSO": "🟡"}
        st.markdown(f"**Clasificación:** {clasif_colors.get(llm_clasif, '')} {llm_clasif}")
        st.markdown(f"**Categoría:** {llm_cat}")
        int_labels = {"1": "1 — Leve", "2": "2 — Ofensivo", "3": "3 — Hostil"}
        st.markdown(f"**Intensidad:** {int_labels.get(str(llm_int), str(llm_int))}")
        if llm_motivo:
            st.markdown(f"**Motivo:** _{llm_motivo}_")

    st.divider()

    _inject_anotacion_form_css()
    fk = f"vllm_x_{msg_uuid}"

    llm_odio_idx = (
        {"ODIO": 0, "NO_ODIO": 1, "DUDOSO": 2}.get(llm_clasif)
    )
    llm_cat_idx = None
    cat_keys = list(CATEGORIAS_LABELS.keys())
    if llm_cat_raw in cat_keys:
        llm_cat_idx = cat_keys.index(llm_cat_raw)
    llm_int_val = int(llm_int) if str(llm_int) in {"1", "2", "3"} else 2

    st.markdown(
        '<div class="ann-form-title">Clasificación de la muestra</div>'
        '<div class="ann-form-subtitle">Los campos vienen precargados con la predicción del LLM. Confirma o corrige y guarda.</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="ann-step-header"><span class="ann-step-num">1</span> ¿Contiene discurso de odio?</div>'
        '<div class="ann-step-desc">Selecciona la clasificación principal del mensaje.</div>',
        unsafe_allow_html=True,
    )
    with _ann_styled_box(key=f"chips_{fk}", css=_ANN_CHIPS_CSS):
        odio_choice = st.radio(
            "¿Es discurso de odio?",
            ["Odio", "No Odio", "Dudoso"],
            horizontal=True,
            index=llm_odio_idx,
            key=f"{fk}_odio",
            label_visibility="collapsed",
        )
    only_odio = odio_choice == "Odio"

    if odio_choice is None:
        st.caption(
            "La predicción del LLM viene precargada; podés cambiarla. "
            "**Nada se guarda** hasta pulsar **Guardar y siguiente** (o **Saltar**)."
        )
    elif odio_choice in ("No Odio", "Dudoso"):
        st.info(
            "**No se guarda automáticamente** al cambiar a No odio o Dudoso. "
            "Pulsá **Guardar y siguiente** abajo para registrar y pasar al siguiente mensaje."
        )
    else:
        st.caption("Ajustá los pasos 2 a 4 si hace falta y pulsá **Guardar y siguiente**.")

    with st.form(key=fk, clear_on_submit=False):
        if only_odio:
            st.markdown(
                '<div class="ann-cond-banner">Completar los siguientes campos <b>solo si la clasificación es Odio</b> (se ignorarán en No Odio / Dudoso).</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                '<div class="ann-cond-banner" style="background:#EDF2F7;border-left-color:#718096;color:#4A5568;">'
                "Los pasos <b>2 a 4</b> solo aplican si marcas <b>Odio</b>. Con <b>No Odio</b> o <b>Dudoso</b> quedan deshabilitados.</div>",
                unsafe_allow_html=True,
            )
        st.markdown(
            '<div class="ann-step-header ann-step-header--standalone">'
            '<span class="ann-step-num">2</span> Categoría de odio</div>',
            unsafe_allow_html=True,
        )

        with _ann_styled_box(key=f"cond_{fk}", css=_ANN_COND_CSS):
            categoria = st.selectbox(
                "Categoría de odio",
                options=cat_keys,
                format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
                index=llm_cat_idx if only_odio else 0,
                key=f"{fk}_cat",
                label_visibility="collapsed",
                disabled=not only_odio,
            )

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">3</span> Intensidad</div>'
                '<div class="ann-step-desc">Elegí un nivel tocando una de las tres opciones.</div>',
                unsafe_allow_html=True,
            )
            with _ann_styled_box(key=f"intchips_{fk}", css=_ANN_INTENSITY_CHIPS_CSS):
                _int_lbl = st.radio(
                    "Intensidad del mensaje",
                    list(_ANN_INTENSITY_RADIO_LABELS),
                    horizontal=True,
                    index=_ann_intensity_radio_index(llm_int_val),
                    key=f"{fk}_int_lvl",
                    label_visibility="collapsed",
                    disabled=not only_odio,
                )
            intensidad = _ANN_INTENSITY_LABEL_TO_INT[_int_lbl]

            st.markdown(
                '<div class="ann-step-header"><span class="ann-step-num">4</span> Humor o sarcasmo</div>'
                '<div class="ann-humor-hint">Marca si el mensaje usa humor o sarcasmo como vector del odio.</div>',
                unsafe_allow_html=True,
            )
            humor = st.checkbox(
                "El mensaje usa humor o sarcasmo",
                key=f"{fk}_humor",
                disabled=not only_odio,
            )

        with _ann_styled_box(key=f"footer_{fk}", css=_ANN_FOOTER_CSS):
            col_save, col_skip = st.columns(2)
            submitted = col_save.form_submit_button(
                "Guardar y siguiente", type="primary", use_container_width=True,
            )
            skipped = col_skip.form_submit_button(
                "Saltar", use_container_width=True,
            )

    if submitted:
        if odio_choice is None:
            st.error("Selecciona una clasificación (Odio / No Odio / Dudoso).")
            return

        es_odio = odio_choice == "Odio"

        if es_odio and not categoria:
            st.error("Si marcas **Odio**, selecciona una categoría.")
            return

        odio_flag = (
            True if odio_choice == "Odio"
            else (False if odio_choice == "No Odio" else None)
        )

        ok = _save_vllm_yt_validation(
            message_uuid=msg_uuid,
            odio_flag=odio_flag,
            categoria_odio=_categoria_odio_for_save(odio_flag, categoria if es_odio else None),
            intensidad=intensidad if es_odio else None,
            humor_flag=humor if es_odio else False,
            annotator_id=annotator,
        )
        if ok:
            _load_vllm_x_corrections.clear()
            st.session_state.get("vllm_x_skipped", set()).discard(msg_uuid)
            _ann_queue_drop("_vllm_x_queue_cache", msg_uuid)
            _ann_kpi_bump_vllm_x(annotator)
            st.session_state.pop("_vllm_x_current_uuid", None)
            st.session_state["_vllm_x_last_status"] = ("ok", msg_uuid[:8])
        else:
            st.session_state["_vllm_x_last_status"] = ("error", "")
        _ann_rerun_fragment()

    if skipped:
        st.session_state.setdefault("vllm_x_skipped", set()).add(msg_uuid)
        _ann_queue_drop("_vllm_x_queue_cache", msg_uuid)
        st.session_state.pop("_vllm_x_current_uuid", None)
        _ann_rerun_fragment()



def _render_supervision_panel(period: str) -> None:
    """Compat: delega al render desde session_state (uso legacy)."""
    _ann_render_supervision_from_state(period)


def render_anotacion():
    """Sección de anotación humana: YouTube, Art. 510 y validación LLM (YT + X)."""
    if not _require_role("admin", "editor", section="Anotación y validación"):
        return
    _render_section_header(
        "Anotación y validación",
        "Anotación en YouTube, validación Art. 510 (X + YouTube) y control de calidad del etiquetado LLM.",
    )

    user_role = st.session_state.get("user_role")
    if user_role in ("admin", "editor"):
        st.subheader("Supervisión de anotaciones")
        period = st.radio(
            "Periodo",
            options=["day", "week", "month"],
            format_func=lambda x: {
                "day": "Último día",
                "week": "Última semana",
                "month": "Último mes",
            }[x],
            horizontal=True,
            key="supervision_period",
        )
        # Al cambiar el periodo, invalidar KPIs en memoria para recargar desde la base.
        _prev_period = st.session_state.get("_kpi_supervision_period_applied")
        if _prev_period != period:
            _ann_kpi_invalidate(
                _KPI_SUPERVISION, _KPI_ANN_YT, _KPI_V510, _KPI_VLLM_YT, _KPI_VLLM_X,
            )
            st.session_state["_kpi_supervision_period_applied"] = period
        st.caption(
            "Las tarjetas de supervisión y de cada pestaña se actualizan al Guardar. "
            "Cambiá el periodo para refrescar los totales desde la base."
        )
        st.divider()

    # --- Identificación del anotador (derivada del usuario autenticado) ---
    # El annotator_id se fija a partir de la sesión autenticada para garantizar
    # la integridad de la autoría en el gold dataset.
    # Los admin pueden sobreescribirlo (para anotar en nombre de otro usuario).
    session_user = st.session_state.get("user_name", "")
    user_role = st.session_state.get("user_role")

    if user_role == "admin":
        annotator = st.text_input(
            "Nombre / ID de anotador",
            value=st.session_state.get("annotator_id", session_user),
            placeholder="Ej: CIEDES, Anotador1...",
            key="ann_id_input",
            help="Admin: podés cambiar el ID para anotar en nombre de otro anotador.",
        )
        if annotator:
            st.session_state["annotator_id"] = annotator.strip()
    else:
        # Editor: annotator_id fijo al usuario de sesión (no editable)
        annotator = session_user
        st.session_state["annotator_id"] = session_user
        st.caption(f"Anotando como: **{session_user}**")

    if not annotator.strip():
        st.info("No se pudo determinar tu ID de anotador. Iniciá sesión nuevamente.")
        return

    # --- Tabs ---
    tab_yt, tab_510, tab_vllm_yt, tab_vllm_x = st.tabs([
        "Anotación odio YouTube",
        "Validación Art. 510 (X + YouTube)",
        "Validación etiquetado LLM (YT)",
        "Validación Etiquetado LLM X",
    ])

    with tab_yt:
        _render_anotacion_youtube(annotator.strip())

    with tab_510:
        _render_validacion_art510(annotator.strip())

    with tab_vllm_yt:
        _render_validacion_llm_youtube(annotator.strip())

    with tab_vllm_x:
        _render_validacion_llm_x(annotator.strip())
