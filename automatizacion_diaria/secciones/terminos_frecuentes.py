"""Sección «Términos frecuentes» del dashboard."""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, List, Optional, Tuple
import json
import re
import unicodedata
from collections import Counter
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import pandas as pd
import plotly.express as px
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from components.constants import CATEGORIAS_LABELS, _expand_platforms, platform_label
from components.ui import (
    _apply_horizontal_bar_labels,
    _render_section_header,
    _role_can_access_raw,
)
from components.exports import render_section_exports
from components.db_helpers import load_filter_options, _pooled_conn

try:
    from terminos_exclusion_oficial import TERMINOS_EXCLUSION_LEMAS
except ImportError:
    import importlib.util as _ilu
    TERMINOS_EXCLUSION_LEMAS = frozenset()
    for _p in (_HERE / "terminos_exclusion_oficial.py", _HERE.parent / "terminos_exclusion_oficial.py"):
        if _p.exists():
            _spec = _ilu.spec_from_file_location("terminos_exclusion_oficial", _p)
            _mod = _ilu.module_from_spec(_spec)
            _spec.loader.exec_module(_mod)
            TERMINOS_EXCLUSION_LEMAS = _mod.TERMINOS_EXCLUSION_LEMAS
            break


@st.cache_data(ttl=300)
def load_terminos(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
    solo_candidatos: bool = True,
    ultimas_horas: Optional[int] = None,
) -> pd.DataFrame:
    platforms = _expand_platforms(list(platforms) if platforms else None)
    medios = list(medios) if medios else None
    categorias = list(categorias) if categorias else None

    conds = ["pm.matched_terms IS NOT NULL", "pm.matched_terms != ''"]
    params = []
    need_llm_join = False

    if solo_candidatos:
        # «Candidatos» en UI: incluir is_candidate (p. ej. YouTube) aunque el ETL deje
        # has_hate_terms_match en FALSE, o filas solo marcadas por lexicón.
        conds.append("(pm.is_candidate = TRUE OR pm.has_hate_terms_match = TRUE)")
    if platforms:
        conds.append("pm.platform IN %s"); params.append(tuple(platforms))
    if medios:
        conds.append("pm.source_media IN %s"); params.append(tuple(medios))
    if categorias:
        conds.append("e.categoria_odio_pred IN %s"); params.append(tuple(categorias))
        need_llm_join = True
    if ultimas_horas:
        # Ingreso al sistema (p. ej. YT cargado hoy con comentario publicado hace días)
        conds.append(
            "COALESCE(pm.processed_at, pm.created_at) >= NOW() - (%s::integer * interval '1 hour')"
        )
        params.append(ultimas_horas)

    where = " AND ".join(conds)
    join_clause = "INNER JOIN processed.etiquetas_llm e USING (message_uuid)" if need_llm_join else ""

    with _pooled_conn() as conn:
        df = pd.read_sql(
            f"SELECT pm.matched_terms FROM processed.mensajes pm {join_clause} WHERE {where}",
            conn, params=params,
        )
    return df


def _normalize_term_for_filter(token: str) -> str:
    """
    Normaliza términos para conteo/exclusión:
    minúsculas, sin tildes, sin artefactos de formato y espacios colapsados.
    """
    s = str(token or "").strip().lower()
    if not s:
        return ""
    s = "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )
    s = s.strip().strip("\"'`[](){}")
    s = re.sub(r"^[^\w]+|[^\w]+$", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    if not re.search(r"[a-zñ]", s):
        return ""
    if len(s) <= 1:
        return ""
    return s


def _parse_and_normalize_matched_terms(value: Any) -> List[str]:
    """
    Convierte `matched_terms` (string/lista/JSON serializado) en tokens limpios.
    Mejora la exclusión evitando que entren términos con comillas/corchetes/ruido.
    """
    if value is None:
        return []
    if isinstance(value, float) and pd.isna(value):
        return []

    raw_tokens: List[Any] = []

    if isinstance(value, (list, tuple, set)):
        raw_tokens.extend(list(value))
    else:
        raw = str(value).strip()
        if not raw:
            return []

        parsed = None
        if raw.startswith("[") and raw.endswith("]"):
            try:
                parsed = json.loads(raw)
            except Exception:
                parsed = None

        if isinstance(parsed, list):
            raw_tokens.extend(parsed)
        elif isinstance(parsed, str):
            raw_tokens.append(parsed)
        else:
            # Soporta delimitadores mixtos frecuentes en matched_terms.
            raw_tokens.extend([t for t in re.split(r"[|,;]", raw) if t is not None])

    normalized: List[str] = []
    for tok in raw_tokens:
        nt = _normalize_term_for_filter(tok)
        if nt:
            normalized.append(nt)
    return normalized


_TERMINOS_EXCLUSION_NORM: frozenset = frozenset(
    x for x in (_normalize_term_for_filter(t) for t in TERMINOS_EXCLUSION_LEMAS) if x
)


def load_terminos_exclusion_set() -> frozenset:
    """Lemas excluidos normalizados. Fuente única: `terminos_exclusion_oficial.py` (generado desde el JSON)."""
    return _TERMINOS_EXCLUSION_NORM


def _filter_counter_terminos_neutros(counter: Counter, exclude: frozenset) -> Counter:
    """Quita del contador las claves cuya forma normalizada está en `exclude`."""
    out = Counter()
    for term, n in counter.items():
        nt = _normalize_term_for_filter(term)
        if not nt or nt in exclude:
            continue
        out[term] = n
    return out


@st.cache_data(ttl=300)
def compute_terminos_counter(
    platforms: Optional[Tuple] = None,
    medios: Optional[Tuple] = None,
    categorias: Optional[Tuple] = None,
    solo_candidatos: bool = True,
    ultimas_horas: Optional[int] = None,
    filtro_neutros: bool = True,
) -> Tuple[Counter, int, int]:
    """
    Hace el trabajo pesado en Python (parseo/normalización de matched_terms +
    conteo + filtro de exclusión) UNA sola vez por combinación de filtros,
    en vez de repetirlo en cada rerun de Streamlit (p. ej. al mover el
    slider "Cantidad de términos", que no cambia estos datos de entrada).

    load_terminos() ya está cacheada (ttl=300) — llamarla acá adentro con
    los mismos parámetros es un cache-hit barato, no una query nueva.

    Devuelve: (counter_filtrado, n_terminos_distintos_antes_del_filtro,
    n_lemas_en_lista_de_exclusion).
    """
    df = load_terminos(
        platforms=platforms,
        medios=medios,
        categorias=categorias,
        solo_candidatos=solo_candidatos,
        ultimas_horas=ultimas_horas,
    )

    all_terms: List[str] = []
    for terms_raw in df["matched_terms"]:
        all_terms.extend(_parse_and_normalize_matched_terms(terms_raw))

    counter = Counter(all_terms)
    n_tokens_antes = len(counter)

    exclude = load_terminos_exclusion_set() if filtro_neutros else frozenset()
    if filtro_neutros:
        counter = _filter_counter_terminos_neutros(counter, exclude)

    return counter, n_tokens_antes, len(exclude)


@st.cache_data(ttl=300)
def generate_wordcloud_array(freqs: Tuple[Tuple[str, int], ...], max_words: int):
    """
    Genera la imagen de la wordcloud como array de numpy (picklable, apto
    para @st.cache_data) a partir de una tupla de frecuencias ya ordenada
    (hasheable). Evita recalcular el layout de la nube de palabras —
    relativamente costoso — si counter y top_n no cambiaron entre reruns.
    """
    wc = WordCloud(
        width=800, height=500, background_color="white",
        colormap="Reds", max_words=max_words, min_font_size=10,
    ).generate_from_frequencies(dict(freqs))
    return wc.to_array()


def render_terminos():
    _render_section_header(
        "Términos de odio más frecuentes",
        "Términos detectados en mensajes candidatos a odio; por defecto se filtran lemas neutros o genéricos.",
    )

    opts = load_filter_options(_role_can_access_raw())

    fc1, fc2, fc3, fc4, fc5 = st.columns([1, 1, 1, 1, 1])
    sel_platforms = fc1.multiselect(
        "Plataforma", opts["platforms"], default=[], key="term_plat",
        format_func=platform_label,
        placeholder="Todas las plataformas",
    )
    sel_medios = fc2.multiselect(
        "Medio", opts["medios"], default=[], key="term_med",
        placeholder="Todos los medios",
    )
    sel_cats = fc3.multiselect(
        "Categoría de odio",
        options=list(CATEGORIAS_LABELS.keys()),
        format_func=lambda x: CATEGORIAS_LABELS.get(x, x),
        default=[], key="term_cat",
        placeholder="Todas las categorías",
    )
    PERIODO_OPTIONS = {"Todo": None, "24 hs": 24, "48 hs": 48, "72 hs": 72}
    sel_periodo = fc4.selectbox(
        "Período", options=list(PERIODO_OPTIONS.keys()), index=0, key="term_periodo",
    )
    solo_candidatos = fc5.checkbox(
        "Solo candidatos a odio",
        value=True,
        key="term_cand",
        help="Incluye mensajes con candidato a odio o con coincidencia en el lexicón (útil para YouTube).",
    )

    filtro_neutros = st.checkbox(
        "Ocultar términos neutros / genéricos (lista oficial)",
        value=True,
        key="term_filtro_neutros",
        help=(
            "Excluye lemas definidos en el repositorio (terminos_exclusion_oficial.py). "
            "Para ampliar la lista: JSON + sync en automatizacion_diaria."
        ),
    )
    st.caption(
        "**Período:** usa la fecha de **ingreso al sistema** (`processed_at`), no solo la publicación del mensaje, "
        "para que comentarios de YouTube recién cargados aparezcan en 24/48/72 h."
    )

    df = load_terminos(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats) if sel_cats else None,
        solo_candidatos=solo_candidatos,
        ultimas_horas=PERIODO_OPTIONS[sel_periodo],
    )

    if df.empty:
        st.warning("No hay términos detectados con los filtros seleccionados.")
        return

    counter, n_tokens_antes, n_exclusion = compute_terminos_counter(
        platforms=tuple(sel_platforms) if sel_platforms else None,
        medios=tuple(sel_medios) if sel_medios else None,
        categorias=tuple(sel_cats) if sel_cats else None,
        solo_candidatos=solo_candidatos,
        ultimas_horas=PERIODO_OPTIONS[sel_periodo],
        filtro_neutros=filtro_neutros,
    )

    if filtro_neutros and n_exclusion == 0:
        st.warning(
            "La lista de exclusiones oficial está vacía. Revisa el despliegue de `terminos_exclusion_oficial.py`."
        )
    if not counter:
        st.warning(
            "No quedan términos tras aplicar el filtro. "
            "Desactiva «Ocultar términos neutros» o amplía plataforma/medio/período."
        )
        return
    if filtro_neutros and n_tokens_antes:
        st.caption(
            f"Términos distintos: {len(counter):,} tras filtro ({n_tokens_antes:,} antes; "
            f"{n_exclusion:,} lemas en lista oficial)."
        )

    @st.fragment
    def _render_terminos_resultados():
        _nc = len(counter)
        _max_n = min(50, max(1, _nc))
        _min_n = min(10, _max_n)
        top_n = st.slider(
            "Cantidad de términos",
            _min_n,
            _max_n,
            min(25, _max_n),
            key="term_topn",
        )
        top_terms = counter.most_common(top_n)

        col1, col2 = st.columns([1, 1])

        with col1:
            df_terms = pd.DataFrame(top_terms, columns=["Término", "Frecuencia"])
            fig = px.bar(
                df_terms, x="Frecuencia", y="Término", orientation="h",
                color="Frecuencia",
                color_continuous_scale=[[0, "#FFF5F5"], [0.5, "#F56565"], [1, "#C0392B"]],
                title=f"Top {top_n} términos más frecuentes",
            )
            fig.update_layout(height=max(400, top_n * 22), yaxis=dict(autorange="reversed"), showlegend=False)
            _apply_horizontal_bar_labels(fig)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            if counter:
                freqs = tuple(counter.most_common())  # tupla ordenada y hasheable, para la caché
                wc_array = generate_wordcloud_array(freqs, top_n)

                fig_wc, ax = plt.subplots(figsize=(10, 6))
                ax.imshow(wc_array, interpolation="bilinear")
                ax.axis("off")
                st.pyplot(fig_wc)

        st.markdown("### Detalle")
        df_all = pd.DataFrame(counter.most_common(100), columns=["Término", "Frecuencia"])
        st.dataframe(df_all, use_container_width=True, hide_index=True)

        render_section_exports(
            section_key="terminos_frecuentes",
            section_title="Términos de odio más frecuentes",
            csv_items=[
                ("terminos_top", df_terms if "df_terms" in locals() else pd.DataFrame()),
                ("terminos_detalle", df_all if "df_all" in locals() else pd.DataFrame()),
                ("mensajes_filtrados", df),
            ],
            fig_items=[
                {"title": "Top términos frecuentes", "fig": fig if "fig" in locals() else None, "kind": "plotly"},
                {"title": "Nube de palabras", "fig": fig_wc if "fig_wc" in locals() else None, "kind": "matplotlib"},
            ],
        )

    _render_terminos_resultados()
