"""
Dashboard RETO — Monitorización de discurso de odio en redes sociales.

Entry point puro: configuración, autenticación, navegación (sidebar) y routing.
La lógica de cada sección vive en `secciones/`; los helpers compartidos en
`components/`.

Uso:
  streamlit run automatizacion_diaria/dashboard.py   (desde la raíz del repo)
"""

from __future__ import annotations

import logging as _logging
import os
import sys
import threading
import time as _time
from pathlib import Path
from typing import Callable, Dict, Optional

import streamlit as st

# ── Timing diagnóstico (solo activo con RETO_DEBUG_TIMING=1) ──────────
_DEBUG_TIMING = os.environ.get("RETO_DEBUG_TIMING", "0") == "1"
_T_PROCESS_START = _time.perf_counter()

# Logger dedicado → stderr → capturado por el log del servidor (local y Streamlit Cloud).
# Solo se configura cuando RETO_DEBUG_TIMING=1 para evitar overhead en producción.
if _DEBUG_TIMING:
    _timing_log = _logging.getLogger("reto.timing")
    _timing_log.setLevel(_logging.INFO)
    if not _timing_log.handlers:          # evita duplicados en reloads de Streamlit
        _h = _logging.StreamHandler()     # escribe a stderr
        _h.setFormatter(_logging.Formatter("%(message)s"))
        _timing_log.addHandler(_h)
        _timing_log.propagate = False     # no hereda el handler raíz de Streamlit


def _tlog(phase: str, t0: float) -> None:
    """Escribe [TIMING] al log del servidor; activo solo con RETO_DEBUG_TIMING=1."""
    if _DEBUG_TIMING:
        _timing_log.info(
            "[TIMING] %s: %.0f ms", phase, (_time.perf_counter() - t0) * 1000
        )

_AUTO_DIR = Path(__file__).resolve().parent
if str(_AUTO_DIR) not in sys.path:
    sys.path.insert(0, str(_AUTO_DIR))

# ── Shell y tema ──────────────────────────────────
from components.auth import _check_auth, _render_login
from components.theme import (
    _inject_global_css,
    _register_plotly_theme,
    _scroll_main_to_top,
    render_footer,
)
from components.layout import _ensure_db_connection, render_sidebar

# ── Secciones ─────────────────────────────────────
from secciones.proyecto_reto import render_proyecto
from secciones.panel_general import render_panel_general
from secciones.categorias_odio import render_categorias
from secciones.ranking_medios import render_ranking_medios
from secciones.analisis_contextual import render_analisis_contextual
from secciones.semaforo_diario import render_semaforo, load_semaforo_diario
from secciones.comparativa_modelos import render_comparativa
from secciones.calidad_llm import render_calidad_llm
from secciones.terminos_frecuentes import render_terminos
from secciones.buscador_analisis import render_buscador_terminos
from secciones.gold_dataset import render_gold_dataset_router
from secciones.analisis_510 import render_analisis_art510
from secciones.anotacion_validacion import render_anotacion
from secciones.delitos_odio import render_delitos

# ============================================================
# CONFIG
# ============================================================
st.set_page_config(
    page_title="RETO — Dashboard",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

_register_plotly_theme()

# ============================================================
# PRECALENTADO DE CACHÉ (una vez por proceso)
# ============================================================
def _firma_ultima_corrida() -> Optional[str]:
    """MAX(GREATEST(pipeline_runs.started_at, pipeline_health.run_at)).

    Devuelve None si la DB no es accesible o las tablas no existen aún.
    """
    from components.db_helpers import _pooled_conn
    try:
        with _pooled_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT GREATEST(
                        (SELECT MAX(started_at) FROM processed.pipeline_runs),
                        (SELECT MAX(run_at)     FROM processed.pipeline_health)
                    )
                """)
                row = cur.fetchone()
                return str(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _warm_wordcloud(compute_fn, wc_fn) -> None:
    """Nube por defecto: mismos argumentos y top_n que render_terminos sin tocar widgets."""
    counter, _, _ = compute_fn(
        platforms=None, medios=None, categorias=None,
        solo_candidatos=True, ultimas_horas=None, filtro_neutros=True,
    )
    if counter:
        _max_n = min(50, max(1, len(counter)))
        wc_fn(tuple(counter.most_common()), min(25, _max_n))


def _precalentar_cache() -> None:
    """Ejecuta load_* de secciones públicas para llenar @st.cache_data.

    Los args deben coincidir exactamente con los que usa cada render_* en
    la primera visita del viewer (sin filtros aplicados). Best-effort —
    un fallo individual no detiene el resto.
    """
    from components.db_helpers import _load_valid_media_map, load_filter_options, load_gold_full
    from secciones.panel_general import (
        load_kpis, load_embudo, _load_panel_combined,
        load_gold_stats, load_matriz_confusion,
    )
    from secciones.categorias_odio import (
        load_llm_stats, load_categorias,
        load_intensidad_por_categoria, load_resultado_clasificacion,
    )
    from secciones.ranking_medios import load_ranking_medios, _load_ranking_medios_raw
    from secciones.analisis_contextual import load_analisis_semanal
    from secciones.semaforo_diario import load_semaforo_diario
    from secciones.terminos_frecuentes import load_terminos, compute_terminos_counter, generate_wordcloud_array
    from secciones.delitos_odio import (
        load_crime_totals, load_crime_solved, load_authors_age,
        load_investigations_sex, load_fiscalia_motives, load_fiscalia_articles,
    )

    llamadas = [
        ("load_filter_options",           lambda: load_filter_options(False)),
        ("load_kpis",                     lambda: load_kpis(False, None, None)),
        ("load_embudo",                   lambda: load_embudo(None, None)),
        ("_load_panel_combined",          lambda: _load_panel_combined(None, None)),
        ("load_gold_stats",               lambda: load_gold_stats()),
        ("load_matriz_confusion",         lambda: load_matriz_confusion()),
        ("load_llm_stats",                lambda: load_llm_stats()),
        ("load_categorias",               lambda: load_categorias(None, None, None)),
        ("load_intensidad_por_categoria", lambda: load_intensidad_por_categoria(None, None, None)),
        ("load_resultado_clasificacion",  lambda: load_resultado_clasificacion(None, None)),
        ("load_ranking_medios",           lambda: load_ranking_medios(fecha_desde=None, fecha_hasta=None)),
        ("_load_valid_media_map",         lambda: _load_valid_media_map()),
        ("_load_ranking_medios_raw",      lambda: _load_ranking_medios_raw(min_msgs=1)),
        ("load_analisis_semanal",         lambda: load_analisis_semanal()),
        ("load_semaforo_diario",          lambda: load_semaforo_diario()),
        ("load_terminos",                 lambda: load_terminos(platforms=None, medios=None, categorias=None, solo_candidatos=True, ultimas_horas=None)),
        ("compute_terminos_counter",      lambda: compute_terminos_counter(platforms=None, medios=None, categorias=None, solo_candidatos=True, ultimas_horas=None, filtro_neutros=True)),
        ("generate_wordcloud_array",      lambda: _warm_wordcloud(compute_terminos_counter, generate_wordcloud_array)),
        ("load_gold_full",                lambda: load_gold_full()),
        ("load_crime_totals",             lambda: load_crime_totals()),
        ("load_crime_solved",             lambda: load_crime_solved()),
        ("load_authors_age",              lambda: load_authors_age()),
        ("load_investigations_sex",       lambda: load_investigations_sex()),
        ("load_fiscalia_motives",         lambda: load_fiscalia_motives()),
        ("load_fiscalia_articles",        lambda: load_fiscalia_articles()),
    ]

    _log = _logging.getLogger("reto.warmup")
    ok = 0
    for nombre, fn in llamadas:
        try:
            t = _time.perf_counter()
            fn()
            _tlog(f"warmup.{nombre}", t)
            ok += 1
        except Exception as exc:
            _log.warning("[WARMUP] %s: %s", nombre, exc)
    _tlog("warmup.TOTAL (desde inicio proceso)", _T_PROCESS_START)
    _logging.getLogger("reto.warmup").warning("[WARMUP] %d/%d loaders completados", ok, len(llamadas))


def _warmer_loop() -> None:
    """Bucle daemon: warmup inicial + refresco horario + invalidación por pipeline."""
    try:
        from zoneinfo import ZoneInfo
    except ImportError:
        from backports.zoneinfo import ZoneInfo  # type: ignore[no-redef]
    import datetime as _dt

    _MADRID = ZoneInfo("Europe/Madrid")
    _log = _logging.getLogger("reto.warmup")
    _firma_cache: Optional[str] = None

    try:
        _precalentar_cache()
    except Exception as exc:
        _log.warning("[WARMUP] warmup inicial fallido: %s", exc)
    _firma_cache = _firma_ultima_corrida()

    while True:
        _time.sleep(3600)
        try:
            if not (7 <= _dt.datetime.now(_MADRID).hour <= 23):
                continue

            firma = _firma_ultima_corrida()
            if firma is not None and firma != _firma_cache:
                _log.warning("[WARMUP] nueva corrida detectada (%s), limpiando caché…", firma)
                st.cache_data.clear()
                _firma_cache = firma

            _precalentar_cache()
        except Exception as exc:
            _log.warning("[WARMUP] iteración de warmer fallida: %s", exc)


@st.cache_resource(show_spinner=False)
def _iniciar_warmer() -> bool:
    """Arranca el warmer en background una sola vez por proceso."""
    threading.Thread(target=_warmer_loop, daemon=True).start()
    return True


# ============================================================
# ROUTING
# ============================================================
_SECTION_RENDERERS: Dict[str, Callable[[], None]] = {
    "Proyecto ReTo": render_proyecto,
    "Panel general": render_panel_general,
    "Categorías de odio (LLM)": render_categorias,
    "Ranking de medios": render_ranking_medios,
    "Análisis contextual": render_analisis_contextual,
    "Semáforo diario": render_semaforo,
    "Comparativa modelos": render_comparativa,
    "Calidad LLM": render_calidad_llm,
    "Términos frecuentes": render_terminos,
    "Buscador y Análisis": render_buscador_terminos,
    "Dataset Gold": render_gold_dataset_router,
    "Análisis Art. 510": render_analisis_art510,
    "Anotación y validación": render_anotacion,
    "Delitos de odio (oficial)": render_delitos,
}


# ============================================================
# MAIN
# ============================================================
def main() -> None:
    _tlog("main.start (desde inicio proceso)", _T_PROCESS_START)

    # Arranca en background; no bloquea el primer render.
    _iniciar_warmer()

    t = _time.perf_counter(); _inject_global_css();  _tlog("main._inject_global_css", t)
    t = _time.perf_counter(); _check_auth();         _tlog("main._check_auth", t)
    if st.session_state.get("_show_login_form") and st.session_state.get(
        "user_role"
    ) not in ("admin", "editor"):
        _render_login()
        return

    t = _time.perf_counter(); section = render_sidebar(); _tlog("main.render_sidebar", t)
    prev_section = st.session_state.get("_nav_section")
    section_changed = prev_section != section
    st.session_state["_nav_section"] = section

    # Proyecto ReTo no usa BD; el resto sí (evita bloqueo 30 min sin secrets)
    t = _time.perf_counter()
    if section != "Proyecto ReTo" and not _ensure_db_connection():
        _tlog("main._ensure_db_connection (FALLÓ)", t)
        return
    _tlog("main._ensure_db_connection", t)

    _nav_count = st.session_state.get("_nav_count", 0) + (1 if section_changed else 0)
    st.session_state["_nav_count"] = _nav_count
    if section_changed:
        _scroll_main_to_top(_nav_count)

    renderer = _SECTION_RENDERERS.get(section)
    if renderer:
        t = _time.perf_counter(); renderer(); _tlog(f"main.render[{section}]", t)
    else:
        st.error(
            f"Sección no reconocida: {section!r}. "
            "Recargá la página o informá al administrador."
        )

    render_footer()


if __name__ == "__main__":
    main()
