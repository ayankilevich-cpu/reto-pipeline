"""Funciones de carga de datos compartidas entre múltiples secciones.

Todas usan @st.cache_data para evitar queries redundantes. Ningún import
proviene de dashboard.py (evita ciclos con el entry point).
"""
from __future__ import annotations

import json
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd
import psycopg2
import psycopg2.pool
import streamlit as st

_HERE = Path(__file__).resolve().parent.parent  # automatizacion_diaria/
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from db_utils import _connection_dsn, get_connection_params
from components.constants import (
    CATEGORIAS_ART510,
    CATEGORIAS_LABELS,
    CATEGORIA_TO_GRUPO_510,
    EXCLUDED_SOURCE_MEDIA,
    LABEL_SOURCE_LABELS,
    PLATFORM_DISPLAY,
    _expand_platforms,
    platform_label,
)

_MEDIOS_JSON_PATH = _HERE / "medios_validos.json"


@st.cache_resource
def _get_connection_pool():
    """Pool de conexiones reutilizado entre reruns de Streamlit.

    minconn=2 / maxconn=12: ahora sirve prácticamente todas las queries de
    lectura y escritura de la app (antes solo un puñado de helpers
    compartidos usaban el pool y el resto abría conexiones sueltas), así
    que necesita más margen para varias sesiones/pestañas concurrentes.
    """
    dsn = _connection_dsn()
    if dsn:
        return psycopg2.pool.ThreadedConnectionPool(2, 12, dsn)
    return psycopg2.pool.ThreadedConnectionPool(2, 12, **get_connection_params())


@contextmanager
def _pooled_conn():
    """Reemplazo de get_conn() para las funciones de este archivo: pide una
    conexión prestada del pool en vez de abrir una nueva cada vez.

    Valida la conexión con un SELECT 1 antes de entregarla — si Neon la
    cerró del lado del servidor mientras estaba inactiva en el pool (scale
    to zero, timeout de idle, etc.), la descarta y pide una conexión nueva
    en el momento, en vez de dejar que la primera query real falle.
    """
    pool = _get_connection_pool()
    conn = pool.getconn()

    # Health-check barato antes de entregarla
    try:
        if conn.closed:
            raise psycopg2.InterfaceError("connection closed")
        with conn.cursor() as _probe:
            _probe.execute("SELECT 1")
    except Exception:
        # Conexión muerta: descartarla del pool (no reciclar) y pedir otra
        try:
            pool.putconn(conn, close=True)
        except Exception:
            pass
        conn = pool.getconn()

    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass  # no tapar el error real si el rollback también falla
        raise
    finally:
        # Si la conexión quedó rota durante el uso, no devolverla como
        # reusable — que el pool la descarte y cree una nueva la próxima vez.
        try:
            broken = bool(conn.closed)
        except Exception:
            broken = True
        pool.putconn(conn, close=broken)


@st.cache_data(ttl=3600)
def _load_valid_media_map() -> Tuple[Set[str], Dict[str, str]]:
    """Carga el JSON con los medios válidos y el mapeo handle → nombre.
    El JSON se genera a partir del Excel maestro y se despliega junto a la app.
    """
    with open(_MEDIOS_JSON_PATH, encoding="utf-8") as f:
        data = json.load(f)
    valid_names: Set[str] = set(data["valid_names"])
    handle_to_name: Dict[str, str] = data["handle_to_name"]
    return valid_names, handle_to_name


def _public_medio_label(source_media: Any) -> Optional[str]:
    """Nombre del medio monitorizado seguro para UI pública.

    Solo devuelve texto si `source_media` está en la lista maestra de medios (o su handle
    mapea a un nombre válido). Cualquier otro valor (p. ej. cuenta personal no catalogada)
    devuelve None para no exponer identificadores.
    """
    if source_media is None:
        return None
    if isinstance(source_media, float) and pd.isna(source_media):
        return None
    sm = str(source_media).strip()
    if not sm:
        return None
    try:
        valid_names, handle_to_name = _load_valid_media_map()
    except Exception:
        return None
    display = handle_to_name.get(sm, sm)
    if display in valid_names:
        return display
    if sm in valid_names:
        return sm
    return None


# ============================================================
# DATA LOADING — filter-aware
# ============================================================
@st.cache_data(ttl=3600)
def load_filter_options(access_raw: bool) -> dict:
    """Load distinct values for all filter dropdowns."""
    platform_sql = (
        "SELECT DISTINCT platform FROM raw.mensajes WHERE platform IS NOT NULL ORDER BY platform"
        if access_raw
        else "SELECT DISTINCT platform FROM processed.mensajes WHERE platform IS NOT NULL ORDER BY platform"
    )
    with _pooled_conn() as conn:
        platforms_raw = pd.read_sql(platform_sql, conn)["platform"].tolist()
        platforms = sorted({PLATFORM_DISPLAY.get(p, p) for p in platforms_raw})
        platform_internal = sorted({
            "x" if p in ("twitter", "x") else p for p in platforms_raw
        })
        platforms = platform_internal

        medios = pd.read_sql(
            "SELECT source_media FROM processed.mensajes "
            "WHERE source_media IS NOT NULL AND source_media != '' "
            "  AND source_media NOT IN %s "
            "GROUP BY source_media "
            "HAVING COUNT(*) >= 100 "
            "ORDER BY source_media", conn,
            params=[tuple(EXCLUDED_SOURCE_MEDIA)],
        )["source_media"].tolist()

        prioridades = pd.read_sql(
            "SELECT DISTINCT priority FROM processed.scores "
            "WHERE priority IS NOT NULL ORDER BY priority", conn
        )["priority"].tolist()

        clasificaciones = pd.read_sql(
            "SELECT DISTINCT clasificacion_principal FROM processed.etiquetas_llm "
            "WHERE clasificacion_principal IS NOT NULL ORDER BY clasificacion_principal", conn
        )["clasificacion_principal"].tolist()

    categorias = list(CATEGORIAS_LABELS.keys())
    intensidades = ["1", "2", "3"]

    return {
        "platforms": platforms,
        "medios": medios,
        "categorias": categorias,
        "intensidades": intensidades,
        "prioridades": prioridades,
        "clasificaciones": clasificaciones,
    }


@st.cache_data(ttl=3600)
def _load_vllm_yt_corrections() -> pd.DataFrame:
    """Carga todas las validaciones humanas de etiquetado LLM en YouTube."""
    try:
        with _pooled_conn() as conn:
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
                WHERE pm.platform = 'youtube'
                ORDER BY v.annotation_date DESC
            """, conn)
    except Exception:
        return pd.DataFrame()
    return df


@st.cache_data(ttl=3600)
def load_art510_summary() -> dict:
    """KPIs generales de Art. 510 (sin filtros)."""
    with _pooled_conn() as conn:
        cur = conn.cursor()

        try:
            cur.execute("SELECT COUNT(*) FROM processed.evaluacion_art510")
            total_evaluados = cur.fetchone()[0]
        except Exception:
            conn.rollback()
            total_evaluados = 0

        try:
            cur.execute("""
                SELECT COUNT(*) FROM processed.evaluacion_art510
                WHERE es_potencial_delito = TRUE
            """)
            total_delitos = cur.fetchone()[0]
        except Exception:
            conn.rollback()
            total_delitos = 0

        try:
            cur.execute("SELECT COUNT(*) FROM processed.validacion_art510_humana")
            total_validados = cur.fetchone()[0]
        except Exception:
            conn.rollback()
            total_validados = 0

        total_confirmados = 0
        total_rechazados = 0
        try:
            cur.execute("""
                SELECT validacion_humana, COUNT(*)
                FROM processed.validacion_art510_humana
                GROUP BY validacion_humana
            """)
            for val, cnt in cur.fetchall():
                if val == "confirmado":
                    total_confirmados = cnt
                elif val == "rechazado":
                    total_rechazados = cnt
        except Exception:
            conn.rollback()

        cur.close()

    return {
        "total_evaluados": total_evaluados,
        "total_delitos": total_delitos,
        "total_validados": total_validados,
        "total_confirmados": total_confirmados,
        "total_rechazados": total_rechazados,
    }


@st.cache_data(ttl=3600)
def load_art510_validaciones_humanas() -> pd.DataFrame:
    """Carga validaciones humanas de Art. 510 cruzadas con la evaluación LLM."""
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT
                vh.message_uuid,
                vh.validacion_humana,
                vh.apartado_510_final,
                vh.grupo_protegido_final,
                vh.conducta_final,
                vh.comentario,
                vh.annotator_id,
                vh.annotation_date,
                ea.es_potencial_delito   AS llm_potencial_delito,
                ea.apartado_510          AS llm_apartado,
                ea.conducta_detectada    AS llm_conducta,
                ea.grupo_protegido       AS llm_grupo,
                ea.confianza             AS llm_confianza,
                ea.justificacion         AS llm_justificacion,
                pm.content_original,
                pm.platform,
                pm.source_media
            FROM processed.validacion_art510_humana vh
            LEFT JOIN processed.evaluacion_art510 ea USING (message_uuid)
            LEFT JOIN processed.mensajes pm USING (message_uuid)
            ORDER BY vh.validacion_humana DESC, vh.annotation_date DESC
        """, conn)
    if not df.empty:
        df["platform_label"] = df["platform"].map(PLATFORM_DISPLAY).fillna(df["platform"])
    return df


@st.cache_data(ttl=3600)
def load_art510_candidates(
    platforms: Optional[Tuple] = None,
    label_sources: Optional[Tuple] = None,
) -> pd.DataFrame:
    """
    Carga candidatos a Art. 510 desde gold dataset y etiquetas LLM.

    Mensajes ODIO cuya categoría mapea a grupos protegidos del Art. 510.
    Se usa como vista previa cuando aún no se ha ejecutado evaluar_art510.py.
    """
    platforms = _expand_platforms(list(platforms) if platforms else None)
    dfs = []

    with _pooled_conn() as conn:
        # --- Fuente LLM ---
        if not label_sources or "llm" in label_sources:
            plat_cond = ""
            params_llm: list = [tuple(CATEGORIAS_ART510)]
            if platforms:
                plat_cond = "AND pm.platform IN %s"
                params_llm.append(tuple(platforms))

            df_llm = pd.read_sql(f"""
                SELECT pm.message_uuid,
                       'llm' AS label_source,
                       pm.content_original,
                       pm.platform,
                       pm.source_media,
                       e.categoria_odio_pred AS categoria,
                       e.intensidad_pred AS intensidad,
                       e.resumen_motivo AS motivo_etiquetado
                FROM processed.mensajes pm
                JOIN processed.etiquetas_llm e USING (message_uuid)
                WHERE e.clasificacion_principal = 'ODIO'
                  AND e.categoria_odio_pred IN %s
                  {plat_cond}
                ORDER BY e.intensidad_pred DESC NULLS LAST
            """, conn, params=params_llm)
            dfs.append(df_llm)

        # --- Fuente Humano (gold + validaciones) ---
        if not label_sources or "humano" in label_sources:
            plat_cond = ""
            params_h: list = [tuple(CATEGORIAS_ART510)]
            if platforms:
                plat_cond = "AND pm.platform IN %s"
                params_h.append(tuple(platforms))

            df_human = pd.read_sql(f"""
                SELECT pm.message_uuid,
                       'humano' AS label_source,
                       pm.content_original,
                       pm.platform,
                       pm.source_media,
                       COALESCE(g.y_categoria_final, v.categoria_odio) AS categoria,
                       COALESCE(g.y_intensidad_final::text, v.intensidad::text) AS intensidad,
                       'Validación humana' AS motivo_etiquetado
                FROM processed.mensajes pm
                LEFT JOIN processed.validaciones_manuales v USING (message_uuid)
                LEFT JOIN processed.gold_dataset g USING (message_uuid)
                WHERE (v.odio_flag = TRUE OR g.y_odio_bin = 1)
                  AND COALESCE(g.y_categoria_final, v.categoria_odio) IN %s
                  {plat_cond}
            """, conn, params=params_h)
            dfs.append(df_human)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    df = df.drop_duplicates(subset=["message_uuid", "label_source"])

    # Pre-filtro Art. 510: solo categorías que mapean a grupos protegidos
    df = df[df["categoria"].isin(CATEGORIAS_ART510)].copy()

    if not df.empty:
        df["platform_label"] = df["platform"].map(platform_label)
        df["source_label"] = df["label_source"].map(
            lambda x: LABEL_SOURCE_LABELS.get(x, x)
        )
        df["grupo_protegido_estimado"] = df["categoria"].map(
            lambda x: CATEGORIA_TO_GRUPO_510.get(x, x)
        )
        df["categoria_label"] = df["categoria"].map(
            lambda x: CATEGORIAS_LABELS.get(x, x)
        )

    return df


@st.cache_data(ttl=3600)
def load_gold_full() -> pd.DataFrame:
    """Carga el gold dataset unido con validaciones manuales y etiquetas LLM."""
    with _pooled_conn() as conn:
        df = pd.read_sql("""
            SELECT
                g.message_uuid,
                pm.platform,
                g.y_odio_final,
                g.y_odio_bin,
                g.y_categoria_final,
                g.y_intensidad_final,
                g.corrigio_odio,
                g.corrigio_categoria,
                g.corrigio_intensidad,
                g.label_source,
                g.split,
                v.odio_flag       AS human_odio,
                v.categoria_odio  AS human_categoria,
                v.intensidad      AS human_intensidad,
                v.humor_flag      AS human_humor,
                v.annotator_id,
                v.coincide_con_llm,
                e.clasificacion_principal AS llm_clasif,
                e.categoria_odio_pred     AS llm_categoria,
                e.intensidad_pred         AS llm_intensidad,
                e.resumen_motivo          AS llm_motivo
            FROM processed.gold_dataset g
            LEFT JOIN processed.mensajes pm USING (message_uuid)
            LEFT JOIN processed.validaciones_manuales v USING (message_uuid)
            LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
            ORDER BY g.message_uuid
        """, conn)
    # Etiquetas de plataforma legibles
    df["platform_label"] = df["platform"].map(
        {"x": "X", "twitter": "X", "youtube": "YouTube"}
    ).fillna(df["platform"])
    df["split"] = df["split"].fillna("sin_asignar")
    df["annotator_id"] = df["annotator_id"].fillna("sin_asignar")
    return df
