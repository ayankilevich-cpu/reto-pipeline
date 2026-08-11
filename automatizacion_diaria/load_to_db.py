#!/usr/bin/env python3
"""
load_to_db.py — Carga los CSVs generados por el pipeline a PostgreSQL (reto_db).

Se ejecuta como último paso del pipeline diario.
Lee los CSVs de salida de cada script y hace UPSERT en las tablas correspondientes.

Los CSVs siguen generándose igual (respaldo físico). Este script solo añade
la escritura a PostgreSQL como destino adicional.

Tablas destino:
  raw.mensajes              ← reto_x_master.csv + youtube_hatemedia_comments_30d.csv
  processed.mensajes        ← reto_x_master_anon.csv + youtube_..._tagged_full.csv
  processed.scores          ← x_manual_label_scored.csv
  processed.etiquetas_llm   ← etiquetado_llm_completo.csv

Uso:
  python load_to_db.py
"""

from __future__ import annotations

import hashlib
import logging
import os
import sys
import time
import uuid as uuidlib
from datetime import datetime
from glob import glob
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import psycopg2

# Añadir este directorio al path para importar db_utils
sys.path.insert(0, str(Path(__file__).parent))
from db_utils import get_conn, upsert_rows

# Reintentos ante errores TRANSITORIOS de conexión durante un loader (p. ej.
# "SSL connection has been closed unexpectedly" a mitad de un upsert grande).
# OperationalError/InterfaceError son los tipos de psycopg2 para problemas de
# conexión — NO cubren errores de datos/esquema (FK, NOT NULL, tipo…), que no
# tiene sentido reintentar porque fallarían igual.
LOAD_DB_MAX_RETRIES = int(os.getenv("LOAD_DB_MAX_RETRIES", "3"))
LOAD_DB_RETRY_BACKOFF_SEC = float(os.getenv("LOAD_DB_RETRY_BACKOFF_SEC", "5"))
_TRANSIENT_DB_ERRORS = (psycopg2.OperationalError, psycopg2.InterfaceError)

# ============================================================
# CONFIGURACIÓN DE RUTAS
# ============================================================
SCRIPT_DIR = Path(__file__).resolve().parent
RETO_ROOT = Path(os.getenv("PROJECT_ROOT", str(SCRIPT_DIR.parent)))

CSV_RAW_MASTER = Path(os.getenv("CSV_RAW_MASTER", str(RETO_ROOT / "X_Mensajes" / "data" / "master" / "reto_x_master.csv")))
# Orquestador unificado: x_full_anon.csv reemplaza a reto_x_master_anon.csv
CSV_ANON = Path(os.getenv("CSV_ANON", str(RETO_ROOT / "outputs" / "pipeline_unificado" / "x" / "x_full_anon.csv")))
# Orquestador unificado: x_scored.csv reemplaza a x_manual_label_scored.csv
CSV_SCORED = Path(os.getenv("CSV_SCORED", str(RETO_ROOT / "outputs" / "pipeline_unificado" / "x" / "x_scored.csv")))

LLM_OUTPUT_GLOB = os.getenv("LLM_OUTPUT_GLOB", str(RETO_ROOT / "Medios" / "ML" / "etiquetado_llm" / "outputs" / "*" / "etiquetado_llm_completo.csv"))

CSV_YT_RAW = Path(os.getenv("CSV_YT_RAW", str(RETO_ROOT / "Medios" / "youtube_hatemedia_comments_30d.csv")))
# Orquestador unificado: youtube_full_anon.csv reemplaza a youtube_hatemedia_comments_30d_tagged_full.csv
CSV_YT_TAGGED = Path(os.getenv("CSV_YT_TAGGED", str(RETO_ROOT / "outputs" / "pipeline_unificado" / "youtube" / "youtube_full_anon.csv")))

CSV_ART510 = Path(os.getenv("CSV_ART510", str(RETO_ROOT / "Medios" / "ML" / "etiquetado_llm" / "outputs" / "art510" / "evaluacion_art510.csv")))

CSV_LLM_YOUTUBE = Path(os.getenv(
    "CSV_LLM_YOUTUBE",
    str(RETO_ROOT / "Medios" / "ML" / "etiquetado_llm" / "outputs" / "youtube" / "etiquetado_llm_youtube.csv"),
))

# UUID v5 namespace para YouTube (determinístico, mismo que gold loaders)
RETO_YT_NS = uuidlib.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")


def yt_to_uuid(comment_id: str) -> str:
    """Convierte un comment_id de YouTube a UUID v5 determinístico."""
    return str(uuidlib.uuid5(RETO_YT_NS, str(comment_id)))


def sha256_hash(value) -> Optional[str]:
    """SHA-256 hash para anonimizar campos de autor (compatible con tag_youtube_hate_auto.py)."""
    if pd.isna(value):
        return None
    return hashlib.sha256(str(value).encode()).hexdigest()


def setup_logging() -> logging.Logger:
    logger = logging.getLogger("load_to_db")
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    log_dir = SCRIPT_DIR / "logs"
    log_dir.mkdir(exist_ok=True)
    date_str = datetime.now().strftime("%Y-%m-%d")
    fh = logging.FileHandler(log_dir / f"load_db_{date_str}.log", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


def find_llm_csv() -> Optional[Path]:
    """Busca el CSV de etiquetado LLM más reciente."""
    candidates = glob(LLM_OUTPUT_GLOB)
    if not candidates:
        return None
    # Ordenar por fecha de modificación, el más reciente primero
    candidates.sort(key=os.path.getmtime, reverse=True)
    return Path(candidates[0])


def safe_val(val, target_type="str"):
    """Convierte un valor a tipo seguro para PostgreSQL, manejando NaN/None."""
    if pd.isna(val):
        return None
    if target_type == "int":
        try:
            return int(val)
        except (ValueError, TypeError):
            return None
    if target_type == "float":
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
    if target_type == "bool":
        if isinstance(val, bool):
            return val
        s = str(val).strip().lower()
        return s in ("true", "1", "yes", "si", "sí")
    return str(val).strip() if val is not None else None


def _normalize_platform(val) -> Optional[str]:
    """Unifica 'twitter' → 'x' para evitar duplicados de plataforma."""
    s = safe_val(val)
    if s and s.lower() in ("twitter", "x"):
        return "x"
    return s


def extract_x_handle(url: Optional[str]) -> Optional[str]:
    """Extrae el handle de Twitter/X de una URL tipo twitter.com/{handle}/status/..."""
    if not url or "twitter.com/" not in url:
        return None
    parts = url.split("twitter.com/", 1)
    if len(parts) < 2:
        return None
    handle = parts[1].split("/")[0].strip()
    return handle if handle else None


def resolve_source_media(row) -> Optional[str]:
    """Devuelve source_media del CSV, o lo extrae de la URL si viene vacío."""
    media = safe_val(row.get("source_media"))
    if media:
        return media
    return extract_x_handle(safe_val(row.get("url")))


# ============================================================
# CARGADORES POR TABLA
# ============================================================

def load_raw_mensajes(conn, logger: logging.Logger) -> int:
    """Carga raw.mensajes desde el master CSV."""
    if not CSV_RAW_MASTER.exists():
        logger.warning("No existe %s — saltando raw.mensajes", CSV_RAW_MASTER)
        return 0

    df = pd.read_csv(CSV_RAW_MASTER, dtype=str)
    logger.info("raw.mensajes: %d filas leídas de CSV", len(df))

    columns = [
        "message_uuid", "platform", "tweet_id", "created_at", "content_original",
        "author_username", "author_id", "source_media", "batch_id", "scrape_date",
        "language", "url", "retweet_count", "reply_count", "like_count", "quote_count",
    ]

    rows = []
    for _, r in df.iterrows():
        rows.append((
            safe_val(r.get("message_uuid")),
            _normalize_platform(r.get("platform")),
            safe_val(r.get("tweet_id")),
            safe_val(r.get("created_at")),
            safe_val(r.get("content_original")),
            safe_val(r.get("author_username")),
            safe_val(r.get("author_id")),
            resolve_source_media(r),
            safe_val(r.get("batch_id")),
            safe_val(r.get("scrape_date")),
            safe_val(r.get("language")),
            safe_val(r.get("url")),
            safe_val(r.get("retweet_count"), "int"),
            safe_val(r.get("reply_count"), "int"),
            safe_val(r.get("like_count"), "int"),
            safe_val(r.get("quote_count"), "int"),
        ))

    # Filtrar filas sin UUID o sin content_original (NOT NULL en BD)
    rows_before = len(rows)
    rows = [r for r in rows if r[0] is not None and r[4] is not None]
    skipped = rows_before - len(rows)
    if skipped:
        logger.warning("raw.mensajes: %d filas descartadas (sin uuid o content_original)", skipped)

    n = upsert_rows(
        conn, "raw.mensajes", columns, rows,
        conflict_columns=["message_uuid"],
        update_columns=[c for c in columns if c != "message_uuid"],
    )
    logger.info("raw.mensajes: %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_processed_mensajes(conn, logger: logging.Logger) -> int:
    """Carga processed.mensajes desde x_full_anon.csv del orquestador.

    El orquestador produce columnas canónicas; se mapean aquí a los nombres
    legacy de la BD para no romper las queries del dashboard:
      published_at   → created_at
      hate_candidate → has_hate_terms_match  (1/0 → bool)
      hate_types     → candidate_reason      (motivos del match)
      (sin equivalente) → strong_phrase = False
    """
    if not CSV_ANON.exists():
        logger.warning("No existe %s — saltando processed.mensajes", CSV_ANON)
        return 0

    df = pd.read_csv(CSV_ANON, dtype=str)
    logger.info("processed.mensajes: %d filas leídas de CSV", len(df))

    # Mapa message_uuid → ingested_at desde raw.mensajes (cargado antes en el
    # pipeline). Garantiza que el INSERT use la fecha real de ingreso al
    # sistema, no NOW(), para que el KPI "nuevos hoy" del viewer sea correcto.
    _cur = conn.cursor()
    _cur.execute("SELECT message_uuid::text, ingested_at FROM raw.mensajes")
    ingested_map = dict(_cur.fetchall())
    _cur.close()

    columns = [
        "message_uuid", "platform", "content_original", "source_media",
        "created_at", "language", "url",
        "author_id_anon", "author_username_anon",
        "matched_terms", "has_hate_terms_match", "match_count",
        "strong_phrase", "is_candidate", "candidate_reason",
        "processed_at", "ingested_at",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid_str = safe_val(r.get("message_uuid"))
        rows.append((
            uuid_str,
            _normalize_platform(r.get("platform")),
            safe_val(r.get("content_original")),
            resolve_source_media(r),
            # orquestador: published_at → BD: created_at
            safe_val(r.get("published_at") or r.get("created_at")),
            safe_val(r.get("language")),
            safe_val(r.get("url")),
            safe_val(r.get("author_id_anon")),
            safe_val(r.get("author_username_anon")),
            safe_val(r.get("matched_terms")),
            # orquestador: hate_candidate (0/1) → BD: has_hate_terms_match (bool)
            safe_val(r.get("hate_candidate") or r.get("has_hate_terms_match"), "bool"),
            safe_val(r.get("match_count"), "int"),
            # sin equivalente en orquestador; mantener False para compatibilidad BD
            False,
            safe_val(r.get("is_candidate"), "bool"),
            # orquestador: hate_types → BD: candidate_reason
            safe_val(r.get("hate_types") or r.get("candidate_reason")),
            safe_val(r.get("processed_at")),
            ingested_map.get(uuid_str),
        ))

    rows_before = len(rows)
    rows = [r for r in rows if r[0] is not None and r[2] is not None]
    skipped = rows_before - len(rows)
    if skipped:
        logger.warning("processed.mensajes: %d filas descartadas (sin uuid o content_original)", skipped)

    # processed_at e ingested_at son metadatos de ingesta: solo se escriben en
    # INSERT, nunca en UPDATE (evita pico falso de "nuevos hoy" tras reproceso).
    _no_update_dates = {"message_uuid", "processed_at", "ingested_at"}
    n = upsert_rows(
        conn, "processed.mensajes", columns, rows,
        conflict_columns=["message_uuid"],
        update_columns=[c for c in columns if c not in _no_update_dates],
    )
    logger.info("processed.mensajes: %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_scores(conn, logger: logging.Logger) -> int:
    """Carga processed.scores desde x_scored.csv del orquestador.

    El orquestador produce columnas prefilter_*; se mapean a los nombres
    legacy de la BD para no romper las queries del dashboard:
      prefilter_score    → proba_odio
      prefilter_pass     → pred_odio  (True→1, False→0)
      prefilter_priority → priority
      prefilter_date     → score_date
      prefilter_method   → model_version
    """
    if not CSV_SCORED.exists():
        logger.warning("No existe %s — saltando processed.scores", CSV_SCORED)
        return 0

    df = pd.read_csv(CSV_SCORED, dtype=str)
    logger.info("processed.scores: %d filas leídas de CSV", len(df))

    columns = [
        "message_uuid", "model_version", "proba_odio", "pred_odio",
        "priority", "score_date",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid = safe_val(r.get("message_uuid"))
        if uuid is None:
            continue
        # Soporta tanto columnas del orquestador (prefilter_*) como legacy
        model_v = (safe_val(r.get("prefilter_method"))
                   or safe_val(r.get("model_version"))
                   or "tfidf_logreg")
        proba = safe_val(r.get("prefilter_score") or r.get("proba_odio"), "float")
        # prefilter_pass es bool (True/False); pred_odio espera entero (1/0)
        pass_raw = r.get("prefilter_pass") or r.get("pred_odio")
        pred = 1 if safe_val(pass_raw, "bool") else 0
        priority = (safe_val(r.get("prefilter_priority"))
                    or safe_val(r.get("priority")))
        score_dt = (safe_val(r.get("prefilter_date"))
                    or safe_val(r.get("score_date")))
        rows.append((uuid, model_v, proba, pred, priority, score_dt))

    n = upsert_rows(
        conn, "processed.scores", columns, rows,
        conflict_columns=["message_uuid", "model_version"],
        update_columns=["proba_odio", "pred_odio", "priority", "score_date"],
    )
    logger.info("processed.scores: %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_etiquetas_llm(conn, logger: logging.Logger) -> int:
    """Carga processed.etiquetas_llm desde el CSV del LLM."""
    csv_path = find_llm_csv()
    if csv_path is None:
        logger.warning("No se encontró CSV del LLM en %s — saltando", LLM_OUTPUT_GLOB)
        return 0

    import csv as _csv
    _VALID_CLASIF = {"ODIO", "NO_ODIO", "DUDOSO"}
    with open(csv_path, encoding="utf-8") as _f:
        reader = _csv.DictReader(_f)
        rows_raw = [
            r for r in reader
            if r.get("clasificacion_principal", "") in _VALID_CLASIF
        ]
    df = pd.DataFrame(rows_raw).astype(str) if rows_raw else pd.DataFrame()
    logger.info("processed.etiquetas_llm: %d filas válidas de %s", len(df), csv_path.name)

    columns = [
        "message_uuid", "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo", "llm_version",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid = safe_val(r.get("message_uuid"))
        if uuid is None:
            continue
        rows.append((
            uuid,
            safe_val(r.get("clasificacion_principal")),
            safe_val(r.get("categoria_odio_pred")),
            safe_val(r.get("intensidad_pred")),
            safe_val(r.get("resumen_motivo")),
            "v1",  # versión del LLM; cambiar cuando iteres el modelo/prompt
        ))

    n = upsert_rows(
        conn, "processed.etiquetas_llm", columns, rows,
        conflict_columns=["message_uuid", "llm_version"],
        update_columns=["clasificacion_principal", "categoria_odio_pred",
                        "intensidad_pred", "resumen_motivo"],
    )
    logger.info("processed.etiquetas_llm: %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_etiquetas_llm_youtube(conn, logger: logging.Logger) -> int:
    """Carga processed.etiquetas_llm desde el CSV del etiquetado YouTube LLM."""
    if not CSV_LLM_YOUTUBE.exists():
        logger.warning("No existe %s — saltando etiquetas YouTube LLM", CSV_LLM_YOUTUBE)
        return 0

    import csv as _csv
    _VALID_CLASIF = {"ODIO", "NO_ODIO", "DUDOSO"}
    with open(CSV_LLM_YOUTUBE, encoding="utf-8") as _f:
        reader = _csv.DictReader(_f)
        rows_raw = [
            r for r in reader
            if r.get("clasificacion_principal", "") in _VALID_CLASIF
        ]
    df = pd.DataFrame(rows_raw).astype(str) if rows_raw else pd.DataFrame()
    logger.info("processed.etiquetas_llm (YouTube): %d filas válidas de %s", len(df), CSV_LLM_YOUTUBE.name)

    if df.empty:
        return 0

    columns = [
        "message_uuid", "clasificacion_principal", "categoria_odio_pred",
        "intensidad_pred", "resumen_motivo", "llm_version",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid = safe_val(r.get("message_uuid"))
        if uuid is None:
            continue
        rows.append((
            uuid,
            safe_val(r.get("clasificacion_principal")),
            safe_val(r.get("categoria_odio_pred")),
            safe_val(r.get("intensidad_pred")),
            safe_val(r.get("resumen_motivo")),
            "v1",
        ))

    n = upsert_rows(
        conn, "processed.etiquetas_llm", columns, rows,
        conflict_columns=["message_uuid", "llm_version"],
        update_columns=["clasificacion_principal", "categoria_odio_pred",
                        "intensidad_pred", "resumen_motivo"],
    )
    logger.info("processed.etiquetas_llm (YouTube): %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_raw_youtube(conn, logger: logging.Logger) -> int:
    """Carga raw.mensajes desde el CSV de extracción de YouTube."""
    if not CSV_YT_RAW.exists():
        logger.warning("No existe %s — saltando raw YouTube", CSV_YT_RAW)
        return 0

    df = pd.read_csv(CSV_YT_RAW, dtype=str)
    logger.info("raw.mensajes (YouTube): %d filas leídas de CSV", len(df))

    columns = [
        "message_uuid", "platform", "tweet_id", "created_at", "content_original",
        "author_username", "author_id", "source_media", "batch_id", "scrape_date",
        "language", "url", "retweet_count", "reply_count", "like_count", "quote_count",
    ]

    rows = []
    for _, r in df.iterrows():
        comment_id = safe_val(r.get("comment_id"))
        comment_text = safe_val(r.get("comment_text"))
        if not comment_id or not comment_text:
            continue
        rows.append((
            yt_to_uuid(comment_id),          # message_uuid (UUID v5)
            "youtube",                         # platform
            safe_val(r.get("video_id")),       # tweet_id → almacena video_id
            safe_val(r.get("comment_published_at")),  # created_at
            comment_text,                      # content_original
            safe_val(r.get("author_display_name")),   # author_username
            safe_val(r.get("author_channel_id")),     # author_id
            safe_val(r.get("medio")),          # source_media
            None,                              # batch_id
            safe_val(r.get("video_published_at")),    # scrape_date → fecha del video
            None,                              # language
            safe_val(r.get("channel_url")),    # url
            None,                              # retweet_count
            None,                              # reply_count
            safe_val(r.get("like_count"), "int"),  # like_count
            None,                              # quote_count
        ))

    n = upsert_rows(
        conn, "raw.mensajes", columns, rows,
        conflict_columns=["message_uuid"],
        update_columns=[c for c in columns if c != "message_uuid"],
    )
    logger.info("raw.mensajes (YouTube): %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_processed_youtube(conn, logger: logging.Logger) -> int:
    """Carga processed.mensajes desde youtube_full_anon.csv del orquestador.

    El orquestador ya produce el CSV con schema canónico + columnas de detección
    y con autores anonimizados (author_id_anon, author_username_anon). Se mapean
    los nombres canónicos a los nombres legacy de la BD:
      published_at   → created_at
      hate_candidate → has_hate_terms_match
      hate_types     → candidate_reason
      (sin equiv.)   → strong_phrase = False
    """
    if not CSV_YT_TAGGED.exists():
        logger.warning("No existe %s — saltando processed YouTube", CSV_YT_TAGGED)
        return 0

    df = pd.read_csv(CSV_YT_TAGGED, dtype=str)
    logger.info("processed.mensajes (YouTube): %d filas leídas de CSV", len(df))

    # Mapa message_uuid → ingested_at desde raw.mensajes (cargado antes en el
    # pipeline). Garantiza que el INSERT use la fecha real de ingreso al
    # sistema, no NOW(), para que el KPI "nuevos hoy" del viewer sea correcto.
    _cur = conn.cursor()
    _cur.execute(
        "SELECT message_uuid::text, ingested_at FROM raw.mensajes WHERE platform = 'youtube'"
    )
    ingested_map = dict(_cur.fetchall())
    _cur.close()

    columns = [
        "message_uuid", "platform", "content_original", "source_media",
        "created_at", "language", "url",
        "author_id_anon", "author_username_anon",
        "matched_terms", "has_hate_terms_match", "match_count",
        "strong_phrase", "is_candidate", "candidate_reason",
        "processed_at", "ingested_at",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid_str = safe_val(r.get("message_uuid"))
        content = safe_val(r.get("content_original"))
        if not uuid_str or not content:
            continue

        rows.append((
            uuid_str,
            "youtube",
            content,
            resolve_source_media(r),
            # orquestador: published_at → BD: created_at
            safe_val(r.get("published_at") or r.get("created_at")),
            safe_val(r.get("language")),
            safe_val(r.get("url")),
            safe_val(r.get("author_id_anon")),
            safe_val(r.get("author_username_anon")),
            safe_val(r.get("matched_terms")),
            # orquestador: hate_candidate (0/1) → BD: has_hate_terms_match (bool)
            safe_val(r.get("hate_candidate") or r.get("has_hate_terms_match"), "bool"),
            safe_val(r.get("match_count"), "int"),
            # sin equivalente en orquestador; mantener False para compatibilidad BD
            False,
            safe_val(r.get("is_candidate"), "bool"),
            # orquestador: hate_types → BD: candidate_reason
            safe_val(r.get("hate_types") or r.get("candidate_reason")),
            safe_val(r.get("processed_at")),
            ingested_map.get(uuid_str),
        ))

    rows_before = len(rows)
    rows = [r for r in rows if r[0] is not None and r[2] is not None]
    skipped = rows_before - len(rows)
    if skipped:
        logger.warning("processed.mensajes (YouTube): %d filas descartadas", skipped)

    # processed_at e ingested_at son metadatos de ingesta: solo se escriben en
    # INSERT, nunca en UPDATE (evita pico falso de "nuevos hoy" tras reproceso).
    _no_update_dates = {"message_uuid", "processed_at", "ingested_at"}
    n = upsert_rows(
        conn, "processed.mensajes", columns, rows,
        conflict_columns=["message_uuid"],
        update_columns=[c for c in columns if c not in _no_update_dates],
    )
    logger.info("processed.mensajes (YouTube): %d filas procesadas (upsert)", len(rows))
    return len(rows)


def load_resumen_diario(conn, logger: logging.Logger) -> int:
    """Calcula y escribe processed.resumen_diario para hoy."""
    cur = conn.cursor()

    today = datetime.now().date()

    # Métricas por plataforma desde las tablas ya cargadas
    cur.execute("""
        SELECT
            rm.platform,
            COUNT(DISTINCT rm.message_uuid)                          AS total_raw,
            COUNT(DISTINCT CASE WHEN pm.is_candidate THEN pm.message_uuid END) AS total_candidatos,
            COUNT(DISTINCT CASE WHEN s.pred_odio = 1 THEN s.message_uuid END)  AS total_odio_baseline,
            COUNT(DISTINCT CASE WHEN e.clasificacion_principal = 'ODIO'
                                THEN e.message_uuid END)             AS total_odio_llm,
            AVG(s.proba_odio)                                        AS score_promedio
        FROM raw.mensajes rm
        LEFT JOIN processed.mensajes pm USING (message_uuid)
        LEFT JOIN processed.scores s    USING (message_uuid)
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        GROUP BY rm.platform
    """)

    rows_to_upsert = []
    for platform, total_raw, total_cand, odio_bl, odio_llm, score_avg in cur.fetchall():
        rows_to_upsert.append((
            today, platform, total_raw, total_cand, odio_bl, odio_llm, score_avg,
        ))

    columns = [
        "fecha", "platform", "total_mensajes_raw", "total_candidatos",
        "total_odio_baseline", "total_odio_llm", "score_promedio",
    ]

    if rows_to_upsert:
        n = upsert_rows(
            conn, "processed.resumen_diario", columns, rows_to_upsert,
            conflict_columns=["fecha", "platform"],
            update_columns=[c for c in columns if c not in ("fecha", "platform")],
        )

    cur.close()
    logger.info("processed.resumen_diario: %d plataformas actualizadas para %s", len(rows_to_upsert), today)
    return len(rows_to_upsert)


def load_evaluacion_art510(conn, logger: logging.Logger) -> int:
    """Carga processed.evaluacion_art510 desde el CSV generado por evaluar_art510.py."""
    if not CSV_ART510.exists():
        # evaluar_art510.py aún no implementado — silenciar hasta que exista el script
        logger.debug("Art510: CSV no encontrado (%s) — etapa pendiente de implementación", CSV_ART510)
        return 0

    # Crear tabla si no existe
    ddl_path = SCRIPT_DIR / "create_tables_art510.sql"
    if ddl_path.exists():
        cur = conn.cursor()
        cur.execute(ddl_path.read_text(encoding="utf-8"))
        cur.close()
        conn.commit()

    df = pd.read_csv(CSV_ART510, dtype=str)
    logger.info("processed.evaluacion_art510: %d filas leídas de %s", len(df), CSV_ART510.name)

    columns = [
        "message_uuid", "label_source", "es_potencial_delito", "apartado_510",
        "grupo_protegido", "conducta_detectada", "justificacion", "confianza",
        "llm_version",
    ]

    rows = []
    for _, r in df.iterrows():
        uuid = safe_val(r.get("message_uuid"))
        if uuid is None:
            continue
        es_delito_raw = safe_val(r.get("es_potencial_delito"))
        es_delito = str(es_delito_raw).strip().lower() in ("true", "1", "si", "sí", "yes")
        rows.append((
            uuid,
            safe_val(r.get("label_source")) or "llm",
            es_delito,
            safe_val(r.get("apartado_510")),
            safe_val(r.get("grupo_protegido")),
            safe_val(r.get("conducta_detectada")),
            safe_val(r.get("justificacion")),
            safe_val(r.get("confianza")),
            "v1",
        ))

    n = upsert_rows(
        conn, "processed.evaluacion_art510", columns, rows,
        conflict_columns=["message_uuid", "label_source"],
        update_columns=[c for c in columns if c not in ("message_uuid", "label_source")],
    )
    logger.info("processed.evaluacion_art510: %d filas procesadas (upsert)", len(rows))
    return len(rows)


# ============================================================
# MAIN
# ============================================================

def _run_loader_with_retry(
    name: str,
    loader_fn,
    logger: logging.Logger,
    max_retries: int = LOAD_DB_MAX_RETRIES,
    backoff_seconds: float = LOAD_DB_RETRY_BACKOFF_SEC,
) -> bool:
    """Ejecuta un loader con una conexión propia; reintenta con una conexión
    NUEVA si el fallo es transitorio (p. ej. "SSL connection has been closed
    unexpectedly" a mitad de un upsert grande — visto en producción con
    raw.mensajes(X), ~143k filas). Un upsert es idempotente (ON CONFLICT), así
    que reintentar el loader completo desde cero es seguro.

    Errores de datos/esquema (FK, NOT NULL, tipo…) NO se reintentan: son
    deterministas y volverían a fallar igual, así que se registran y se
    devuelve False de inmediato.
    """
    last_exc: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            # Conexión nueva por loader (y por intento): si esta etapa se
            # cae, no arrastra a las siguientes ni reintenta con una
            # conexión que ya sabemos rota.
            with get_conn() as conn:
                loader_fn(conn, logger)
            return True
        except _TRANSIENT_DB_ERRORS as e:
            last_exc = e
            if attempt < max_retries:
                wait = backoff_seconds * attempt
                logger.warning(
                    "Error transitorio de conexión cargando %s (intento %d/%d): %s. "
                    "Reintentando en %.0fs con una conexión nueva...",
                    name, attempt, max_retries, e, wait,
                )
                time.sleep(wait)
                continue
            logger.error(
                "Error cargando %s tras %d intentos (fallo transitorio persistente): %s",
                name, max_retries, e, exc_info=True,
            )
            return False
        except Exception as e:
            # Error no transitorio (datos/esquema): no tiene sentido
            # reintentar. get_conn() ya hace su propio rollback/close
            # internamente (db_utils.py) — no hace falta repetirlo acá,
            # para no arriesgarnos a tapar este mensaje con un segundo
            # error de limpieza.
            logger.error("Error cargando %s: %s", name, e, exc_info=True)
            return False
    logger.error("Error cargando %s: %s", name, last_exc, exc_info=True)
    return False


def main() -> int:
    logger = setup_logging()
    logger.info("=== Inicio carga a PostgreSQL ===")

    ok = 0
    fail = 0
    skipped = 0
    # Éxito/fracaso de las etapas raw por plataforma, para poder saltar con
    # criterio las etapas processed/scores que dependen de ellas (mismo lote
    # de message_uuid — si raw no se comprometió, processed/scores fallarían
    # igual por FK, solo que 20-50s más tarde y con un traceback confuso).
    raw_ok: Dict[str, bool] = {}

    # Orden: primero raw, luego processed (por las FK). depends_on indica de
    # qué etapa raw depende cada loader (None = no depende de ninguna raw de
    # esta corrida, p. ej. porque opera sobre backlog histórico ya cargado).
    loaders = [
        ("raw.mensajes (X)", load_raw_mensajes, None, "x"),
        ("processed.mensajes (X)", load_processed_mensajes, "x", None),
        ("raw.mensajes (YouTube)", load_raw_youtube, None, "youtube"),
        ("processed.mensajes (YouTube)", load_processed_youtube, "youtube", None),
        ("processed.scores", load_scores, "x", None),
        ("processed.etiquetas_llm", load_etiquetas_llm, None, None),
        ("processed.etiquetas_llm (YouTube)", load_etiquetas_llm_youtube, None, None),
        ("processed.evaluacion_art510", load_evaluacion_art510, None, None),
        ("processed.resumen_diario", load_resumen_diario, None, None),
    ]

    for name, loader_fn, depends_on, provides in loaders:
        if depends_on is not None and raw_ok.get(depends_on) is False:
            logger.warning(
                "Saltando %s — la etapa raw de la que depende (%s) falló en "
                "esta corrida; reintentarla habría fallado igual por "
                "integridad referencial (los message_uuid de hoy no llegaron "
                "a comprometerse).",
                name, depends_on,
            )
            skipped += 1
            continue

        success = _run_loader_with_retry(name, loader_fn, logger)
        if provides is not None:
            raw_ok[provides] = success
        if success:
            ok += 1
        else:
            fail += 1

    logger.info("=== Fin carga === OK: %d, Fallos: %d, Saltados: %d", ok, fail, skipped)
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
