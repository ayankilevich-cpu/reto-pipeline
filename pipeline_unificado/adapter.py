# pipeline_unificado/adapter.py
"""Normaliza salidas de ingesta (X, YouTube) a un schema canónico común."""

from __future__ import annotations

import uuid as uuidlib
from datetime import datetime, timezone
from typing import Any, Iterable, List

import numpy as np
import pandas as pd

RETO_YT_NS = uuidlib.UUID("a1b2c3d4-e5f6-7890-abcd-ef1234567890")

CANONICAL_SCHEMA: List[str] = [
    "message_uuid",
    "platform",
    "content_original",
    "published_at",
    "source_media",
    "author_username",
    "author_id",
    "url",
    "language",
    "parent_id",
    "likes",
    "retweet_count",
    "reply_count",
    "quote_count",
    "ingestion_batch_id",
    "ingestion_date",
    "platform_native_id",
]

CANONICAL_DTYPES = {
    "likes": int,
    "retweet_count": int,
    "reply_count": int,
    "quote_count": int,
}

_INT_COLS = list(CANONICAL_DTYPES.keys())

# Columnas mínimas del output de consolidar_csv.py (X)
_X_REQUIRED = ("message_uuid", "content_original")

# Columnas mínimas del output de youtube_extract_hate.py (YouTube)
_YT_REQUIRED = ("comment_id", "comment_text")


def yt_comment_to_uuid(comment_id: str) -> str:
    """Genera UUID v5 determinístico desde comment_id de YouTube."""
    return str(uuidlib.uuid5(RETO_YT_NS, str(comment_id)))


def _require_columns(df: pd.DataFrame, names: Iterable[str], plataforma: str) -> None:
    missing = [c for c in names if c not in df.columns]
    if missing:
        raise ValueError(
            f"Faltan columnas requeridas en el DataFrame de {plataforma}: {missing}. "
            f"Columnas presentes: {list(df.columns)}"
        )


def _empty_str(val: Any) -> str:
    if val is None:
        return ""
    if isinstance(val, float) and np.isnan(val):
        return ""
    if isinstance(val, str) and val.lower() == "nan":
        return ""
    return str(val)


def _series_str(s: pd.Series) -> pd.Series:
    out = s.copy()
    out = out.where(~out.isna(), "")
    return out.map(_empty_str)


def _series_int(s: pd.Series) -> pd.Series:
    num = pd.to_numeric(s, errors="coerce")
    return num.fillna(0).astype(np.int64)


def _to_iso_utc(val: Any) -> str:
    """Convierte fechas a ISO 8601 en UTC; cadenas no parseables se devuelven tal cual (sin NaN)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    if isinstance(val, (float, np.floating)) and np.isnan(val):
        return ""
    if isinstance(val, pd.Timestamp):
        ts = val
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        else:
            ts = ts.tz_convert("UTC")
        return ts.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    if isinstance(val, datetime):
        if pd.isna(val):
            return ""
        ts = val
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return ""
    try:
        ts = pd.to_datetime(s, utc=True, errors="coerce")
        if pd.isna(ts):
            return ""
        return ts.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    except Exception:
        return s


def _col(df: pd.DataFrame, name: str, default: Any = "") -> pd.Series:
    if name not in df.columns:
        return pd.Series([default] * len(df), index=df.index)
    return df[name]


def adapt_x(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte output de consolidar_csv.py (X) al schema canónico."""
    _require_columns(df, _X_REQUIRED, "X (consolidar_csv)")

    n = len(df)
    tweet_id = _col(df, "tweet_id", "")

    out = pd.DataFrame(
        {
            "message_uuid": _series_str(_col(df, "message_uuid")),
            # Salida X: siempre "x" (minúsculas), independientemente de variantes en el CSV.
            "platform": pd.Series(["x"] * n, index=df.index, dtype=object),
            "content_original": _series_str(_col(df, "content_original")),
            "published_at": _col(df, "created_at").map(_to_iso_utc),
            "source_media": _series_str(_col(df, "source_media")),
            "author_username": _series_str(_col(df, "author_username")),
            "author_id": _series_str(_col(df, "author_id")),
            "url": _series_str(_col(df, "url")),
            "language": _series_str(_col(df, "language", "es")),
            "parent_id": pd.Series([""] * n, index=df.index, dtype=object),
            "likes": _series_int(_col(df, "like_count", 0)),
            "retweet_count": _series_int(_col(df, "retweet_count", 0)),
            "reply_count": _series_int(_col(df, "reply_count", 0)),
            "quote_count": _series_int(_col(df, "quote_count", 0)),
            "ingestion_batch_id": _series_str(_col(df, "batch_id")),
            "ingestion_date": _col(df, "scrape_date").map(_to_iso_utc),
            "platform_native_id": _series_str(tweet_id),
        },
        index=df.index,
    )
    lang = out["language"].astype(str)
    out["language"] = np.where(lang.str.strip().eq("") | lang.str.lower().eq("nan"), "es", lang)

    out = out[CANONICAL_SCHEMA]
    validate_canonical(out)
    return out


def adapt_youtube(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte output de youtube_extract_hate.py al schema canónico."""
    _require_columns(df, _YT_REQUIRED, "YouTube (youtube_extract_hate)")

    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S") + "Z"

    vid = _series_str(_col(df, "video_id"))
    cid = _series_str(_col(df, "comment_id"))
    url_parts = []
    for v, c in zip(vid, cid):
        if v and c:
            url_parts.append(f"https://www.youtube.com/watch?v={v}&lc={c}")
        else:
            url_parts.append("")
    url_s = pd.Series(url_parts, index=df.index, dtype=object)

    msg_uuid = cid.map(lambda x: yt_comment_to_uuid(x) if str(x).strip() else "")

    out = pd.DataFrame(
        {
            "message_uuid": msg_uuid,
            "platform": pd.Series(["youtube"] * len(df), index=df.index, dtype=object),
            "content_original": _series_str(_col(df, "comment_text")),
            "published_at": _col(df, "comment_published_at").map(_to_iso_utc),
            "source_media": _series_str(_col(df, "medio")),
            "author_username": _series_str(_col(df, "author_display_name")),
            "author_id": _series_str(_col(df, "author_channel_id")),
            "url": _series_str(url_s),
            "language": pd.Series(["es"] * len(df), index=df.index, dtype=object),
            "parent_id": vid,
            "likes": _series_int(_col(df, "like_count", 0)),
            "retweet_count": pd.Series([0] * len(df), index=df.index, dtype=np.int64),
            "reply_count": pd.Series([0] * len(df), index=df.index, dtype=np.int64),
            "quote_count": pd.Series([0] * len(df), index=df.index, dtype=np.int64),
            "ingestion_batch_id": pd.Series([""] * len(df), index=df.index, dtype=object),
            "ingestion_date": pd.Series([now_iso] * len(df), index=df.index, dtype=object),
            "platform_native_id": cid,
        },
        index=df.index,
    )

    out = out[CANONICAL_SCHEMA]
    validate_canonical(out)
    return out


def validate_canonical(df: pd.DataFrame) -> None:
    """Verifica que df cumple el schema canónico. Raise ValueError si no."""
    if list(df.columns) != CANONICAL_SCHEMA:
        raise ValueError(
            "Columnas u orden incorrectos en DataFrame canónico. "
            f"Esperado: {CANONICAL_SCHEMA}, recibido: {list(df.columns)}"
        )
    if df.isna().any().any():
        bad = df.columns[df.isna().any()].tolist()
        raise ValueError(f"El DataFrame canónico contiene NaN en columnas: {bad}")

    for c in _INT_COLS:
        if not np.issubdtype(df[c].dtype, np.integer):
            raise ValueError(
                f"Columna entera '{c}' tiene dtype {df[c].dtype}; se esperaba un entero."
            )
