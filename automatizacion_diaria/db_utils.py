"""
db_utils.py — Utilidades de conexión y escritura a PostgreSQL para el pipeline RETO.

Carga credenciales desde .env y ofrece funciones de upsert genéricas.
"""

from __future__ import annotations

import json
import os
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qs, unquote, urlparse

import psycopg2
import psycopg2.extras

# Cargar .env del mismo directorio
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent / ".env")
except ImportError:
    pass  # sin python-dotenv, usar variables de entorno directas

_CONNECT_TIMEOUT_SEC = int(os.environ.get("DB_CONNECT_TIMEOUT", "10"))


def _is_postgres_url(value: str) -> bool:
    return bool(re.match(r"^postgres(ql)?://", value.strip(), re.I))


def _params_from_database_url(url: str) -> Dict[str, Any]:
    """Parsea postgresql://user:pass@host:port/db?sslmode=require (solo metadatos)."""
    parsed = urlparse(url.strip())
    if parsed.scheme not in ("postgresql", "postgres"):
        raise ValueError(f"Esquema de URL no soportado: {parsed.scheme}")

    dbname = (parsed.path or "/").lstrip("/") or "reto_db"
    query = parse_qs(parsed.query)
    params: Dict[str, Any] = {
        "host": parsed.hostname or "localhost",
        "port": int(parsed.port or 5432),
        "dbname": dbname,
        "user": unquote(parsed.username or "postgres"),
        "password": unquote(parsed.password or ""),
        "connect_timeout": _CONNECT_TIMEOUT_SEC,
    }
    sslmode = (query.get("sslmode") or [""])[0]
    if sslmode:
        params["sslmode"] = sslmode
    elif parsed.hostname and "neon" in parsed.hostname:
        params["sslmode"] = "require"
    return params


def _params_from_fields(
    host: str,
    port: Any = 5432,
    dbname: str = "reto_db",
    user: str = "postgres",
    password: str = "",
    sslmode: str = "",
) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "host": host.strip(),
        "port": int(port or 5432),
        "dbname": (dbname or "reto_db").strip(),
        "user": (user or "postgres").strip(),
        "password": password or "",
        "connect_timeout": _CONNECT_TIMEOUT_SEC,
    }
    if sslmode:
        params["sslmode"] = sslmode.strip()
    elif "neon" in params["host"]:
        params["sslmode"] = "require"
    return params


def _connection_dsn() -> Optional[str]:
    """
    URL completa de conexión si existe.

    Usar tal cual en psycopg2.connect(dsn) para no perder parámetros Neon
    (p. ej. options=endpoint%3D... en branches).
    """
    for env_key in ("DATABASE_URL", "POSTGRES_URL", "NEON_DATABASE_URL"):
        url = os.environ.get(env_key, "").strip()
        if url and _is_postgres_url(url):
            return url

    for env_key in ("POSTGRES", "postgres"):
        blob = os.environ.get(env_key, "").strip()
        if blob and _is_postgres_url(blob):
            return blob

    try:
        import streamlit as st
        for key in ("DATABASE_URL", "database_url"):
            url = st.secrets.get(key)
            if url and _is_postgres_url(str(url)):
                return str(url)
        sec = st.secrets.get("postgres")
        if isinstance(sec, str) and _is_postgres_url(sec):
            return sec
        if sec:
            url = sec.get("url") or sec.get("database_url") or sec.get("DATABASE_URL")
            if url and _is_postgres_url(str(url)):
                return str(url)
    except Exception:
        pass

    return None


def _parse_postgres_blob(raw: str) -> Optional[Dict[str, Any]]:
    """Parsea URL, JSON o líneas host= del secret `postgres` en HF."""
    raw = raw.strip()
    if not raw:
        return None

    if _is_postgres_url(raw):
        return _params_from_database_url(raw)

    if raw.startswith("{") or raw.startswith("["):
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and "postgres" in data:
                data = data["postgres"]
            return _merge_postgres_mapping(data)
        except json.JSONDecodeError:
            pass

    fields: Dict[str, str] = {}
    for line in raw.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("[") and line.endswith("]"):
            continue
        if "=" not in line:
            continue
        key, val = line.split("=", 1)
        fields[key.strip().lower()] = val.strip().strip('"').strip("'")

    if fields.get("host"):
        return _params_from_fields(
            host=fields["host"],
            port=fields.get("port", 5432),
            dbname=fields.get("dbname") or fields.get("database"),
            user=fields.get("user") or fields.get("username"),
            password=fields.get("password", ""),
            sslmode=fields.get("sslmode", ""),
        )

    return None


def _merge_postgres_mapping(sec: Any) -> Optional[Dict[str, Any]]:
    if not sec:
        return None
    if isinstance(sec, str):
        return _parse_postgres_blob(sec)
    if not isinstance(sec, dict):
        try:
            sec = dict(sec)
        except Exception:
            return None

    url = sec.get("url") or sec.get("database_url") or sec.get("DATABASE_URL")
    if url:
        return _params_from_database_url(str(url))

    host = sec.get("host") or sec.get("hostname") or sec.get("POSTGRES_HOST")
    if not host:
        return None
    return _params_from_fields(
        host=str(host),
        port=sec.get("port", 5432),
        dbname=sec.get("dbname") or sec.get("database") or sec.get("name"),
        user=sec.get("user") or sec.get("username"),
        password=sec.get("password", ""),
        sslmode=sec.get("sslmode", ""),
    )


def _params_from_hf_env() -> Optional[Dict[str, Any]]:
    """Variables sueltas en HF (sin URL única)."""
    for env_key in ("POSTGRES", "postgres"):
        blob = os.environ.get(env_key, "").strip()
        if blob and not _is_postgres_url(blob):
            parsed = _parse_postgres_blob(blob)
            if parsed:
                return parsed

    host = (
        os.environ.get("POSTGRES_HOST")
        or os.environ.get("POSTGRES_HOSTNAME")
        or os.environ.get("DB_HOST", "")
    ).strip()
    if host:
        return _params_from_fields(
            host=host,
            port=os.environ.get("POSTGRES_PORT") or os.environ.get("DB_PORT") or 5432,
            dbname=os.environ.get("POSTGRES_DBNAME")
            or os.environ.get("POSTGRES_DB")
            or os.environ.get("POSTGRES_DATABASE")
            or os.environ.get("DB_NAME")
            or "reto_db",
            user=os.environ.get("POSTGRES_USER") or os.environ.get("DB_USER") or "postgres",
            password=os.environ.get("POSTGRES_PASSWORD") or os.environ.get("DB_PASSWORD") or "",
            sslmode=os.environ.get("POSTGRES_SSLMODE") or os.environ.get("DB_SSLMODE") or "",
        )

    return None


def get_connection_params() -> Dict[str, Any]:
    """Metadatos de conexión (host, dbname, user) para mensajes de error."""
    dsn = _connection_dsn()
    if dsn:
        return _params_from_database_url(dsn)

    hf_params = _params_from_hf_env()
    if hf_params:
        return hf_params

    try:
        import streamlit as st
        params = _merge_postgres_mapping(st.secrets.get("postgres"))
        if params:
            return params
    except Exception:
        pass

    params = {
        "host": os.environ.get("DB_HOST", "localhost"),
        "port": int(os.environ.get("DB_PORT", 5432)),
        "dbname": os.environ.get("DB_NAME", "reto_db"),
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD", ""),
        "connect_timeout": _CONNECT_TIMEOUT_SEC,
    }
    sslmode = os.environ.get("DB_SSLMODE", "")
    if sslmode:
        params["sslmode"] = sslmode
    return params


def postgres_configured() -> bool:
    if _connection_dsn():
        return True
    try:
        if _params_from_hf_env() is not None:
            return True
    except Exception:
        pass
    try:
        import streamlit as st
        if _merge_postgres_mapping(st.secrets.get("postgres")):
            return True
        if st.secrets.get("DATABASE_URL") or st.secrets.get("database_url"):
            return True
    except Exception:
        pass
    if os.environ.get("DB_HOST", "").strip() and os.environ.get("DB_PASSWORD", "").strip():
        return True
    return False


@contextmanager
def get_conn():
    """Context manager que abre y cierra una conexión a PostgreSQL."""
    dsn = _connection_dsn()
    if dsn:
        conn = psycopg2.connect(dsn, connect_timeout=_CONNECT_TIMEOUT_SEC)
    else:
        conn = psycopg2.connect(**get_connection_params())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_rows(
    conn,
    table: str,
    columns: List[str],
    rows: List[Tuple],
    conflict_columns: List[str],
    update_columns: Optional[List[str]] = None,
) -> int:
    if not rows:
        return 0

    cols_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))

    if update_columns:
        set_clause = ", ".join(
            f"{c} = EXCLUDED.{c}" for c in update_columns
        )
        conflict_action = f"DO UPDATE SET {set_clause}"
    else:
        conflict_action = "DO NOTHING"

    conflict_str = ", ".join(conflict_columns)

    sql = f"""
        INSERT INTO {table} ({cols_str})
        VALUES ({placeholders})
        ON CONFLICT ({conflict_str}) {conflict_action}
    """

    cur = conn.cursor()
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    count = cur.rowcount
    cur.close()
    return count
