#!/usr/bin/env python3
"""
audit_hate_terms.py — Auditoría estadística del diccionario hate_terms_clean.csv.

Cruza cada término detectado (matched_terms) contra la clasificación del LLM
(ODIO / NO_ODIO) en processed.mensajes ⋈ processed.etiquetas_llm y calcula un
odds-ratio con corrección de Haldane-Anscombe (+0.5) y su z-score, para decidir
qué términos mantener, descartar o revisar manualmente.

Todo el output es NUEVO, en outputs/pipeline_unificado/audit_terminos/.
NO sobrescribe ningún archivo de producción. El swap a producción es manual.

Uso:
  python3 pipeline_unificado/audit_hate_terms.py
"""
from __future__ import annotations

import csv
import json
import math
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv

_REPO_ROOT = Path(__file__).resolve().parent.parent  # Clases/RETO/

HATE_TERMS_CLEAN = _REPO_ROOT / "Medios" / "hate_terms_clean.csv"
HATE_GENERAL_TERMS = _REPO_ROOT / "Medios" / "hate_general_terms.csv"
OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"

# Umbrales de decisión
MIN_SUPPORT = 5          # soporte mínimo (a + c) para evaluar automáticamente
Z_THRESHOLD = 1.96       # z-score para significancia (~95% dos colas)


# ---------------------------------------------------------------------------
# Conexión a BD (mismo patrón que Reto - CIEDES/filtrar_relevancia_llm.py)
# ---------------------------------------------------------------------------

def _get_db_conn():
    """
    Conexión a PostgreSQL. Prioriza DATABASE_URL (patrón de
    Reto - CIEDES/filtrar_relevancia_llm.py); si no existe, arma la conexión
    desde los campos sueltos DB_* que usa automatizacion_diaria/.env
    (mismo criterio que automatizacion_diaria/db_utils.py).
    """
    try:
        import psycopg2
        url = os.getenv("DATABASE_URL")
        if url:
            return psycopg2.connect(url)

        host = os.getenv("DB_HOST", "").strip()
        if not host:
            return None
        params = {
            "host": host,
            "port": int(os.getenv("DB_PORT", "5432") or 5432),
            "dbname": os.getenv("DB_NAME", "reto_db"),
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", ""),
            "connect_timeout": 10,
        }
        sslmode = os.getenv("DB_SSLMODE", "").strip()
        if sslmode:
            params["sslmode"] = sslmode
        elif "neon" in host:
            params["sslmode"] = "require"
        return psycopg2.connect(**params)
    except Exception as e:
        print(f"  ⚠ No se pudo conectar a BD: {e}")
        return None


# ---------------------------------------------------------------------------
# Parseo/normalización de términos
# --- mirror intencional de secciones/terminos_frecuentes.py ---
# --- mantener sincronizado si cambia allá ---
# ---------------------------------------------------------------------------

def _normalize_term_for_filter(token: str) -> str:
    """
    Normaliza términos para conteo/exclusión:
    minúsculas, sin tildes, sin artefactos de formato y espacios colapsados.

    mirror intencional de secciones/terminos_frecuentes.py — mantener
    sincronizado si cambia allá.
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

    mirror intencional de secciones/terminos_frecuentes.py — mantener
    sincronizado si cambia allá.
    """
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
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


# ---------------------------------------------------------------------------
# Carga de diccionarios
# ---------------------------------------------------------------------------

def _load_csv_column(path: Path, column: str) -> List[str]:
    """Lee una columna de un CSV; devuelve lista de valores no vacíos (crudos)."""
    if not path.exists():
        raise FileNotFoundError(f"No existe el archivo: {path}")
    values: List[str] = []
    with open(path, encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if column not in (reader.fieldnames or []):
            raise ValueError(
                f"Columna '{column}' no encontrada en {path.name}. "
                f"Columnas: {reader.fieldnames}"
            )
        for row in reader:
            raw = (row.get(column) or "").strip()
            if raw:
                values.append(raw)
    return values


# ---------------------------------------------------------------------------
# Fetch + conteo
# ---------------------------------------------------------------------------

def fetch_rows(conn) -> List[Tuple[str, str, str]]:
    """Devuelve filas (platform, matched_terms, clasificacion_principal)."""
    query = """
        SELECT m.platform, m.matched_terms, e.clasificacion_principal
        FROM processed.mensajes m
        JOIN processed.etiquetas_llm e USING (message_uuid)
        WHERE e.clasificacion_principal IN ('ODIO', 'NO_ODIO')
          AND m.matched_terms IS NOT NULL AND m.matched_terms != ''
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


def count_terms(
    rows: List[Tuple[str, str, str]]
) -> Tuple[Dict[str, int], Dict[str, int], int, int]:
    """
    Cuenta por término normalizado en cuántos mensajes ODIO (a) y NO_ODIO (c)
    aparece. Un término se cuenta una sola vez por mensaje (set por fila).

    Returns: (odio_counts, no_odio_counts, total_odio, total_no_odio)
    """
    odio_counts: Dict[str, int] = defaultdict(int)
    no_odio_counts: Dict[str, int] = defaultdict(int)
    total_odio = 0
    total_no_odio = 0

    for _platform, matched_terms, clasif in rows:
        terms = set(_parse_and_normalize_matched_terms(matched_terms))
        if clasif == "ODIO":
            total_odio += 1
            for t in terms:
                odio_counts[t] += 1
        elif clasif == "NO_ODIO":
            total_no_odio += 1
            for t in terms:
                no_odio_counts[t] += 1

    return odio_counts, no_odio_counts, total_odio, total_no_odio


# ---------------------------------------------------------------------------
# Score (odds-ratio con corrección Haldane-Anscombe + z-score)
# ---------------------------------------------------------------------------

def term_stats(a: int, c: int, total_odio: int, total_no_odio: int) -> Dict[str, float]:
    """
    a = mensajes ODIO con el término
    c = mensajes NO_ODIO con el término
    b = total_odio - a
    d = total_no_odio - c
    """
    b = total_odio - a
    d = total_no_odio - c

    or_ratio = ((a + 0.5) * (d + 0.5)) / ((b + 0.5) * (c + 0.5))
    log_or = math.log(or_ratio)
    se = math.sqrt(1 / (a + 0.5) + 1 / (b + 0.5) + 1 / (c + 0.5) + 1 / (d + 0.5))
    z = log_or / se

    return {
        "a": a, "b": b, "c": c, "d": d,
        "soporte": a + c,
        "or_ratio": or_ratio,
        "log_or": log_or,
        "z": z,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    dotenv_path = _REPO_ROOT / "automatizacion_diaria" / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"✓ .env: {dotenv_path} (existe={dotenv_path.exists()})")

    print("=" * 70)
    print("AUDITORÍA DE hate_terms_clean.csv — odds-ratio vs clasificación LLM")
    print("=" * 70)

    conn = _get_db_conn()
    if conn is None:
        print("✗ Sin conexión a BD. Verificá DATABASE_URL en .env")
        return 1

    try:
        rows = fetch_rows(conn)
    finally:
        conn.close()

    print(f"  Filas ODIO/NO_ODIO con matched_terms: {len(rows):,}")

    odio_counts, no_odio_counts, total_odio, total_no_odio = count_terms(rows)
    print(f"  total_odio    = {total_odio:,}")
    print(f"  total_no_odio = {total_no_odio:,}")
    print(f"  términos normalizados distintos observados: "
          f"{len(set(odio_counts) | set(no_odio_counts)):,}")

    # Diccionarios a evaluar
    clean_raw = _load_csv_column(HATE_TERMS_CLEAN, "Lemas")
    general_raw = _load_csv_column(HATE_GENERAL_TERMS, "term")
    general_norm: Set[str] = {
        n for n in (_normalize_term_for_filter(t) for t in general_raw) if n
    }
    print(f"  hate_terms_clean.csv: {len(clean_raw):,} términos (columna Lemas)")
    print(f"  hate_general_terms.csv: {len(general_norm):,} términos normalizados (columna term)")

    # Clasificar cada término de hate_terms_clean.csv (dedup por forma normalizada,
    # conservando la primera grafía original vista).
    seen_norm: Set[str] = set()
    buckets: Dict[str, List[Dict[str, Any]]] = {
        "mantener": [],
        "descartar": [],
        "revision_manual": [],
    }

    for raw_term in clean_raw:
        nt = _normalize_term_for_filter(raw_term)
        if not nt or nt in seen_norm:
            continue
        seen_norm.add(nt)

        a = odio_counts.get(nt, 0)
        c = no_odio_counts.get(nt, 0)
        stats = term_stats(a, c, total_odio, total_no_odio)
        soporte = stats["soporte"]
        z = stats["z"]

        record = {
            "Lemas": raw_term,
            "norm": nt,
            "a": stats["a"],
            "b": stats["b"],
            "c": stats["c"],
            "d": stats["d"],
            "z": round(z, 4),
            "soporte": soporte,
            "in_general": nt in general_norm,
        }

        if nt in general_norm:
            # Ya curado en hate_general_terms.csv: siempre mantener, pero loguear.
            print(f"  [general] {raw_term!r}: z={z:.2f}, soporte={soporte} "
                  f"(mantener por estar en hate_general_terms.csv)")
            buckets["mantener"].append(record)
        elif soporte < MIN_SUPPORT:
            buckets["revision_manual"].append(record)
        elif z >= Z_THRESHOLD:
            buckets["mantener"].append(record)
        else:
            buckets["descartar"].append(record)

    _write_outputs(buckets, general_norm)
    _print_summary(buckets)
    return 0


def _write_outputs(
    buckets: Dict[str, List[Dict[str, Any]]],
    general_norm: Set[str],
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # 1) hate_terms_clean_v2.csv — mantener y NO ya presentes en hate_general_terms.csv
    v2_path = OUT_DIR / "hate_terms_clean_v2.csv"
    with open(v2_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Lemas"])
        for r in buckets["mantener"]:
            if r["norm"] not in general_norm:
                w.writerow([r["Lemas"]])

    # 2) descartados
    desc_path = OUT_DIR / "hate_terms_clean_descartados.csv"
    _write_detail_csv(desc_path, buckets["descartar"])

    # 3) revisión manual
    rev_path = OUT_DIR / "hate_terms_clean_revision_manual.csv"
    _write_detail_csv(rev_path, buckets["revision_manual"])

    print()
    print(f"  Escrito: {v2_path}")
    print(f"  Escrito: {desc_path}")
    print(f"  Escrito: {rev_path}")


def _write_detail_csv(path: Path, records: List[Dict[str, Any]]) -> None:
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Lemas", "a", "b", "c", "d", "z", "soporte"])
        for r in sorted(records, key=lambda x: x["z"]):
            w.writerow([r["Lemas"], r["a"], r["b"], r["c"], r["d"], r["z"], r["soporte"]])


def _print_summary(buckets: Dict[str, List[Dict[str, Any]]]) -> None:
    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  mantener:        {len(buckets['mantener']):,}")
    print(f"  descartar:       {len(buckets['descartar']):,}")
    print(f"  revision_manual: {len(buckets['revision_manual']):,}")

    # 20 términos con z más bajo (más ruido) entre los descartados
    descartar_sorted = sorted(buckets["descartar"], key=lambda x: x["z"])
    print()
    print("  ── 20 términos con z más bajo (más ruido, descartados) ──")
    print(f"  {'término':<28}{'z':>8}{'soporte':>9}{'a':>7}{'c':>7}")
    for r in descartar_sorted[:20]:
        print(f"  {r['Lemas'][:27]:<28}{r['z']:>8.2f}{r['soporte']:>9}{r['a']:>7}{r['c']:>7}")

    # 20 términos con z más alto entre los nuevos confirmados (mantener, no en general)
    nuevos_confirmados = [r for r in buckets["mantener"] if not r["in_general"]]
    nuevos_sorted = sorted(nuevos_confirmados, key=lambda x: x["z"], reverse=True)
    print()
    print("  ── 20 términos con z más alto (nuevos confirmados, no en general) ──")
    print(f"  {'término':<28}{'z':>8}{'soporte':>9}{'a':>7}{'c':>7}")
    for r in nuevos_sorted[:20]:
        print(f"  {r['Lemas'][:27]:<28}{r['z']:>8.2f}{r['soporte']:>9}{r['a']:>7}{r['c']:>7}")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
