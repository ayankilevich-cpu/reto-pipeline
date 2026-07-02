#!/usr/bin/env python3
"""
filtrar_relevancia_youtube.py — Clasificación de relevancia LLM para YouTube.

Patrón idéntico a etiquetar_completo_llm.py:
  1. fetch_pending_from_db()  → processed.mensajes WHERE platform='youtube'
                                AND relevante_llm IS NULL
  2. Clasifica cada mensaje con LLM (gpt-4o-mini por defecto)
  3. upload_results_to_db()   → UPDATE processed.mensajes SET
                                relevante_llm / relevante_score / relevante_motivo
  4. Caché en disco para no reprocesar ni cobrar dos veces el mismo mensaje
  5. Fallback a CSV si no hay conexión a BD

Prerequisito (una sola vez):
  psql $DATABASE_URL -f automatizacion_diaria/migrations/20260702_relevancia_youtube.sql

Uso:
  python filtrar_relevancia_youtube.py
  python filtrar_relevancia_youtube.py --max-rows 200   # limitar para pruebas
  python filtrar_relevancia_youtube.py --dry-run        # solo mostrar pendientes
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from openai import OpenAI

# ========= CONFIG =========
SCRIPT_DIR = Path(__file__).resolve().parent
RETO_ROOT  = SCRIPT_DIR.parent.parent.parent  # Clases/RETO

MODEL               = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
RELEVANCE_THRESHOLD = 0.25          # score mínimo para marcar como SI
MAX_ROWS            = 0             # 0 = todos; override con --max-rows N
CACHE_SAVE_INTERVAL = 25

OUT_DIR     = str(SCRIPT_DIR / "outputs" / "youtube")
CACHE_FILE  = os.path.join(OUT_DIR, "relevancia_cache.json")
OUTPUT_FILE = os.path.join(OUT_DIR, "relevancia_all.csv")

# Fallback CSV si no hay BD disponible
INPUT_CSV_FALLBACK = os.getenv(
    "LLM_RELEVANCE_INPUT_CSV",
    str(RETO_ROOT / "Medios" / "youtube_manual_label_for_sheets.csv"),
)
TEXT_COL = "content_original"
ID_COL   = "message_uuid"

DB_UTILS_DIR = str(RETO_ROOT / "automatizacion_diaria")
# ==========================


# =============================================================================
# ARGPARSE
# =============================================================================

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Clasificación de relevancia YouTube con LLM")
    p.add_argument(
        "--max-rows", type=int, default=0,
        help="Limitar número de mensajes a procesar (0 = todos)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Solo mostrar pendientes sin llamar al LLM",
    )
    return p.parse_args()


# =============================================================================
# CACHÉ
# =============================================================================

def _text_hash(text: str) -> str:
    return hashlib.md5(text.encode("utf-8")).hexdigest()


def load_cache() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(CACHE_FILE):
        return {}
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            cache = json.load(f)
        print(f"  ✓ Caché cargado: {len(cache)} entradas")
        return cache
    except (json.JSONDecodeError, IOError) as e:
        print(f"  ⚠ Error cargando caché, iniciando vacío: {e}")
        return {}


def save_cache(cache: Dict[str, Dict[str, Any]]) -> None:
    os.makedirs(OUT_DIR, exist_ok=True)
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump(cache, f, ensure_ascii=False, indent=2)


def _get_cached(cache: Dict, msg_id: str, text: str) -> Optional[Dict[str, Any]]:
    """Devuelve resultado cacheado si existe y el texto no cambió."""
    if msg_id not in cache:
        return None
    cached = cache[msg_id]
    if cached.get("text_hash") != _text_hash(text):
        return None
    return {
        "relevante_llm":    cached["relevante_llm"],
        "relevante_score":  cached["relevante_score"],
        "relevante_motivo": cached["relevante_motivo"],
    }


def _set_cached(cache: Dict, msg_id: str, text: str, result: Dict[str, Any]) -> None:
    cache[msg_id] = {
        "text_hash":        _text_hash(text),
        "relevante_llm":    result["relevante_llm"],
        "relevante_score":  result["relevante_score"],
        "relevante_motivo": result["relevante_motivo"],
        "cached_at":        datetime.now().isoformat(),
    }


# =============================================================================
# BASE DE DATOS
# =============================================================================

def _get_db_module():
    """Importa db_utils dinámicamente (puede no estar disponible en todos los entornos)."""
    try:
        sys.path.insert(0, DB_UTILS_DIR)
        from db_utils import get_conn  # type: ignore[import-not-found]
        return get_conn
    except Exception:
        return None


def fetch_pending_from_db() -> Optional[List[Dict[str, Any]]]:
    """
    Trae de processed.mensajes los comentarios de YouTube sin relevante_llm.
    Retorna lista de dicts con message_uuid y content_original,
    o None si no hay BD disponible.
    """
    get_conn = _get_db_module()
    if get_conn is None:
        return None

    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT message_uuid::text, content_original
                FROM processed.mensajes
                WHERE platform = 'youtube'
                  AND relevante_llm IS NULL
                  AND content_original IS NOT NULL
                  AND content_original <> ''
                ORDER BY created_at DESC NULLS LAST
            """)
            cols = [desc[0] for desc in cur.description]
            rows = [dict(zip(cols, row)) for row in cur.fetchall()]
            cur.close()
        print(f"  ✓ BD consultada: {len(rows)} comentarios YouTube pendientes de clasificación")
        return rows
    except Exception as e:
        print(f"  ⚠ No se pudo conectar a la BD: {e}")
        return None


def upload_results_to_db(results: List[Dict[str, Any]]) -> int:
    """
    UPDATE processed.mensajes SET relevante_llm, relevante_score, relevante_motivo
    para todos los UUIDs en results.
    """
    get_conn = _get_db_module()
    if get_conn is None:
        print("  ⚠ db_utils no disponible — resultados NO subidos a BD")
        return 0

    sql = """
        UPDATE processed.mensajes
        SET relevante_llm    = %s,
            relevante_score  = %s,
            relevante_motivo = %s
        WHERE message_uuid = %s::uuid
          AND platform = 'youtube'
    """

    rows = []
    for r in results:
        uuid = str(r.get(ID_COL, "")).strip()
        if not uuid:
            continue
        try:
            score = float(r.get("relevante_score", 0.0))
        except (ValueError, TypeError):
            score = 0.0
        llm    = str(r.get("relevante_llm", "NO")).strip() or "NO"
        motivo = str(r.get("relevante_motivo", "")).strip() or None
        rows.append((llm, score, motivo, uuid))

    if not rows:
        return 0

    try:
        import psycopg2.extras
        with get_conn() as conn:
            cur = conn.cursor()
            psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
            conn.commit()
            cur.close()
        print(f"  ✓ {len(rows):,} comentarios actualizados en processed.mensajes")
        return len(rows)
    except Exception as e:
        print(f"  ⚠ Error subiendo a BD: {e}")
        return 0


def _print_diagnostics() -> None:
    """Muestra la distribución de YouTube por estado de relevancia (ayuda diagnóstico)."""
    get_conn = _get_db_module()
    if get_conn is None:
        return
    try:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT relevante_llm, COUNT(*) AS total
                FROM processed.mensajes
                WHERE platform = 'youtube'
                GROUP BY relevante_llm
                ORDER BY relevante_llm NULLS LAST
            """)
            print("\n  Distribución YouTube en BD:")
            for rl, c in cur.fetchall():
                label = rl if rl is not None else "NULL (sin clasificar)"
                print(f"    {label}: {c:,}")
            cur.close()
    except Exception:
        pass


# =============================================================================
# FALLBACK CSV
# =============================================================================

def _load_rows_from_csv() -> List[Dict[str, Any]]:
    if not os.path.exists(INPUT_CSV_FALLBACK):
        print(f"  ⚠ CSV fallback no encontrado: {INPUT_CSV_FALLBACK}")
        return []
    with open(INPUT_CSV_FALLBACK, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    print(f"  ✓ CSV cargado: {len(rows)} filas desde {INPUT_CSV_FALLBACK}")
    return rows


# =============================================================================
# LLM
# =============================================================================

_SYSTEM_PROMPT = (
    "Sos un clasificador para filtrar comentarios de YouTube en videos de medios de "
    "comunicación relevantes para un proyecto de discurso de odio. "
    "Tu tarea NO es etiquetar en detalle, solo decidir si el comentario merece "
    "revisión humana/ML para ODIO."
)

_USER_TMPL = """Decidí si el comentario es potencialmente relevante para ODIO o hostilidad hacia grupos/colectivos.
Relevante: insultos fuertes, deshumanización, incitación, amenazas, ataques a inmigrantes/etnias/religión/género/orientación.
No relevante: noticia neutra, discusión general sin hostilidad, quejas a servicios/políticos sin atacar a un grupo.
Considerá ironía, sarcasmo o respuestas en hilo.

COMENTARIO (YouTube):
{txt}
"""

_RELEVANCE_SCHEMA = {
    "type": "json_schema",
    "name": "relevance_result",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "relevante": {"type": "string", "enum": ["SI", "NO"]},
            "score":     {"type": "number"},
            "motivo":    {"type": "string"},
        },
        "required": ["relevante", "score", "motivo"],
        "additionalProperties": False,
    },
}


def _clamp01(x: Any) -> float:
    try:
        v = float(x)
    except Exception:
        return 0.0
    return max(0.0, min(1.0, v))


def _norm_si_no(x: Any) -> str:
    s = str(x).strip().upper()
    return "SI" if s in {"SI", "SÍ", "YES", "Y", "1", "TRUE"} else "NO"


def _call_llm(client: OpenAI, txt: str) -> Dict[str, Any]:
    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user",   "content": _USER_TMPL.format(txt=txt)},
        ],
        text={"format": _RELEVANCE_SCHEMA},
        max_output_tokens=200,
    )
    obj = json.loads(resp.output_text)
    return {
        "relevante_llm":    _norm_si_no(obj["relevante"]),
        "relevante_score":  _clamp01(obj["score"]),
        "relevante_motivo": str(obj.get("motivo", "")).strip(),
    }


# =============================================================================
# MAIN
# =============================================================================

def main() -> None:
    load_dotenv()
    load_dotenv(Path(DB_UTILS_DIR) / ".env")
    if not os.getenv("OPENAI_API_KEY"):
        raise RuntimeError("Falta OPENAI_API_KEY en .env")

    args = _parse_args()
    os.makedirs(OUT_DIR, exist_ok=True)

    print("=" * 70)
    print("RELEVANCIA YOUTUBE LLM — ReTo (con caché + BD)")
    print("=" * 70)
    print(f"Modelo: {MODEL} | Threshold: {RELEVANCE_THRESHOLD}")
    print(f"Caché:  {CACHE_FILE}")
    print(f"Output: {OUTPUT_FILE}")
    print()

    # -------------------------------------------------------------------------
    # 1. Caché
    # -------------------------------------------------------------------------
    cache = load_cache()

    # -------------------------------------------------------------------------
    # 2. Pendientes desde BD; fallback a CSV
    # -------------------------------------------------------------------------
    source  = "BD"
    db_rows = fetch_pending_from_db()

    if db_rows is not None and len(db_rows) > 0:
        rows = db_rows
        print(f"\n  Fuente: BASE DE DATOS ({len(rows)} pendientes)")
    elif db_rows is not None and len(db_rows) == 0:
        print("\n✅ BD consultada: 0 comentarios pendientes. Nada que hacer.")
        _print_diagnostics()
        return
    else:
        print("  Sin conexión a BD — usando CSV local.")
        rows = _load_rows_from_csv()
        source = "CSV"

    if not rows:
        print("\n✅ Sin datos de entrada. Nada que hacer.")
        return

    print(f"  - Filas totales en input: {len(rows)}")

    if args.max_rows and args.max_rows > 0:
        rows = rows[: args.max_rows]
        print(f"  - Limitado a: {len(rows)} filas (--max-rows)")

    if args.dry_run:
        print(f"\n[DRY-RUN] {len(rows)} mensajes pendientes de clasificación. Sin llamadas LLM.")
        _print_diagnostics()
        return

    # -------------------------------------------------------------------------
    # 3. Separar en caché vs pendientes
    # -------------------------------------------------------------------------
    rows_from_cache: List[tuple] = []
    rows_to_process: List[Dict[str, Any]] = []

    for r in rows:
        msg_id = str(r.get(ID_COL, "")).strip()
        txt    = str(r.get(TEXT_COL) or "").strip()
        cached = _get_cached(cache, msg_id, txt)
        if cached is not None:
            rows_from_cache.append((r, cached))
        else:
            rows_to_process.append(r)

    print(f"\n  - En caché (a reutilizar):    {len(rows_from_cache)}")
    print(f"  - Pendientes (llamar LLM):    {len(rows_to_process)}")

    if not rows_to_process and not rows_from_cache:
        print("\n✅ Todo ya está en caché. Nada que hacer.")
        return

    # -------------------------------------------------------------------------
    # 4. Preparar archivo de salida CSV (append si ya existe)
    # -------------------------------------------------------------------------
    LLM_EXTRA_COLS = ["relevante_llm", "relevante_score", "relevante_motivo"]
    first_row  = rows_to_process[0] if rows_to_process else rows_from_cache[0][0]
    fieldnames = list(first_row.keys()) + LLM_EXTRA_COLS
    file_exists = os.path.exists(OUTPUT_FILE)
    mode = "a" if file_exists else "w"

    print(f"\nModo de escritura: {'Agregar a existente' if file_exists else 'Crear nuevo'}")

    client = OpenAI()

    # -------------------------------------------------------------------------
    # 5. Procesar
    # -------------------------------------------------------------------------
    nuevos_llm  = 0
    desde_cache = 0
    all_results: List[Dict[str, Any]] = []

    interrupted = False
    try:
        with open(OUTPUT_FILE, mode, encoding="utf-8", newline="") as fo:
            w = csv.DictWriter(fo, fieldnames=fieldnames, extrasaction="ignore",
                               quoting=csv.QUOTE_ALL)
            if not file_exists:
                w.writeheader()

            # 5a. Recuperar del caché
            for r, cached_labels in rows_from_cache:
                merged = {**{str(k): str(v) for k, v in r.items()}, **cached_labels}
                w.writerow(merged)
                all_results.append(merged)
                desde_cache += 1

            if desde_cache:
                fo.flush()
                print(f"  ✓ {desde_cache} filas recuperadas del caché")

            # 5b. Procesar con LLM
            print(f"\nProcesando {len(rows_to_process)} comentarios con LLM...")

            for i, r in enumerate(rows_to_process, 1):
                msg_id = str(r.get(ID_COL, "")).strip()
                txt    = str(r.get(TEXT_COL) or "").strip()

                if txt:
                    extra = _call_llm(client, txt)
                else:
                    extra = {
                        "relevante_llm":    "NO",
                        "relevante_score":  0.0,
                        "relevante_motivo": "Texto vacío",
                    }

                merged = {**{str(k): str(v) for k, v in r.items()}, **extra}
                w.writerow(merged)
                fo.flush()

                if msg_id:
                    _set_cached(cache, msg_id, txt, extra)

                all_results.append(merged)
                nuevos_llm += 1

                if nuevos_llm % CACHE_SAVE_INTERVAL == 0:
                    save_cache(cache)
                    print(f"   💾 Caché guardado ({len(cache)} entradas)")

                if nuevos_llm % 25 == 0 or nuevos_llm == len(rows_to_process):
                    si_count = sum(1 for res in all_results
                                   if res.get("relevante_llm") == "SI")
                    print(f"  Procesados: {nuevos_llm}/{len(rows_to_process)} | "
                          f"relevantes: {si_count}")

    except KeyboardInterrupt:
        interrupted = True
        print(f"\n⚠️  Interrumpido en {nuevos_llm}/{len(rows_to_process)}")
    finally:
        save_cache(cache)
        print(f"  💾 Caché guardado: {len(cache)} entradas "
              f"({nuevos_llm} nuevas en esta ejecución)")

    # -------------------------------------------------------------------------
    # 6. Subir resultados a BD
    # -------------------------------------------------------------------------
    db_uploaded = 0
    if all_results and not interrupted:
        print("\nSubiendo resultados a BD...")
        db_uploaded = upload_results_to_db(all_results)

    # -------------------------------------------------------------------------
    # 7. Resumen
    # -------------------------------------------------------------------------
    si_total = sum(1 for r in all_results if r.get("relevante_llm") == "SI")
    print("\n" + "=" * 70)
    if interrupted:
        print("⚠️  PROCESAMIENTO PARCIAL (interrumpido)")
        print("   Volvé a ejecutar; el caché reutilizará lo ya procesado.")
    else:
        print("✅ RELEVANCIA COMPLETADA")
    print("=" * 70)
    print(f"  - Fuente de datos:            {source}")
    print(f"  - Recuperados del caché:      {desde_cache}")
    print(f"  - Procesados con LLM:         {nuevos_llm}")
    print(f"  - Marcados como SI:           {si_total} / {len(all_results)} "
          f"(threshold={RELEVANCE_THRESHOLD})")
    print(f"  - Subidos a BD:               {db_uploaded}")
    print(f"  - Total en caché:             {len(cache)}")
    print(f"  - Output: {OUTPUT_FILE}")
    print(f"  - Caché:  {CACHE_FILE}")

    if not interrupted:
        _print_diagnostics()


if __name__ == "__main__":
    main()
