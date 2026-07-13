#!/usr/bin/env python3
"""
hydrate_priority_messages_v2.py — Hidrata mensajes prioritarios v2 con su texto
para etiquetar_llm_unified.py.

Toma mensajes_prioritarios_v2.csv (salida de prioritize_revision_terms_v2.py:
message_uuid + platform + terminos_pendientes_matcheados con lista "|"-separada),
trae content_original desde processed.mensajes y produce dos CSV — uno por
plataforma — con el esquema que espera etiquetar_llm_unified.py vía --input:
message_uuid + content_original (+ terminos_pendientes_matcheados para trazabilidad).

Solo lectura de BD. No escribe en processed.etiquetas_llm ni en ninguna tabla.
Todo el output es NUEVO, en outputs/pipeline_unificado/audit_terminos/.

Uso:
  python3 pipeline_unificado/hydrate_priority_messages_v2.py
"""
from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Dict, List

from dotenv import load_dotenv

# Reutilización desde el compañero audit_hate_terms.py (misma carpeta):
# conexión a BD como fuente única, igual que los scripts anteriores.
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from audit_hate_terms import _get_db_conn  # noqa: E402

_REPO_ROOT = _SCRIPT_DIR.parent  # Clases/RETO/

OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"
PRIORITY_CSV = OUT_DIR / "mensajes_prioritarios_v2.csv"
OUT_X = OUT_DIR / "hidratado_v2_x.csv"
OUT_YOUTUBE = OUT_DIR / "hidratado_v2_youtube.csv"

# Esquema mínimo que consume etiquetar_llm_unified.py (--input): message_uuid +
# content_original. terminos_pendientes_matcheados se conserva para trazabilidad.
OUTPUT_FIELDS = ["message_uuid", "content_original", "terminos_pendientes_matcheados"]


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------

def _read_priority_csv(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(
            f"No existe {path}. Corré antes prioritize_revision_terms_v2.py."
        )
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


# ---------------------------------------------------------------------------
# Fetch (solo lectura)
# ---------------------------------------------------------------------------

def fetch_texts(conn, uuids: List[str]) -> Dict[str, str]:
    """message_uuid -> content_original para los UUIDs pedidos (solo lectura)."""
    texts: Dict[str, str] = {}
    if not uuids:
        return texts
    query = """
        SELECT message_uuid::text, content_original
        FROM processed.mensajes
        WHERE message_uuid::text = ANY(%s)
    """
    cur = conn.cursor()
    cur.execute(query, (uuids,))
    for uuid, content in cur.fetchall():
        texts[uuid] = content or ""
    cur.close()
    return texts


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    dotenv_path = _REPO_ROOT / "automatizacion_diaria" / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"✓ .env: {dotenv_path} (existe={dotenv_path.exists()})")

    print("=" * 70)
    print("HIDRATACIÓN v2 — mensajes prioritarios (set cover) para etiquetado LLM")
    print("=" * 70)

    priority_rows = _read_priority_csv(PRIORITY_CSV)
    print(f"  Mensajes prioritarios en entrada: {len(priority_rows):,}")

    uuids = sorted({
        (r.get("message_uuid") or "").strip()
        for r in priority_rows
        if (r.get("message_uuid") or "").strip()
    })
    print(f"  message_uuid distintos: {len(uuids):,}")

    conn = _get_db_conn()
    if conn is None:
        print("✗ Sin conexión a BD. Verificá DATABASE_URL en .env")
        return 1

    try:
        texts = fetch_texts(conn, uuids)
    finally:
        conn.close()
    print(f"  Textos recuperados de processed.mensajes: {len(texts):,}")

    rows_by_platform: Dict[str, List[Dict[str, str]]] = {"x": [], "youtube": []}
    sin_texto = 0
    uuids_sin_texto: List[str] = []
    otras_plataformas: Dict[str, int] = {}

    for r in priority_rows:
        uuid = (r.get("message_uuid") or "").strip()
        platform = (r.get("platform") or "").strip().lower()
        terminos = (r.get("terminos_pendientes_matcheados") or "").strip()
        content = texts.get(uuid, "")
        if not uuid or not content:
            sin_texto += 1
            if uuid:
                uuids_sin_texto.append(uuid)
            continue
        record = {
            "message_uuid": uuid,
            "content_original": content,
            "terminos_pendientes_matcheados": terminos,
        }
        if platform in rows_by_platform:
            rows_by_platform[platform].append(record)
        else:
            otras_plataformas[platform] = otras_plataformas.get(platform, 0) + 1

    _write_csv(OUT_X, rows_by_platform["x"])
    _write_csv(OUT_YOUTUBE, rows_by_platform["youtube"])
    _print_summary(rows_by_platform, sin_texto, uuids_sin_texto, otras_plataformas)
    return 0


def _write_csv(path: Path, records: List[Dict[str, str]]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=OUTPUT_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in records:
            w.writerow(r)


def _print_summary(
    rows_by_platform: Dict[str, List[Dict[str, str]]],
    sin_texto: int,
    uuids_sin_texto: List[str],
    otras_plataformas: Dict[str, int],
) -> None:
    print()
    print(f"  Escrito: {OUT_X} ({len(rows_by_platform['x']):,} filas)")
    print(f"  Escrito: {OUT_YOUTUBE} ({len(rows_by_platform['youtube']):,} filas)")

    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  Filas X:                 {len(rows_by_platform['x']):,}")
    print(f"  Filas YouTube:           {len(rows_by_platform['youtube']):,}")
    print(f"  Sin texto (descartadas): {sin_texto:,}")
    if uuids_sin_texto:
        print(f"  UUIDs sin texto: {', '.join(uuids_sin_texto[:5])}"
              + (f" ... (+{len(uuids_sin_texto) - 5} más)" if len(uuids_sin_texto) > 5 else ""))
    if otras_plataformas:
        otras = ", ".join(f"{k}={v}" for k, v in sorted(otras_plataformas.items()))
        print(f"  Otras plataformas (no exportadas): {otras}")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
