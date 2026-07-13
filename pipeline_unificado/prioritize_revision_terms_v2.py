#!/usr/bin/env python3
"""
prioritize_revision_terms_v2.py — Prioriza mensajes por COBERTURA de términos con
poca evidencia (set cover greedy), no por frecuencia total.

Reemplaza el criterio de prioritize_revision_terms.py (que rankeaba por frecuencia
total y terminaba priorizando términos genéricos ya obviamente ruido). Aquí el
objetivo es que los términos en "revisión manual" (soporte a+c < 5) alcancen el
umbral mínimo de decisión que usa audit_hate_terms.py, con el menor número de
mensajes a etiquetar posible: cada término necesita `needed = target - soporte`
apariciones más, y se eligen greedy los mensajes que cubren más términos pendientes.

Solo lectura de BD. Todo el output es NUEVO, en
outputs/pipeline_unificado/audit_terminos/. No toca archivos de producción.

Uso:
  python3 pipeline_unificado/prioritize_revision_terms_v2.py [--budget 800] [--target-soporte 5]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List, Set, Tuple

from dotenv import load_dotenv

# Reutilización desde los compañeros de pipeline_unificado/ (misma carpeta):
# conexión/normalización desde audit_hate_terms.py, filtro de exclusión desde
# prioritize_revision_terms.py — fuentes únicas, sin reescribir lógica.
_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from audit_hate_terms import (  # noqa: E402
    _get_db_conn,
    _normalize_term_for_filter,
    _parse_and_normalize_matched_terms,
)
from prioritize_revision_terms import _build_exclusion_set  # noqa: E402

_REPO_ROOT = _SCRIPT_DIR.parent  # Clases/RETO/

OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"
REVISION_MANUAL_CSV = OUT_DIR / "hate_terms_clean_revision_manual.csv"
OUT_MENSAJES = OUT_DIR / "mensajes_prioritarios_v2.csv"
OUT_COBERTURA = OUT_DIR / "cobertura_terminos_v2.csv"

DEFAULT_BUDGET = 800
DEFAULT_TARGET_SOPORTE = 5


# ---------------------------------------------------------------------------
# Fetch (solo lectura)
# ---------------------------------------------------------------------------

def fetch_unlabeled(conn) -> List[Tuple[str, str, str]]:
    """
    Mensajes con matched_terms que NO tienen fila en processed.etiquetas_llm.
    Devuelve (message_uuid, platform, matched_terms).
    """
    query = """
        SELECT m.message_uuid::text, m.platform, m.matched_terms
        FROM processed.mensajes m
        LEFT JOIN processed.etiquetas_llm e USING (message_uuid)
        WHERE e.message_uuid IS NULL
          AND m.matched_terms IS NOT NULL AND m.matched_terms != ''
    """
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    return rows


# ---------------------------------------------------------------------------
# Entrada
# ---------------------------------------------------------------------------

def _load_revision_terms(
    path: Path, target_soporte: int
) -> Tuple[Dict[str, Dict], Dict[str, str]]:
    """
    Lee hate_terms_clean_revision_manual.csv. Devuelve:
      info[norm] = {"Lemas": raw, "soporte": int, "needed": int}
      norm_to_raw[norm] = raw (primera grafía)
    Descarta términos con needed == 0 (ya decidibles).
    """
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}. Corré antes audit_hate_terms.py.")
    info: Dict[str, Dict] = {}
    norm_to_raw: Dict[str, str] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            raw = (row.get("Lemas") or "").strip()
            nt = _normalize_term_for_filter(raw)
            if not nt or nt in info:
                continue
            try:
                soporte = int(row.get("soporte") or 0)
            except ValueError:
                soporte = 0
            needed = max(0, target_soporte - soporte)
            if needed == 0:
                continue
            info[nt] = {"Lemas": raw, "soporte": soporte, "needed": needed}
            norm_to_raw[nt] = raw
    return info, norm_to_raw


# ---------------------------------------------------------------------------
# Selección greedy (set cover)
# ---------------------------------------------------------------------------

def _greedy_select(
    candidates: List[Dict],
    needed: Dict[str, int],
    budget: int,
) -> List[Dict]:
    """
    candidates: [{"message_uuid","platform","terms": set(norm pendientes)}]
    needed: contador mutable por término (se decrementa al seleccionar).
    Elige greedy el mensaje que cubre más términos con needed>0; desempata por
    más términos totales. Para cuando se agota budget o needed queda todo en 0.
    """
    selected: List[Dict] = []
    remaining = list(candidates)

    while remaining and len(selected) < budget and any(v > 0 for v in needed.values()):
        best = None
        best_cover = 0
        best_total = -1
        best_idx = -1
        for i, msg in enumerate(remaining):
            cover = sum(1 for t in msg["terms"] if needed.get(t, 0) > 0)
            if cover == 0:
                continue
            total = len(msg["terms"])
            if cover > best_cover or (cover == best_cover and total > best_total):
                best = msg
                best_cover = cover
                best_total = total
                best_idx = i

        if best is None or best_cover == 0:
            break  # ningún mensaje restante aporta términos pendientes

        selected.append(best)
        remaining.pop(best_idx)
        for t in best["terms"]:
            if needed.get(t, 0) > 0:
                needed[t] -= 1

    return selected


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--budget", type=int, default=DEFAULT_BUDGET,
                   help=f"Máximo de mensajes a seleccionar (default: {DEFAULT_BUDGET})")
    p.add_argument("--target-soporte", type=int, default=DEFAULT_TARGET_SOPORTE,
                   help=f"Umbral de soporte a+c decidible (default: {DEFAULT_TARGET_SOPORTE})")
    return p.parse_args()


def main() -> int:
    args = _parse_args()

    dotenv_path = _REPO_ROOT / "automatizacion_diaria" / ".env"
    load_dotenv(dotenv_path=dotenv_path, override=True)
    print(f"✓ .env: {dotenv_path} (existe={dotenv_path.exists()})")

    print("=" * 70)
    print("PRIORIZACIÓN v2 — cobertura de términos con poca evidencia (set cover)")
    print("=" * 70)

    info, norm_to_raw = _load_revision_terms(REVISION_MANUAL_CSV, args.target_soporte)
    print(f"  Términos revision_manual con needed>0 (target={args.target_soporte}): "
          f"{len(info):,}")

    # Filtro de exclusión (mismo que prioritize_revision_terms.py).
    exclusion_set = _build_exclusion_set()
    excluded = [nt for nt in info if nt in exclusion_set]
    for nt in excluded:
        del info[nt]
        del norm_to_raw[nt]
    print(f"  Excluidos por stopwords/exclusión oficial: {len(excluded):,} "
          f"({len(info):,} términos restantes)")
    if excluded:
        preview = ", ".join(norm for norm in excluded[:10])
        print(f"  Primeros 10 excluidos: {preview}")

    if not info:
        print("✗ No quedan términos para cubrir tras el filtro. Nada que hacer.")
        return 0

    pendientes: Set[str] = set(info.keys())
    needed: Dict[str, int] = {nt: info[nt]["needed"] for nt in info}

    conn = _get_db_conn()
    if conn is None:
        print("✗ Sin conexión a BD. Verificá DATABASE_URL en .env")
        return 1
    try:
        unlabeled = fetch_unlabeled(conn)
    finally:
        conn.close()
    print(f"  Mensajes sin etiqueta con matched_terms: {len(unlabeled):,}")

    # Candidatos: mensajes que matchean al menos un término pendiente.
    candidates: List[Dict] = []
    for message_uuid, platform, matched_terms in unlabeled:
        terms = set(_parse_and_normalize_matched_terms(matched_terms)) & pendientes
        if terms:
            candidates.append({
                "message_uuid": message_uuid,
                "platform": platform or "",
                "terms": terms,
            })
    print(f"  Candidatos (matchean ≥1 término pendiente): {len(candidates):,}")

    selected = _greedy_select(candidates, needed, args.budget)

    _write_outputs(selected, info, norm_to_raw, needed)
    _print_summary(selected, info, needed)
    return 0


def _write_outputs(
    selected: List[Dict],
    info: Dict[str, Dict],
    norm_to_raw: Dict[str, str],
    needed_after: Dict[str, int],
) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    with open(OUT_MENSAJES, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["message_uuid", "platform", "terminos_pendientes_matcheados"])
        for msg in selected:
            terms_raw = sorted(norm_to_raw[t] for t in msg["terms"])
            w.writerow([msg["message_uuid"], msg["platform"], "|".join(terms_raw)])

    with open(OUT_COBERTURA, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["Lemas", "soporte_actual", "needed_antes",
                    "needed_despues_proyectado", "cubierto_completo"])
        for nt, meta in sorted(info.items(), key=lambda kv: kv[1]["Lemas"]):
            needed_after_val = max(0, needed_after.get(nt, 0))
            w.writerow([
                meta["Lemas"],
                meta["soporte"],
                meta["needed"],
                needed_after_val,
                needed_after_val == 0,
            ])

    print()
    print(f"  Escrito: {OUT_MENSAJES} ({len(selected):,} filas)")
    print(f"  Escrito: {OUT_COBERTURA} ({len(info):,} filas)")


def _print_summary(
    selected: List[Dict],
    info: Dict[str, Dict],
    needed_after: Dict[str, int],
) -> None:
    cubiertos = sum(1 for nt in info if needed_after.get(nt, 0) == 0)
    total_terms = len(info)
    total_cover = sum(len(m["terms"]) for m in selected)
    avg_cover = (total_cover / len(selected)) if selected else 0.0

    print()
    print("=" * 70)
    print("RESUMEN")
    print("=" * 70)
    print(f"  Términos que entraron (post-filtro):     {total_terms:,}")
    print(f"  ├─ quedarían completamente cubiertos:     {cubiertos:,}")
    print(f"  └─ aún incompletos tras el lote:          {total_terms - cubiertos:,}")
    print(f"  Mensajes seleccionados:                   {len(selected):,}")
    print(f"  Términos pendientes cubiertos por mensaje (promedio): {avg_cover:.2f}")
    print("  (referencia: el script v1 cubría ~1 término por mensaje)")
    print("=" * 70)


if __name__ == "__main__":
    sys.exit(main())
