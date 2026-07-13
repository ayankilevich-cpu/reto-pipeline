#!/usr/bin/env python3
"""
sample_priority_batch.py — Muestra reducida del lote prioritario para primera
corrida de etiquetado LLM.

Toma los hidratado_prioritario_*.csv, ordena por frecuencia_total del término
prioritario (desde terminos_revision_priorizados.csv) y guarda los top-N por
plataforma. No modifica los CSV completos.

Uso:
  python3 pipeline_unificado/sample_priority_batch.py [--sample-n 500]
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Dict, List

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from audit_hate_terms import _normalize_term_for_filter  # noqa: E402

_REPO_ROOT = _SCRIPT_DIR.parent
OUT_DIR = _REPO_ROOT / "outputs" / "pipeline_unificado" / "audit_terminos"

TERMS_CSV = OUT_DIR / "terminos_revision_priorizados.csv"
INPUT_FIELDS = ["message_uuid", "content_original", "termino_prioritario_matcheado"]

DEFAULT_SAMPLE_N = 500


def _load_freq_map(path: Path) -> Dict[str, int]:
    """Lemas normalizado -> frecuencia_total."""
    freq: Dict[str, int] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            lema = (row.get("Lemas") or "").strip()
            nt = _normalize_term_for_filter(lema)
            if not nt:
                continue
            try:
                freq[nt] = int(row.get("frecuencia_total") or 0)
            except ValueError:
                freq[nt] = 0
    return freq


def _read_hidratado(path: Path) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _sample_rows(
    rows: List[Dict[str, str]],
    freq_map: Dict[str, int],
    n: int,
) -> List[Dict[str, str]]:
    scored = []
    for r in rows:
        term = (r.get("termino_prioritario_matcheado") or "").strip()
        nt = _normalize_term_for_filter(term)
        scored.append((freq_map.get(nt, 0), r))
    scored.sort(key=lambda x: (-x[0], x[1].get("message_uuid", "")))
    return [r for _, r in scored[:n]]


def _write_csv(path: Path, rows: List[Dict[str, str]]) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=INPUT_FIELDS, quoting=csv.QUOTE_ALL)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in INPUT_FIELDS})


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--sample-n", type=int, default=DEFAULT_SAMPLE_N,
        help=f"Filas por plataforma (default: {DEFAULT_SAMPLE_N})",
    )
    return p.parse_args()


def main() -> int:
    args = _parse_args()
    n = args.sample_n

    print("=" * 70)
    print(f"MUESTRA PRIORITARIA — top {n} por plataforma (por frecuencia_total)")
    print("=" * 70)

    freq_map = _load_freq_map(TERMS_CSV)
    print(f"  Términos con frecuencia: {len(freq_map):,}")

    batches = [
        ("x", OUT_DIR / "hidratado_prioritario_x.csv",
         OUT_DIR / "hidratado_prioritario_x_muestra500.csv"),
        ("youtube", OUT_DIR / "hidratado_prioritario_youtube.csv",
         OUT_DIR / "hidratado_prioritario_youtube_muestra500.csv"),
    ]

    for platform, in_path, out_path in batches:
        if not in_path.exists():
            print(f"  ✗ No existe {in_path.name} — corré hydrate_priority_messages.py")
            return 1
        rows = _read_hidratado(in_path)
        sample = _sample_rows(rows, freq_map, n)
        _write_csv(out_path, sample)
        print(f"  {platform}: {len(rows):,} → {len(sample):,} filas → {out_path.name}")

    print()
    print("=" * 70)
    print("  Comandos para etiquetar la muestra (local, NO en Actions):")
    print()
    print("    # X")
    print("    python3 pipeline_unificado/etiquetar_llm_unified.py \\")
    print("        --platform x \\")
    print("        --input outputs/pipeline_unificado/audit_terminos/hidratado_prioritario_x_muestra500.csv \\")
    print("        --out   outputs/pipeline_unificado/audit_terminos/hidratado_prioritario_x_muestra500_labeled.csv \\")
    print("        --cache-path outputs/pipeline_unificado/audit_terminos/etiquetado_cache_x_muestra500.json")
    print()
    print("    # YouTube")
    print("    python3 pipeline_unificado/etiquetar_llm_unified.py \\")
    print("        --platform youtube \\")
    print("        --input outputs/pipeline_unificado/audit_terminos/hidratado_prioritario_youtube_muestra500.csv \\")
    print("        --out   outputs/pipeline_unificado/audit_terminos/hidratado_prioritario_youtube_muestra500_labeled.csv \\")
    print("        --cache-path outputs/pipeline_unificado/audit_terminos/etiquetado_cache_youtube_muestra500.json")
    print()
    print("  Subir a BD: load_to_db.py en la corrida siguiente del pipeline.")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
