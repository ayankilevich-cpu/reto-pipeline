#!/usr/bin/env python3
"""
Regenera `terminos_exclusion_oficial.py` desde `terminos_excluidos_visualizacion.json`.

Uso (desde esta carpeta):
  python sync_terminos_exclusion_oficial.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
JSON_PATH = ROOT / "terminos_excluidos_visualizacion.json"
OUT_PATH = ROOT / "terminos_exclusion_oficial.py"


def main() -> int:
    if not JSON_PATH.exists():
        print(f"No existe {JSON_PATH}", file=sys.stderr)
        return 1
    data = json.loads(JSON_PATH.read_text(encoding="utf-8"))
    raw = data.get("excluir") if isinstance(data, dict) else data
    if not isinstance(raw, list) or not raw:
        print("JSON sin lista 'excluir' válida.", file=sys.stderr)
        return 1
    terms = sorted(set(str(x).strip() for x in raw if str(x).strip()), key=str.lower)

    lines = [
        '"""',
        "Lista oficial de lemas excluidos en la sección «Términos frecuentes».",
        "",
        f"Fuente: {JSON_PATH.name}",
        f"Regenerar: python {Path(__file__).name}",
        '"""',
        "",
        "from __future__ import annotations",
        "",
        "# Una sola fuente en runtime: importada por dashboard.py",
        "TERMINOS_EXCLUSION_LEMAS: frozenset[str] = frozenset({",
    ]
    for t in terms:
        lines.append(f"    {repr(t)},")
    lines.append("})")
    lines.append("")
    OUT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"OK {OUT_PATH} ({len(terms)} lemas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
