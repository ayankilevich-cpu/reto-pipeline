"""
replay_umbral.py — Prueba de replay de la simulación v1 → v22bcrit (SIN base de datos).

Reproduce la simulación del informe de calibración (esperado: 5 de 6 picos detectados).

`r_semana` NO es un % de odio: es la PROPORCIÓN de mensajes ODIO(v1) de esa semana que
siguen siendo ODIO con v22bcrit (0 a 1). La simulación aplica:

    pct_sim    = pct_v1    × r_semana        (% de odio semanal simulado con v22bcrit)
    umbral_sim = umbral_v1 × r_100           (umbral v1 escalado por el r global)
    spike_sim  = pct_sim >= umbral_sim  y  total_mensajes >= 300   (regla de es_spike)

CSV de entrada (una fila por semana; las líneas que empiezan por # se ignoran):

    semana_inicio,total_mensajes,pct_v1,umbral_v1,r_semana,pico_real
    2026-07-20,820,4.10,6.00,0.82,0

  semana_inicio  : lunes de la semana (YYYY-MM-DD)
  total_mensajes : mensajes de la semana
  pct_v1         : pct_odio con criterio v1 (analisis_semanal.pct_odio), en %
  umbral_v1      : umbral_spike_pct que tenía esa semana con v1 (analisis_semanal), en %
  r_semana       : proporción 0-1 de ODIO(v1) que sigue siendo ODIO con v22bcrit
  pico_real      : 1 si el informe marca esa semana como pico real, 0 si no

`r_100` (r global del informe) se pasa con --r100 (un único número 0-1).

Uso:
    python replay_umbral.py fixtures/replay_ejemplo_SINTETICO.csv --r100 0.80 \
        --esperado-picos 6 --esperado-detectados 5

Sale con código 0 si el resultado coincide con lo esperado, 1 si no.
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

_AQUI = Path(__file__).resolve().parent
for _cand in (_AQUI.parent.parent / "automatizacion_diaria",
              _AQUI.parent / "nuevos" / "automatizacion_diaria"):
    if (_cand / "criterio_etiquetado.py").exists():
        sys.path.insert(0, str(_cand))
        break

from criterio_etiquetado import es_spike  # noqa: E402


@dataclass
class Semana:
    semana_inicio: str
    total_mensajes: int
    pct_v1: float
    umbral_v1: float
    r_semana: float
    pico_real: bool


@dataclass
class Resultado:
    semana: Semana
    pct_sim: float
    umbral_sim: float
    spike: bool


def leer_csv(path: Path) -> List[Semana]:
    with open(path, encoding="utf-8", newline="") as f:
        lineas = [ln for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    out = []
    for r in csv.DictReader(lineas):
        r_sem = float(r["r_semana"])
        if not 0.0 <= r_sem <= 1.0:
            raise ValueError(
                f"r_semana={r_sem} fuera de [0, 1] en {r['semana_inicio']}: debe ser una "
                "proporción, no un % de odio."
            )
        out.append(Semana(
            semana_inicio=r["semana_inicio"].strip(),
            total_mensajes=int(r["total_mensajes"]),
            pct_v1=float(r["pct_v1"]),
            umbral_v1=float(r["umbral_v1"]),
            r_semana=r_sem,
            pico_real=str(r["pico_real"]).strip() in {"1", "true", "True", "si", "sí"},
        ))
    return sorted(out, key=lambda s: s.semana_inicio)


def replay(semanas: List[Semana], r100: float) -> List[Resultado]:
    res: List[Resultado] = []
    for s in semanas:
        pct_sim = s.pct_v1 * s.r_semana
        umbral_sim = s.umbral_v1 * r100
        res.append(Resultado(s, pct_sim, umbral_sim,
                             es_spike(pct_sim, s.total_mensajes, umbral_sim)))
    return res


def resumen(res: List[Resultado]) -> dict:
    picos = [r for r in res if r.semana.pico_real]
    return {
        "semanas": len(res),
        "picos_reales": len(picos),
        "detectados": sum(1 for r in picos if r.spike),
        "perdidos": [r.semana.semana_inicio for r in picos if not r.spike],
        "falsos_positivos": [r.semana.semana_inicio for r in res
                             if r.spike and not r.semana.pico_real],
    }


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(description="Replay de la simulación v1 → v22bcrit")
    ap.add_argument("csv", type=Path)
    ap.add_argument("--r100", type=float, required=True,
                    help="r global del informe (0-1); umbral_sim = umbral_v1 × r100")
    ap.add_argument("--esperado-picos", type=int, default=6)
    ap.add_argument("--esperado-detectados", type=int, default=5)
    a = ap.parse_args(argv)
    if not 0.0 <= a.r100 <= 1.0:
        ap.error("--r100 debe estar entre 0 y 1")

    res = replay(leer_csv(a.csv), a.r100)
    print(f"{'semana':<12}{'msgs':>6}{'pct_v1':>8}{'r_sem':>7}{'pct_sim':>9}"
          f"{'umb_v1':>8}{'umb_sim':>9}  {'spike':<6}pico_real")
    for r in res:
        s = r.semana
        print(f"{s.semana_inicio:<12}{s.total_mensajes:>6}{s.pct_v1:>8.2f}{s.r_semana:>7.2f}"
              f"{r.pct_sim:>9.2f}{s.umbral_v1:>8.2f}{r.umbral_sim:>9.2f}  "
              f"{'SI' if r.spike else '-':<6}{'SI' if s.pico_real else '-'}")
    m = resumen(res)
    print(f"\nPicos reales: {m['picos_reales']} | detectados: {m['detectados']} | "
          f"perdidos: {m['perdidos']} | falsos positivos: {m['falsos_positivos']}")
    ok = (m["picos_reales"] == a.esperado_picos and m["detectados"] == a.esperado_detectados)
    print(f"Esperado (informe de calibración): {a.esperado_detectados} de {a.esperado_picos} → "
          f"{'COINCIDE' if ok else 'NO COINCIDE'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
