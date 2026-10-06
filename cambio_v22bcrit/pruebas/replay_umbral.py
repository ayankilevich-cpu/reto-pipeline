"""
replay_umbral.py — Prueba de replay de la lógica nueva de umbral/spike (SIN base de datos).

Aplica a un CSV de semanas ya calculadas (r_semana) la MISMA lógica que usará
analisis_contexto_semanal.py (funciones de criterio_etiquetado.py) y la compara con la
simulación del informe de calibración (esperado: 5 de 6 picos detectados).

CSV de entrada (una fila por semana; las líneas que empiezan por # se ignoran):
    semana_inicio,total_mensajes,r_semana_pct,pico_real
    2026-07-20,812,4.1,0
    ...
  - r_semana_pct : % de odio de la semana con el criterio v22bcrit (el r_semana del informe).
  - pico_real    : 1 si el informe de calibración marca esa semana como pico real, 0 si no.

Cómo se aplica la lógica: se tratan las semanas del CSV como semanas post-corte, en orden.
Para cada una, n_previas = nº de semanas anteriores del CSV (+ --previas-postcorte) y
promedio = media de sus r_semana; umbral = umbral_spike(n_previas, promedio). Con las 12
semanas del informe todas están en la fase provisional (5,70 %), salvo que se indique
--previas-postcorte >= 12.

Uso:
    python replay_umbral.py fixtures/replay_ejemplo_SINTETICO.csv \
        --esperado-picos 6 --esperado-detectados 5
    python replay_umbral.py replay_real.csv --esperado-picos 6 --esperado-detectados 5

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

from criterio_etiquetado import es_spike, umbral_spike  # noqa: E402


@dataclass
class Semana:
    semana_inicio: str
    total_mensajes: int
    r_semana_pct: float
    pico_real: bool


@dataclass
class Resultado:
    semana: Semana
    n_previas: int
    umbral: float
    provisional: bool
    spike: bool


def leer_csv(path: Path) -> List[Semana]:
    with open(path, encoding="utf-8", newline="") as f:
        lineas = [ln for ln in f if ln.strip() and not ln.lstrip().startswith("#")]
    out = []
    for r in csv.DictReader(lineas):
        out.append(Semana(
            semana_inicio=r["semana_inicio"].strip(),
            total_mensajes=int(r["total_mensajes"]),
            r_semana_pct=float(r["r_semana_pct"]),
            pico_real=str(r["pico_real"]).strip() in {"1", "true", "True", "si", "sí"},
        ))
    return sorted(out, key=lambda s: s.semana_inicio)


def replay(semanas: List[Semana], previas_postcorte: int = 0) -> List[Resultado]:
    res: List[Resultado] = []
    for i, s in enumerate(semanas):
        anteriores = [x.r_semana_pct for x in semanas[:i]]
        n_prev = previas_postcorte + len(anteriores)
        promedio: Optional[float] = (
            sum(anteriores) / len(anteriores) if anteriores else None
        )
        umbral, prov = umbral_spike(n_prev, promedio)
        res.append(Resultado(s, n_prev, umbral, prov,
                             es_spike(s.r_semana_pct, s.total_mensajes, umbral)))
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
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    ap.add_argument("csv", type=Path)
    ap.add_argument("--previas-postcorte", type=int, default=0,
                    help="semanas post-corte ya acumuladas antes de la primera fila (def. 0)")
    ap.add_argument("--esperado-picos", type=int, default=6)
    ap.add_argument("--esperado-detectados", type=int, default=5)
    a = ap.parse_args(argv)

    res = replay(leer_csv(a.csv), a.previas_postcorte)
    print(f"{'semana':<12}{'msgs':>7}{'r_semana':>10}{'umbral':>9}  {'fase':<12}{'spike':<7}pico_real")
    for r in res:
        print(f"{r.semana.semana_inicio:<12}{r.semana.total_mensajes:>7}"
              f"{r.semana.r_semana_pct:>9.2f}%{r.umbral:>8.2f}%  "
              f"{'provisional' if r.provisional else 'definitivo':<12}"
              f"{'SI' if r.spike else '-':<7}{'SI' if r.semana.pico_real else '-'}")
    m = resumen(res)
    print(f"\nPicos reales: {m['picos_reales']} | detectados: {m['detectados']} | "
          f"perdidos: {m['perdidos']} | falsos positivos: {m['falsos_positivos']}")
    ok = (m["picos_reales"] == a.esperado_picos and m["detectados"] == a.esperado_detectados)
    print(f"Esperado (informe de calibración): {a.esperado_detectados} de {a.esperado_picos} → "
          f"{'COINCIDE' if ok else 'NO COINCIDE'}")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
