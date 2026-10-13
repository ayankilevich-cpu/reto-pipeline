"""
test_analisis_semanal_pendientes.py — Qué semanas recalcula analisis_contexto_semanal.py.

Motivado por oct 2026: la corrida del DOMINGO marcaba la semana como "cerrada"
(analisis_date >= domingo), así que la carga del lunes (mensajes del domingo) y el
etiquetado LLM manual que llegaba después nunca entraban en pct_odio.

No requiere BD — solo la lógica pura de selección de semanas.
Ejecutar: pytest automatizacion_diaria/tests/test_analisis_semanal_pendientes.py -v
"""
import sys
from datetime import date
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import analisis_contexto_semanal as acs  # noqa: E402

LUN_12 = date(2026, 10, 12)   # semana 12/10 → domingo 18/10
LUN_19 = date(2026, 10, 19)   # semana 19/10 → domingo 25/10 (primera v22bcrit)


def test_semana_analizada_el_domingo_no_queda_cerrada():
    """Bug original: análisis guardado el domingo 25/10 → el lunes 26/10 debe re-procesarse."""
    ultimo = {LUN_19: date(2026, 10, 25)}
    assert acs.motivo_pendiente(LUN_19, ultimo, hoy=date(2026, 10, 26)) == "ventana"


def test_ventana_recalcula_hasta_7_dias_despues_del_domingo():
    ultimo = {LUN_19: date(2026, 10, 31)}  # ya analizada después del domingo
    assert acs.motivo_pendiente(LUN_19, ultimo, hoy=date(2026, 11, 1)) == "ventana"   # domingo+7
    assert acs.motivo_pendiente(LUN_19, ultimo, hoy=date(2026, 11, 2)) is None        # domingo+8 → cerrada


def test_fuera_de_ventana_con_cierre_incompleto_se_reprocesa_una_vez():
    ultimo = {LUN_12: date(2026, 10, 18)}  # se guardó el domingo
    assert acs.motivo_pendiente(LUN_12, ultimo, hoy=date(2026, 11, 10)) == "cierre_incompleto"
    ultimo = {LUN_12: date(2026, 11, 10)}  # tras reprocesar
    assert acs.motivo_pendiente(LUN_12, ultimo, hoy=date(2026, 11, 11)) is None


def test_semana_sin_analisis_se_procesa():
    assert acs.motivo_pendiente(LUN_12, {}, hoy=date(2026, 12, 1)) == "sin_analisis"


def test_semanas_pendientes_filtra_y_conserva_orden():
    semanas = [date(2026, 9, 28), date(2026, 10, 5), LUN_12, LUN_19]
    ultimo = {
        date(2026, 9, 28): date(2026, 10, 6),   # cerrada bien (después del domingo 4/10, fuera de ventana)
        date(2026, 10, 5): date(2026, 10, 11),  # guardada el domingo 11/10 → cierre incompleto
        LUN_12: date(2026, 10, 19),
        LUN_19: date(2026, 10, 21),
    }
    hoy = date(2026, 10, 21)
    assert acs.semanas_pendientes(semanas, ultimo, hoy) == [date(2026, 10, 5), LUN_12, LUN_19]
