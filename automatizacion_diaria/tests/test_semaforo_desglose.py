from datetime import date
import pandas as pd
from secciones.semaforo_diario import (
    _rangos_alerta, _contar_patrones, _tabla_crecimiento,
    _diagnostico_volumen, _TEMAS_SEMAFORO, _COLECTIVOS_SEMAFORO,
)


def test_rangos_no_se_solapan():
    ini_v, fin_v, ini_r, fin_r = _rangos_alerta(date(2026, 10, 22))
    assert (ini_v, fin_v) == (date(2026, 10, 20), date(2026, 10, 22))
    assert (ini_r, fin_r) == (date(2026, 9, 29), date(2026, 10, 19))


def test_contar_patrones_un_mensaje_cuenta_una_vez():
    s = pd.Series(["menas y patera en la frontera", None, "nada"])
    assert _contar_patrones(s, _TEMAS_SEMAFORO)["Inmigración / fronteras"] == 1


def test_tabla_crecimiento_ordena_y_filtra():
    t = _tabla_crecimiento(pd.Series({"a": 10, "b": 3, "c": 8}), 50,
                           pd.Series({"a": 20, "c": 40}), 400)
    assert list(t.index) == ["a", "c"]  # b < MIN_MENSAJES_TEMA


def _serie(alta_v, tot_v):
    df = pd.DataFrame({"fecha": pd.date_range("2026-09-20", "2026-10-22"),
                       "volumen_total": 300, "volumen_alta": 30})
    m = df["fecha"] >= "2026-10-20"
    df.loc[m, "volumen_alta"], df.loc[m, "volumen_total"] = alta_v, tot_v
    return df


def test_diagnostico_mas_odio():
    assert _diagnostico_volumen(_serie(60, 300), date(2026, 10, 22))["tipo"] == "mas_odio"


def test_diagnostico_menos_volumen():
    assert _diagnostico_volumen(_serie(30, 150), date(2026, 10, 22))["tipo"] == "menos_volumen"


def test_regex_sin_falsos_positivos():
    s = pd.Series(["los estados unidos y la comunidad", "la izquierda socialista"])
    assert _contar_patrones(s, _TEMAS_SEMAFORO)["Educación"] == 0
    assert _contar_patrones(s, _COLECTIVOS_SEMAFORO)["Sánchez / PSOE / izquierda"] == 1
