"""
test_anotacion_t24.py — Lógica pura de la pestaña «Muestra T2.4» (sin BD ni Streamlit runtime).
Ejecutar: pytest automatizacion_diaria/tests/test_anotacion_t24.py -v
"""
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent.parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

import anotacion_t24 as m  # noqa: E402


def test_sin_clasificacion_da_error():
    assert m.validar_anotacion(None, [], None, None, None)


def test_odio_exige_colectivo_categoria_intensidad():
    errores = m.validar_anotacion("ODIO", [], None, None, None)
    assert len(errores) == 3


def test_dos_colectivos_exigen_tipo_cruce():
    errores = m.validar_anotacion("ODIO", ["mujeres", "migrantes"], None, "odio_etnico_cultural_religioso", 2)
    assert any("cruce" in e for e in errores)
    assert m.validar_anotacion("ODIO", ["mujeres", "migrantes"], "combinado", "odio_etnico_cultural_religioso", 2) == []


def test_persona_concreta_no_cuenta_como_colectivo():
    assert m.n_colectivos_reales(["persona_concreta", "mujeres"]) == 1
    assert m.validar_anotacion("ODIO", ["persona_concreta", "mujeres"], None, "odio_personal_generacional", 1) == []


def test_no_odio_y_dudoso_validos_sin_mas_campos():
    assert m.validar_anotacion("NO_ODIO", [], None, None, None) == []
    assert m.validar_anotacion("DUDOSO", [], None, None, None) == []


def test_normalizar_no_odio_vacia_campos_de_odio():
    a = m.normalizar_anotacion("NO_ODIO", ["mujeres"], True, "combinado", "odio_x", 3, ["sexualizacion"], True, " ")
    assert a["colectivos"] == [] and a["tipo_cruce"] is None
    assert a["categoria_principal"] is None and a["intensidad"] is None
    assert a["narrativas"] == [] and a["humor_flag"] is False and a["observaciones"] is None
    assert a["insulto_identitario_generico"] is True


def test_normalizar_descarta_codigos_desconocidos():
    a = m.normalizar_anotacion("ODIO", ["mujeres", "inventado"], False, None, "odio_etnico_cultural_religioso", 2, ["nada"], False, None)
    assert a["colectivos"] == ["mujeres"] and a["narrativas"] == []


def test_campos_gold_mismo_formato_que_otras_pestanas():
    odio = m.campos_gold(m.normalizar_anotacion("ODIO", ["mujeres"], False, None, "odio_genero_identidad_orientacion", 3, [], True, None))
    assert (odio["odio_flag"], odio["y_odio_final"], odio["y_odio_bin"], odio["intensidad"]) == (True, "Odio", 1, 3)
    no = m.campos_gold(m.normalizar_anotacion("NO_ODIO", [], False, None, None, None, [], False, None))
    assert (no["odio_flag"], no["categoria_odio"], no["y_odio_final"], no["y_odio_bin"]) == (False, "no_odio", "No Odio", 0)
    du = m.campos_gold(m.normalizar_anotacion("DUDOSO", [], False, None, None, None, [], False, None))
    assert (du["odio_flag"], du["categoria_odio"], du["y_odio_final"], du["y_odio_bin"]) == (None, "dudoso", "Dudoso", None)


def test_categorias_coinciden_con_constants():
    from components.constants import CATEGORIAS_LABELS
    assert set(m.CATEGORIAS) == set(CATEGORIAS_LABELS)


def test_avance_incluye_sin_empezar_y_calcula_estado():
    import pandas as pd
    db = pd.DataFrame([
        {"annotator_id": "Cppa", "anotados": 240, "hoy": 10, "ult_7d": 50, "odio": 30, "seg_medios": 20, "ultima": None},
        {"annotator_id": "Movi", "anotados": 60, "hoy": 0, "ult_7d": 60, "odio": 5, "seg_medios": 25, "ultima": None},
        {"annotator_id": "Admin", "anotados": 3, "hoy": 3, "ult_7d": 3, "odio": 1, "seg_medios": 9, "ultima": None},
    ])
    df = m.combinar_avance(db, ["Cifal", "Cppa", "Movi"], meta=240)
    est = dict(zip(df["Usuario"], df["Estado"]))
    assert est == {"Cppa": "Completado", "Movi": "En curso", "Cifal": "Sin empezar", "Admin": "Fuera de campaña"}
    av = dict(zip(df["Usuario"], df["Avance"]))
    assert av["Cppa"] == 100 and av["Movi"] == 25 and av["Cifal"] == 0
    assert list(df["Usuario"])[-1] == "Admin"  # fuera de campaña al final


def test_avance_bd_vacia():
    import pandas as pd
    df = m.combinar_avance(pd.DataFrame(), ["Cppa", "Guajira"])
    assert set(df["Usuario"]) == {"Cppa", "Guajira"}
    assert (df["Anotados"] == 0).all() and (df["Estado"] == "Sin empezar").all()
