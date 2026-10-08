"""
Pruebas del cambio v22bcrit. No necesitan BD ni credenciales.

Ejecutar (desde la raíz del repo):
    pytest cambio_v22bcrit/pruebas -v

Antes de desplegar los parches se prueban los módulos nuevos (criterio_etiquetado.py,
replay). Las pruebas que tocan load_to_db.py / analisis_contexto_semanal.py se saltan
solas mientras esos parches no estén aplicados.
"""
from __future__ import annotations

import logging
import re
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

AQUI = Path(__file__).resolve().parent
RAIZ = AQUI.parent.parent
AUTO = RAIZ / "automatizacion_diaria"
NUEVOS = AQUI.parent / "nuevos" / "automatizacion_diaria"
for p in (AUTO if (AUTO / "criterio_etiquetado.py").exists() else NUEVOS, AQUI):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

import criterio_etiquetado as ce  # noqa: E402
import replay_umbral as rp  # noqa: E402

FIXTURE = AQUI / "fixtures" / "replay_ejemplo_SINTETICO.csv"


# ── Corte por semana ─────────────────────────────────────────────────────────
def test_corte_es_lunes():
    assert ce.CRITERIO_CORTE_SEMANA.weekday() == 0


def test_inicio_semana_igual_que_postgres():
    assert ce.inicio_semana(date(2026, 10, 18)) == date(2026, 10, 12)  # domingo
    assert ce.inicio_semana(date(2026, 10, 19)) == date(2026, 10, 19)  # lunes


@pytest.mark.parametrize("fecha,esperado", [
    (date(2026, 10, 18), ce.CRITERIO_V1),        # domingo anterior al corte
    (date(2026, 10, 19), ce.CRITERIO_V22BCRIT),  # lunes del corte
    (date(2026, 10, 25), ce.CRITERIO_V22BCRIT),  # domingo de la semana del corte
    ("2026-10-18 23:59:00+00:00", ce.CRITERIO_V1),
    ("2026-10-19T00:00:00Z", ce.CRITERIO_V22BCRIT),
    (None, ce.CRITERIO_V1),
    ("nan", ce.CRITERIO_V1),
    ("", ce.CRITERIO_V1),
])
def test_criterio_para_fecha(fecha, esperado):
    assert ce.criterio_para_fecha(fecha) == esperado


def test_zona_horaria_se_pasa_a_utc():
    # 18/10 23:30 en UTC-5 = 19/10 04:30 UTC → semana del corte (como created_at::date en Neon/UTC)
    dt = datetime(2026, 10, 18, 23, 30, tzinfo=timezone(timedelta(hours=-5)))
    assert ce.criterio_para_fecha(dt) == ce.CRITERIO_V22BCRIT


def test_mensaje_tardio_sigue_el_criterio_de_su_semana_de_publicacion():
    # publicado el 15/10 (v1), se ingesta/etiqueta el 22/10: la función solo mira la publicación
    assert ce.criterio_para_fecha(date(2026, 10, 15)) == ce.CRITERIO_V1


def test_toda_la_semana_comparte_criterio():
    for ini in (date(2026, 10, 12), date(2026, 10, 19), date(2026, 11, 2)):
        criterios = {ce.criterio_para_fecha(ini + timedelta(days=i)) for i in range(7)}
        assert len(criterios) == 1


# ── Aislamiento de plataforma ─────────────────────────────────────────────────

def test_youtube_siempre_v1_independiente_de_fecha():
    """YouTube nunca debe recibir v22bcrit, independientemente de la fecha."""
    assert ce.criterio_para_fecha(date(2026, 10, 19), platform="youtube") == ce.CRITERIO_V1
    assert ce.criterio_para_fecha(date(2026, 12, 1),  platform="youtube") == ce.CRITERIO_V1
    assert ce.criterio_para_fecha(None,               platform="youtube") == ce.CRITERIO_V1


def test_x_post_corte_recibe_v22bcrit():
    assert ce.criterio_para_fecha(date(2026, 10, 19), platform="x") == ce.CRITERIO_V22BCRIT


def test_x_pre_corte_sigue_siendo_v1():
    assert ce.criterio_para_fecha(date(2026, 10, 18), platform="x") == ce.CRITERIO_V1


def test_plataforma_desconocida_es_v1():
    """Cualquier plataforma que no sea 'x' cae a v1 (fail-safe)."""
    for plat in ("youtube", "tiktok", "instagram", "", "X", "X ".strip()):
        assert ce.criterio_para_fecha(date(2026, 10, 25), platform=plat) == ce.CRITERIO_V1, \
            f"platform={plat!r} debería ser v1"


def test_default_platform_es_x():
    """Sin pasar platform=, el comportamiento histórico de X se preserva."""
    assert ce.criterio_para_fecha(date(2026, 10, 19)) == ce.CRITERIO_V22BCRIT
    assert ce.criterio_para_fecha(date(2026, 10, 18)) == ce.CRITERIO_V1


# ── Umbral ───────────────────────────────────────────────────────────────────
def test_umbral_provisional_hasta_12_semanas():
    assert ce.UMBRAL_PROVISIONAL_PCT == 5.70
    for n in (0, 1, 11):
        assert ce.umbral_spike(n, 9.0) == (5.70, True)
    assert ce.umbral_spike(5, None) == (5.70, True)


def test_umbral_definitivo_desde_12_semanas():
    assert ce.umbral_spike(12, 4.0) == (6.0, False)
    assert ce.umbral_spike(20, 3.8) == (5.7, False)


def test_es_spike_reglas():
    assert ce.es_spike(5.70, 300, 5.70) is True    # pct == umbral cuenta
    assert ce.es_spike(5.69, 300, 5.70) is False
    assert ce.es_spike(9.0, 299, 5.70) is False    # volumen insuficiente


# ── Replay (r_semana = proporción de ODIO(v1) que sigue siendo ODIO con v22bcrit) ──
def test_replay_sintetico_5_de_6():
    res = rp.replay(rp.leer_csv(FIXTURE), r100=0.80)
    m = rp.resumen(res)
    assert (m["picos_reales"], m["detectados"]) == (6, 5)
    assert m["perdidos"] == ["2026-08-10"]
    assert m["falsos_positivos"] == []


def test_replay_formula():
    s = rp.Semana("2026-07-27", 790, 8.0, 6.0, 0.80, True)
    (r,) = rp.replay([s], r100=0.80)
    assert r.pct_sim == pytest.approx(6.4)       # pct_v1 × r_semana
    assert r.umbral_sim == pytest.approx(4.8)    # umbral_v1 × r_100
    assert r.spike is True


def test_replay_exige_300_mensajes():
    (r,) = rp.replay([rp.Semana("2026-07-27", 299, 9.0, 6.0, 0.9, True)], r100=0.8)
    assert r.spike is False


def test_replay_rechaza_r_semana_que_parece_porcentaje(tmp_path):
    f = tmp_path / "x.csv"
    f.write_text("semana_inicio,total_mensajes,pct_v1,umbral_v1,r_semana,pico_real\n"
                 "2026-07-27,800,8.0,6.0,5.7,1\n", encoding="utf-8")
    with pytest.raises(ValueError, match="proporción"):
        rp.leer_csv(f)


def test_replay_cli_coincide_y_discrepa():
    base = [str(FIXTURE), "--r100", "0.80", "--esperado-picos", "6"]
    assert rp.main(base + ["--esperado-detectados", "5"]) == 0
    assert rp.main(base + ["--esperado-detectados", "6"]) == 1


# ── Consistencia de la constante de corte entre copias ───────────────────────
_EXCLUIR_DIRS = frozenset({".venv", ".git", "__pycache__", "site-packages"})


def _py_files(base: Path):
    """rglob("*.py") sin .venv, .git, __pycache__, site-packages ni carpetas ocultas."""
    for f in base.rglob("*.py"):
        partes = f.relative_to(base).parts
        if not any(p.startswith(".") or p in _EXCLUIR_DIRS for p in partes):
            yield f


def test_corte_consistente():
    patron = re.compile(r"^CRITERIO_CORTE_SEMANA\s*=\s*date\(([^)]*)\)", re.M)
    valores = {}
    for base in (AUTO, AQUI.parent / "nuevos" / "automatizacion_diaria"):
        for f in _py_files(base):
            m = patron.search(f.read_text(encoding="utf-8"))
            if m:
                valores[str(f)] = m.group(1).replace(" ", "")
    assert valores, "no se encontró ninguna definición de CRITERIO_CORTE_SEMANA"
    assert len(set(valores.values())) == 1, valores
    # el monolito del dashboard (se despliega solo) lleva su propia copia
    v3 = AUTO / "Diseñador Web Reto" / "dashboard_v3.py"
    if v3.exists() and "CRITERIO_CORTE_SEMANA" in v3.read_text(encoding="utf-8"):
        assert patron.search(v3.read_text(encoding="utf-8")).group(1).replace(" ", "") \
            == next(iter(valores.values()))


def test_modulo_nuevo_igual_en_repo_y_en_nuevos():
    en_repo = AUTO / "criterio_etiquetado.py"
    en_nuevos = NUEVOS / "criterio_etiquetado.py"
    if en_repo.exists() and en_nuevos.exists():
        assert en_repo.read_text(encoding="utf-8") == en_nuevos.read_text(encoding="utf-8")


# ── load_to_db.py (solo si el parche está aplicado) ──────────────────────────
@pytest.fixture
def loader(monkeypatch, tmp_path):
    sys.path.insert(0, str(AUTO))
    import load_to_db
    if not hasattr(load_to_db, "LLM_CRITERIO_STRICT"):
        pytest.fail("parche de load_to_db.py sin aplicar")
    return load_to_db


def _csv_llm(tmp_path, filas, con_criterio=True):
    import csv
    ruta = tmp_path / "etiquetado_llm_completo.csv"
    cols = ["message_uuid", "created_at", "clasificacion_principal",
            "categoria_odio_pred", "intensidad_pred", "resumen_motivo"]
    if con_criterio:
        cols.append("llm_criterio")
    with open(ruta, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        w.writerows(filas)
    return ruta


def _fila(uuid, created_at, criterio=None):
    d = {"message_uuid": uuid, "created_at": created_at, "clasificacion_principal": "ODIO",
         "categoria_odio_pred": "odio_ideologico_politico", "intensidad_pred": "2",
         "resumen_motivo": "x"}
    if criterio:
        d["llm_criterio"] = criterio
    return d


def test_loader_guarda_criterio_y_omite_incoherentes(loader, tmp_path, monkeypatch):
    ruta = _csv_llm(tmp_path, [
        _fila("u1", "2026-10-12 10:00:00+00:00", "v1"),
        _fila("u2", "2026-10-20 10:00:00+00:00", "v22bcrit"),
        _fila("u3", "2026-10-20 10:00:00+00:00", "v1"),        # post-corte con prompt viejo → omitir
        _fila("u4", "2026-10-12 10:00:00+00:00", "v22bcrit"),  # pre-corte con prompt nuevo → omitir
    ])
    monkeypatch.setattr(loader, "find_llm_csv", lambda: ruta)
    captura = {}

    def falso_upsert(conn, tabla, columns, rows, conflict_columns, update_columns=None):
        captura.update(columns=columns, rows=rows, conflict=conflict_columns, upd=update_columns)
        return len(rows)

    monkeypatch.setattr(loader, "upsert_rows", falso_upsert)
    n = loader.load_etiquetas_llm(MagicMock(), logging.getLogger("t"))
    assert n == 2
    assert captura["conflict"] == ["message_uuid", "llm_version"]   # PK intacta
    assert "llm_criterio" in captura["columns"] and "llm_criterio" in captura["upd"]
    assert {(r[0], r[5], r[6]) for r in captura["rows"]} == {
        ("u1", "v1", "v1"), ("u2", "v1", "v22bcrit")}               # llm_version sigue 'v1'


def test_loader_csv_antiguo_sin_columna_es_v1(loader, tmp_path, monkeypatch):
    ruta = _csv_llm(tmp_path, [_fila("u1", "2026-09-01 10:00:00+00:00")], con_criterio=False)
    monkeypatch.setattr(loader, "find_llm_csv", lambda: ruta)
    capt = {}
    monkeypatch.setattr(loader, "upsert_rows",
                        lambda conn, t, c, rows, **k: capt.setdefault("rows", rows) and len(rows))
    loader.load_etiquetas_llm(MagicMock(), logging.getLogger("t"))
    assert capt["rows"][0][6] == "v1"


# ── analisis_contexto_semanal.py (solo si el parche está aplicado) ───────────
def test_baseline_postcorte_solo_cuenta_semanas_nuevas(monkeypatch):
    sys.path.insert(0, str(AUTO))
    pytest.importorskip("openai")
    pytest.importorskip("pandas")
    import pandas as pd
    import analisis_contexto_semanal as acs
    if not hasattr(acs, "UMBRAL_PROVISIONAL_PCT"):
        pytest.fail("parche de analisis_contexto_semanal.py sin aplicar")
    llamadas = []

    def falso_read_sql(sql, conn, params=None):
        llamadas.append((sql, params))
        return pd.DataFrame(columns=["semana", "total", "odio"])

    monkeypatch.setattr(acs.pd, "read_sql", falso_read_sql)
    corte = acs.CRITERIO_CORTE_SEMANA
    # semana post-corte sin semanas previas del criterio nuevo → referencia coherente con el provisional
    assert acs.compute_avg_pct_prior_to_week(None, corte + timedelta(days=14)) == (3.8, 0)
    assert corte in llamadas[-1][1] and ">= %s" in llamadas[-1][0]
    # semana pre-corte → consulta de siempre (sin filtro de corte) y fallback histórico 3.0
    assert acs.compute_avg_pct_prior_to_week(None, corte - timedelta(days=14)) == (3.0, 0)
    assert corte not in llamadas[-1][1]


# ── etiquetar_completo_llm.py (solo si el parche está aplicado) ──────────────
@pytest.fixture
def etiquetador():
    pytest.importorskip("openai")
    pytest.importorskip("dotenv")
    sys.path.insert(0, str(RAIZ / "Medios" / "ML" / "etiquetado_llm"))
    import etiquetar_completo_llm as el
    if not hasattr(el, "config_criterio"):
        pytest.fail("parche de etiquetar_completo_llm.py sin aplicar")
    el._CONFIG_V22BCRIT = None
    return el


def test_etiquetador_elige_criterio_por_fecha_de_publicacion(etiquetador):
    assert etiquetador.criterio_de_fila({"created_at": "2026-10-12 09:00:00+00:00"}) == "v1"
    assert etiquetador.criterio_de_fila({"created_at": "2026-10-20 09:00:00+00:00"}) == "v22bcrit"


def test_etiquetador_se_niega_con_prompt_sin_rellenar(etiquetador, tmp_path, monkeypatch):
    f = tmp_path / "prompt.json"
    f.write_text('{"system": "__PENDIENTE__", "user": "__PENDIENTE__ {txt}"}', encoding="utf-8")
    monkeypatch.setattr(etiquetador, "PROMPT_V22BCRIT_FILE", f)
    with pytest.raises(RuntimeError, match="incompleto"):
        etiquetador.config_criterio("v22bcrit")
    # v1 no depende del archivo y conserva el prompt actual
    assert etiquetador.config_criterio("v1")["user"] == etiquetador.USER_TMPL


def test_etiquetador_prompt_v22bcrit_admite_llaves_literales(etiquetador, tmp_path, monkeypatch):
    f = tmp_path / "prompt.json"
    f.write_text('{"system": "S", "user": "Devuelve {\\"a\\": 1}. MENSAJE: {txt}", "model": "m-x"}',
                 encoding="utf-8")
    monkeypatch.setattr(etiquetador, "PROMPT_V22BCRIT_FILE", f)
    cfg = etiquetador.config_criterio("v22bcrit")
    assert cfg["model"] == "m-x" and cfg["formato"] == "replace"
    assert cfg["user"].replace("{txt}", "hola") == 'Devuelve {"a": 1}. MENSAJE: hola'


def test_etiquetador_para_si_el_csv_de_salida_no_tiene_llm_criterio(etiquetador, tmp_path, monkeypatch):
    import csv as _csv
    f = tmp_path / "salida.csv"
    monkeypatch.setattr(etiquetador, "OUTPUT_FILE", str(f))

    # CSV sin message_uuid → cabecera ilegible → debe lanzar RuntimeError
    f.write_text("columna_rara,otra\nu1,x\n", encoding="utf-8")
    with pytest.raises(RuntimeError):
        etiquetador.verificar_cabecera_salida()

    # CSV con message_uuid pero sin llm_criterio → migra (no lanza),
    # añade llm_criterio y crea backup
    f.write_text("message_uuid,clasificacion_principal\nu1,ODIO\n", encoding="utf-8")
    etiquetador.verificar_cabecera_salida()  # no lanza
    with open(f, newline="", encoding="utf-8") as fh:
        cols = next(_csv.reader(fh))
    assert "llm_criterio" in cols, "la migración debe añadir llm_criterio a la cabecera"
    bak = f.with_suffix(f.suffix + ".pre_llm_criterio.bak")
    assert bak.exists(), "la migración debe crear un fichero de backup"

    # CSV ya correcto → no-op, no lanza
    etiquetador.verificar_cabecera_salida()
