#!/usr/bin/env python3
"""
pipeline_unificado/test_recalcular_terminos_youtube.py

Tests para recalcular_terminos_youtube.py.

Ejecutar:
  python3 -m pytest pipeline_unificado/test_recalcular_terminos_youtube.py -v
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# Ajustar path para importar módulos del pipeline
_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.filter_and_anonymize_unified import (
    detect_hate_terms,
    detect_target,
    normalize_text,
)
from pipeline_unificado.recalcular_terminos_youtube import (
    MIN_MATCHES,
    SKIP_PRUNING,
    fetch_old_messages,
    recalculate_all,
)


# ---------------------------------------------------------------------------
# Test 1: stopwords y tokens cortos no producen matches
# ---------------------------------------------------------------------------

def test_no_hate_stopwords():
    """
    Texto con 'lo', 'una', 'ya' y ningún término de odio real
    da match_count=0 e is_candidate=False.

    'lo', 'una', 'ya' son filtrados por _is_informative (len ≤ 3)
    en load_terms antes de llegar al diccionario, por lo que nunca
    aparecen en single_terms.
    """
    # Términos de odio reales (ninguno es stopword ni tiene len ≤ 3)
    single_terms = {"odio", "imbecil", "inutil"}
    text = "lo veo una vez ya que son sin tan gobierno vox estado fuera mal"
    text_norm = normalize_text(text)

    hate_cand, matched_str, match_cnt, _ = detect_hate_terms(
        text_norm, single_terms, [], {}
    )

    assert match_cnt == 0, f"No debería haber matches; got matched_str='{matched_str}'"
    assert hate_cand == 0
    assert matched_str == ""

    directed, _ = detect_target(text, text_norm)
    is_cand = (match_cnt >= MIN_MATCHES) and (directed == 1)
    assert not is_cand, "No debe ser candidato con solo stopwords/palabras genéricas"


# ---------------------------------------------------------------------------
# Test 2: término de odio + pronombre de segunda persona → candidato
# ---------------------------------------------------------------------------

def test_candidate_with_pronoun():
    """
    Texto con un término de odio y pronombre de segunda persona
    produce is_candidate=True.
    """
    single_terms = {"imbecil"}
    text = "imbecil tu no sirves para nada"
    text_norm = normalize_text(text)

    hate_cand, matched_str, match_cnt, _ = detect_hate_terms(
        text_norm, single_terms, [], {}
    )
    directed, target_type = detect_target(text, text_norm)

    is_cand = (match_cnt >= MIN_MATCHES) and (directed == 1)

    assert hate_cand == 1
    assert match_cnt >= 1
    assert directed == 1, f"Debe detectar target (tu → person); got target_type={target_type}"
    assert is_cand, "Debe ser candidato con término de odio + pronombre personal"


# ---------------------------------------------------------------------------
# Test 3: formato de salida usa '|' con términos ordenados
# ---------------------------------------------------------------------------

def test_matched_terms_format_pipe_sorted():
    """
    Con múltiples términos de odio, matched_str usa '|' como separador
    y los términos aparecen en orden alfabético.
    """
    single_terms = {"zurdos", "imbecil", "maldito"}
    text = "maldito zurdos imbecil"
    text_norm = normalize_text(text)

    _, matched_str, match_cnt, _ = detect_hate_terms(
        text_norm, single_terms, [], {}
    )

    assert match_cnt == 3
    assert "|" in matched_str, f"Separador debe ser '|'; got '{matched_str}'"
    assert ";" not in matched_str, "No debe haber ';' en matched_terms"
    parts = matched_str.split("|")
    assert parts == sorted(parts), f"Términos deben estar ordenados; got '{matched_str}'"
    assert set(parts) == {"imbecil", "maldito", "zurdos"}


# ---------------------------------------------------------------------------
# Test 4: dry-run no ejecuta ninguna escritura (mock de conexión)
# ---------------------------------------------------------------------------

def test_dry_run_no_db_writes():
    """
    En modo dry-run (sin --apply), apply_changes nunca se llama
    y no se ejecuta ningún UPDATE en la conexión mockeada.
    """
    # Construir un DataFrame mínimo con columnas que necesita recalculate_all
    df_input = pd.DataFrame([
        {
            "message_uuid": "uuid-test-001",
            "content_original": "lo una ya sin mas",
            "created_at": "2026-01-15 10:00:00",
            "old_matched_terms": "lo;una;ya",
            "old_match_count": 3,
            "old_has_hate": True,
            "old_is_candidate": True,
            "old_candidate_reason": "",
        }
    ])

    single_terms = {"odio", "imbecil"}
    df_result = recalculate_all(df_input, single_terms, [], {})

    # Verificar que el nuevo matched_terms no tiene ';'
    assert ";" not in (df_result["new_matched_terms"].iloc[0] or "")
    # Verificar que is_candidate=False porque no hay términos reales
    assert not df_result["new_is_candidate"].iloc[0]

    # Simular que apply_changes NO se llama al invocar main en dry-run
    with patch(
        "pipeline_unificado.recalcular_terminos_youtube.apply_changes"
    ) as mock_apply:
        # Mockear get_conn para que no conecte a la BD real
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)   # para los COUNT de cross-checks
        mock_cursor.fetchall.return_value = [
            (
                "uuid-test-001",
                "lo una ya sin mas",
                "2026-01-15 10:00:00",
                "lo;una;ya",
                3,
                True,
                True,
                "",
            )
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        with patch(
            "pipeline_unificado.recalcular_terminos_youtube.get_conn",
            return_value=mock_conn,
        ):
            with patch(
                "pipeline_unificado.recalcular_terminos_youtube.OUT_CSV",
                new=Path("/tmp/recalculo_test.csv"),
            ):
                with patch(
                    "pipeline_unificado.recalcular_terminos_youtube.OUT_MD",
                    new=Path("/tmp/recalculo_test.md"),
                ):
                    from pipeline_unificado.recalcular_terminos_youtube import main
                    main(["--cutoff", "2026-07-01"])

        # apply_changes nunca debe haberse llamado
        mock_apply.assert_not_called()


# ---------------------------------------------------------------------------
# Test 5: fetch_old_messages selecciona también filas antiguas con 1 solo
# término (sin ';')
# ---------------------------------------------------------------------------

def test_fetch_old_messages_includes_single_term_rows():
    """
    Antes del fix, la consulta solo tomaba filas con ';', NULL o '', por lo
    que filas antiguas con un único término (p.ej. matched_terms='lo') se
    quedaban fuera. La consulta corregida selecciona TODAS las filas
    pre-corte, y sólo filtra por ';' en las post-corte.
    """
    import datetime as _dt

    captured_sql = {}

    class _Cur:
        def execute(self, sql, params=None):
            captured_sql["sql"] = sql
            captured_sql["params"] = params

        def fetchall(self):
            # Fila antigua con un único término 'lo' (sin ';'): debería
            # devolverse por la query nueva.
            return [
                (
                    "uuid-old-1",
                    "lo digo yo",
                    "2026-01-10 09:00:00",
                    "lo",         # old_matched_terms sin ';'
                    1,
                    True,
                    True,
                    "",
                ),
            ]

        def close(self):
            pass

    class _Conn:
        def cursor(self):
            return _Cur()

    df = fetch_old_messages(_Conn(), _dt.date(2026, 7, 15))

    # Verifica que la SQL ya no filtra sólo por ';'/NULL/''
    sql = captured_sql["sql"]
    assert "created_at < %s" in sql
    assert "created_at >= %s AND matched_terms LIKE '%%;%%'" in sql
    # La fila con un único término (sin ';') debe aparecer en el DataFrame
    assert len(df) == 1
    assert df.iloc[0]["old_matched_terms"] == "lo"


# ---------------------------------------------------------------------------
# Test 6: con skip_pruning=True, un término presente en todos los mensajes
# del lote sigue detectándose (la poda lo habría eliminado por df_max_ratio)
# ---------------------------------------------------------------------------

def test_skip_pruning_keeps_ubiquitous_term():
    """
    Un término que aparece en todos los mensajes del lote superaría el umbral
    df_max_ratio=0.03 y sería eliminado por prune_terms_by_corpus_frequency.
    Con skip_pruning=True el término se detecta en cada mensaje.
    """
    single_terms = {"imbecil"}

    rows = []
    for i in range(10):
        rows.append({
            "message_uuid": f"uuid-{i:03d}",
            "content_original": "tu eres un imbecil total",
            "created_at": f"2026-02-{i+1:02d} 10:00:00",
            "old_matched_terms": "imbecil",
            "old_match_count": 1,
            "old_has_hate": True,
            "old_is_candidate": True,
            "old_candidate_reason": "",
        })
    df_input = pd.DataFrame(rows)

    # Con skip_pruning=True (default histórico) el término aparece siempre.
    df_sp = recalculate_all(df_input, single_terms, [], {}, skip_pruning=True)
    assert (df_sp["new_matched_terms"] == "imbecil").all()
    assert (df_sp["new_match_count"] == 1).all()

    # Con skip_pruning=False la poda per-day lo elimina (df_ratio = 1.0 > 0.03).
    df_wp = recalculate_all(df_input, single_terms, [], {}, skip_pruning=False)
    assert (df_wp["new_matched_terms"] == "").all()
    assert (df_wp["new_match_count"] == 0).all()


# ---------------------------------------------------------------------------
# Test 7: constante SKIP_PRUNING refleja el default histórico (True)
# ---------------------------------------------------------------------------

def test_skip_pruning_default_is_true():
    assert SKIP_PRUNING is True
