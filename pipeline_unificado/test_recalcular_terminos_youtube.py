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
