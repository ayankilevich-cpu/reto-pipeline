#!/usr/bin/env python3
"""
pipeline_unificado/test_poda_terminos.py

Tests para el cambio de poda del diccionario y los nuevos GROUP_TERMS.

Ejecutar:
  python3 -m pytest pipeline_unificado/test_poda_terminos.py -v
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.filter_and_anonymize_unified import (
    GROUP_TERMS,
    detect_hate_terms,
    detect_target,
    load_stopwords,
    load_terms,
    normalize_text,
    prune_terms_by_corpus_frequency,
)


MIN_MATCHES = 1


def _is_candidate(text: str, single_terms, multi_terms=None, term_to_type=None) -> bool:
    multi_terms = multi_terms or []
    term_to_type = term_to_type or {}
    text_norm = normalize_text(text)
    _, _, match_cnt, _ = detect_hate_terms(text_norm, single_terms, multi_terms, term_to_type)
    directed, _ = detect_target(text, text_norm)
    return (match_cnt >= MIN_MATCHES) and (directed == 1)


# ---------------------------------------------------------------------------
# Test 1: sin poda, un término al 10 % del lote sigue detectándose
# ---------------------------------------------------------------------------

def test_skip_pruning_keeps_frequent_terms():
    """
    Con pruning activo (df_max_ratio=0.03), un término presente en el 10 %
    del lote se elimina. Con --skip-pruning (sin aplicar la poda) el término
    se conserva y detect_hate_terms lo encuentra.
    """
    single_terms = {"basura"}
    # 100 docs; "basura" aparece en 10 → df = 10 %, bien por encima del 3 %
    texts = (
        ["basura es horrible"] * 10
        + ["texto cualquiera sin problemas aqui mismo"] * 90
    )

    # --- con poda activa (comportamiento actual del pipeline) ---
    kept_with_pruning = prune_terms_by_corpus_frequency(
        texts, single_terms, df_max_ratio=0.03, df_min_count=3
    )
    assert "basura" not in kept_with_pruning, (
        "Con la poda activa, 'basura' se elimina porque supera el 3 %."
    )

    # --- sin poda (--skip-pruning) ---
    kept_without_pruning = single_terms  # el workflow salta la poda
    assert "basura" in kept_without_pruning

    # Detección sobre un mensaje concreto del lote
    text = "basura es horrible"
    text_norm = normalize_text(text)
    _, matched, match_cnt, _ = detect_hate_terms(
        text_norm, kept_without_pruning, [], {}
    )
    assert match_cnt == 1
    assert matched == "basura"


# ---------------------------------------------------------------------------
# Test 2: expresiones compuestas con destinatarios conocidos son candidatos
# ---------------------------------------------------------------------------

def test_vieja_de_mierda_tu_is_candidate():
    """'vieja de mierda' + 'tu' → candidato (pronombre de 2ª persona)."""
    single_terms = {"mierda"}
    text = "tu vieja de mierda"
    assert _is_candidate(text, single_terms)


def test_basura_roja_facha_is_candidate():
    """'basura vaga roja es facha' → candidato (destinatario de grupo 'facha')."""
    single_terms = {"basura"}
    text = "basura vaga roja es facha"
    # 'facha' está en GROUP_TERMS
    assert _is_candidate(text, single_terms)


# ---------------------------------------------------------------------------
# Test 3: GROUP_TERMS incluye el singular 'comunista'
# ---------------------------------------------------------------------------

def test_comunista_singular_is_group_target():
    """'PEDRO SÁNCHEZ COMUNISTA HIJO DE PUTA' → candidato por grupo 'comunista'."""
    # Comprobación directa sobre GROUP_TERMS (regresión del Paso 1.4)
    assert "comunista" in GROUP_TERMS
    assert "musulman" in GROUP_TERMS and "musulmana" in GROUP_TERMS
    assert "mena" in GROUP_TERMS and "menas" in GROUP_TERMS
    assert "rojo" in GROUP_TERMS and "rojos" in GROUP_TERMS
    assert "sociata" in GROUP_TERMS and "sociatas" in GROUP_TERMS
    assert "perroflauta" in GROUP_TERMS and "perroflautas" in GROUP_TERMS

    single_terms = {"puta"}
    text = "PEDRO SÁNCHEZ COMUNISTA HIJO DE PUTA"
    text_norm = normalize_text(text)

    _, _, match_cnt, _ = detect_hate_terms(text_norm, single_terms, [], {})
    directed, target_type = detect_target(text, text_norm)

    assert match_cnt >= 1, "Debe detectar al menos 'puta'"
    assert directed == 1, "Debe detectar destinatario"
    assert target_type == "group", (
        f"'comunista' debe clasificar como 'group'; got '{target_type}'"
    )
    assert _is_candidate(text, single_terms)


# ---------------------------------------------------------------------------
# Test 4: palabras del archivo de stopwords no se detectan
# ---------------------------------------------------------------------------

def test_stopwords_file_terms_are_excluded():
    """
    Cualquier palabra incluida en el stopwords file se filtra en load_terms
    y nunca aparece en single_terms, aunque esté en el CSV de términos.
    """
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        # Fichero de términos (incluye 'basura' y 'generico')
        terms_file = tmp_path / "terms.csv"
        terms_file.write_text(
            "term\nbasura\ngenerico\ninsulto\n", encoding="utf-8"
        )
        # Fichero de stopwords que excluye 'basura' y 'generico'
        sw_file = tmp_path / "stopwords.txt"
        sw_file.write_text(
            "# comentario\nbasura\ngenerico\n", encoding="utf-8"
        )

        stopwords = load_stopwords(sw_file)
        assert "basura" in stopwords
        assert "generico" in stopwords

        single_terms, _, _ = load_terms([terms_file], stopwords=stopwords)

        assert "basura" not in single_terms, (
            "'basura' debería quedar excluido al estar en el stopwords file."
        )
        assert "generico" not in single_terms
        assert "insulto" in single_terms, (
            "'insulto' no está en stopwords y debe mantenerse."
        )

        # Detección: con 'basura' excluido, el texto no genera match
        text_norm = normalize_text("esto es basura y generico")
        _, matched, cnt, _ = detect_hate_terms(text_norm, single_terms, [], {})
        assert cnt == 0
        assert matched == ""
