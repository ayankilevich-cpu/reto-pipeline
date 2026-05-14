#!/usr/bin/env python3
"""pipeline_unificado/test_prefilter_scorer.py

Tests for prefilter_scorer.py
All external dependencies (joblib, OpenAI) are mocked — no real models or API calls.
"""

import json
import re
import sys
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from prefilter_scorer import (
    DEFAULT_THRESHOLDS,
    LLMScorer,
    MLScorer,
    get_scorer,
)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _make_df(texts, uuids=None):
    """Minimal DataFrame with content_original (and optional message_uuid)."""
    records = []
    for i, text in enumerate(texts):
        r = {"content_original": text}
        if uuids is not None:
            r["message_uuid"] = uuids[i]
        records.append(r)
    return pd.DataFrame(records)


def _ml_mocks(proba: float):
    """Return (mock_vectorizer, mock_model) producing predict_proba → [[1-p, p]]."""
    mock_vec = Mock()
    mock_vec.transform.return_value = Mock()  # sparse matrix — shape irrelevant
    mock_model = Mock()
    mock_model.predict_proba.return_value = np.array([[1.0 - proba, proba]])
    return mock_vec, mock_model


# ---------------------------------------------------------------------------
# Test 1: MLScorer assigns priority "alta" when proba >= threshold_high (0.55)
# ---------------------------------------------------------------------------

@patch("prefilter_scorer.joblib.load")
def test_ml_priority_alta(mock_load):
    """proba=0.90 → priority 'alta' (threshold_high=0.55 by default)."""
    mock_vec, mock_model = _ml_mocks(0.90)
    mock_load.side_effect = [mock_vec, mock_model]

    scorer = MLScorer("v.joblib", "m.joblib")
    result = scorer.score(_make_df(["texto de prueba"]))

    assert result["prefilter_priority"].iloc[0] == "alta"
    assert bool(result["prefilter_pass"].iloc[0]) is True
    assert abs(result["prefilter_score"].iloc[0] - 0.90) < 1e-9


# ---------------------------------------------------------------------------
# Test 2: MLScorer assigns priority "baja" when proba < threshold_low (0.30)
# ---------------------------------------------------------------------------

@patch("prefilter_scorer.joblib.load")
def test_ml_priority_baja(mock_load):
    """proba=0.10 → priority 'baja' and prefilter_pass=False."""
    mock_vec, mock_model = _ml_mocks(0.10)
    mock_load.side_effect = [mock_vec, mock_model]

    scorer = MLScorer("v.joblib", "m.joblib")
    result = scorer.score(_make_df(["texto inofensivo"]))

    assert result["prefilter_priority"].iloc[0] == "baja"
    assert bool(result["prefilter_pass"].iloc[0]) is False
    assert abs(result["prefilter_score"].iloc[0] - 0.10) < 1e-9


# ---------------------------------------------------------------------------
# Test 3: MLScorer prefilter_reason has the expected format
# ---------------------------------------------------------------------------

@patch("prefilter_scorer.joblib.load")
def test_ml_prefilter_reason_format(mock_load):
    """prefilter_reason must match 'proba=X.XX, threshold=Y.YY'."""
    mock_vec, mock_model = _ml_mocks(0.75)
    mock_load.side_effect = [mock_vec, mock_model]

    scorer = MLScorer("v.joblib", "m.joblib")
    result = scorer.score(_make_df(["texto de ejemplo"]))

    reason = result["prefilter_reason"].iloc[0]
    pattern = r"^proba=\d+\.\d{2}, threshold=\d+\.\d{2}$"
    assert re.match(pattern, reason), f"Reason format wrong: {reason!r}"

    # Values must be consistent with the score and threshold
    assert "proba=0.75" in reason
    assert f"threshold={DEFAULT_THRESHOLDS['x']['low']:.2f}" in reason


# ---------------------------------------------------------------------------
# Test 4: LLMScorer with cache_path=None never calls _load_cache or _save_cache
# ---------------------------------------------------------------------------

@patch("prefilter_scorer.OpenAI")
def test_llm_no_cache_write(mock_openai_cls):
    """cache_path=None must produce no file I/O at all."""
    # Wire up a minimal mock OpenAI response
    mock_client = Mock()
    mock_resp = Mock()
    mock_resp.output_text = json.dumps(
        {"relevante": "SI", "score": 0.8, "motivo": "insulto hacia inmigrantes"}
    )
    mock_client.responses.create.return_value = mock_resp
    mock_openai_cls.return_value = mock_client

    scorer = LLMScorer(cache_path=None)
    assert scorer.cache_path is None, "cache_path should be None"

    df = _make_df(
        ["comentario con insulto hacia inmigrantes"],
        uuids=["test-uuid-001"],
    )

    with patch.object(scorer, "_load_cache") as mock_load, \
         patch.object(scorer, "_save_cache") as mock_save:
        result = scorer.score(df)
        mock_load.assert_not_called()
        mock_save.assert_not_called()

    # Output must still contain the unified columns
    assert "prefilter_score" in result.columns
    assert abs(result["prefilter_score"].iloc[0] - 0.8) < 1e-9
    assert result["prefilter_reason"].iloc[0] == "insulto hacia inmigrantes"


# ---------------------------------------------------------------------------
# Test 5: get_scorer returns the correct type for each platform
# ---------------------------------------------------------------------------

@patch("prefilter_scorer.joblib.load")
def test_get_scorer_returns_correct_types(mock_load):
    """get_scorer('x') → MLScorer; get_scorer('youtube') → LLMScorer."""
    mock_load.return_value = Mock()  # both joblib.load calls (vec + model) return Mock

    x_scorer  = get_scorer("x", vectorizer_path="v.joblib", model_path="m.joblib")
    yt_scorer = get_scorer("youtube")

    assert isinstance(x_scorer, MLScorer)
    assert isinstance(yt_scorer, LLMScorer)

    # Thresholds must reflect platform defaults
    assert x_scorer.threshold_high == DEFAULT_THRESHOLDS["x"]["high"]
    assert x_scorer.threshold_low  == DEFAULT_THRESHOLDS["x"]["low"]
    assert yt_scorer.threshold_high == DEFAULT_THRESHOLDS["youtube"]["high"]
    assert yt_scorer.threshold_low  == DEFAULT_THRESHOLDS["youtube"]["low"]

    # Unknown platform raises
    with pytest.raises(ValueError, match="Unknown platform"):
        get_scorer("tiktok")
