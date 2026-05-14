#!/usr/bin/env python3
"""pipeline_unificado/test_etiquetar_llm.py

Tests for etiquetar_llm_unified.py
All OpenAI calls are mocked — no real API calls.
"""

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, Mock, call, patch

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from etiquetar_llm_unified import LLMLabeler, _md5


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_VALID_RESULT = {
    "clasificacion_principal": "ODIO",
    "categoria_odio_pred": "odio_etnico_cultural_religioso",
    "intensidad_pred": "2",
    "resumen_motivo": "Insulto hacia inmigrantes",
}


def _make_mock_client(json_response: str | dict | None = None) -> Mock:
    """Return a mock OpenAI client whose chat.completions.create returns json_response."""
    if json_response is None:
        json_response = _VALID_RESULT
    if isinstance(json_response, dict):
        json_response = json.dumps(json_response)

    mock_message = Mock()
    mock_message.content = json_response

    mock_choice = Mock()
    mock_choice.message = mock_message

    mock_resp = Mock()
    mock_resp.choices = [mock_choice]

    mock_client = Mock()
    mock_client.chat.completions.create.return_value = mock_resp
    return mock_client


def _make_df(texts, uuids=None) -> pd.DataFrame:
    records = []
    for i, text in enumerate(texts):
        r = {"content_original": text}
        if uuids is not None:
            r["message_uuid"] = uuids[i]
        records.append(r)
    return pd.DataFrame(records)


# ---------------------------------------------------------------------------
# Test 1: label() returns DataFrame with the 4 LLM output columns
# ---------------------------------------------------------------------------

def test_label_returns_four_new_columns():
    """label() must append exactly the 4 LLM output columns to the input DataFrame."""
    client = _make_mock_client(_VALID_RESULT)
    labeler = LLMLabeler(platform="x", openai_client=client, cache_path=None)

    df = _make_df(["odio a los inmigrantes"], uuids=["uuid-001"])
    df_out = labeler.label(df)

    for col in LLMLabeler.LLM_OUTPUT_COLS:
        assert col in df_out.columns, f"Missing column: {col}"

    assert df_out["clasificacion_principal"].iloc[0] == "ODIO"
    assert df_out["categoria_odio_pred"].iloc[0] == "odio_etnico_cultural_religioso"
    assert df_out["intensidad_pred"].iloc[0] == "2"
    assert df_out["resumen_motivo"].iloc[0] == "Insulto hacia inmigrantes"
    assert len(df_out) == len(df), "Row count must not change"


# ---------------------------------------------------------------------------
# Test 2: Cache hit — matching MD5 → no LLM call
# ---------------------------------------------------------------------------

def test_cache_hit_skips_llm_call():
    """If the cache entry has the right MD5, the LLM must NOT be called."""
    client = _make_mock_client()
    labeler = LLMLabeler(platform="x", openai_client=client, cache_path=None)

    txt = "texto ya etiquetado antes"
    uuid = "uuid-cached-001"

    # Pre-populate the in-memory cache
    labeler._cache[uuid] = {
        "text_hash": _md5(txt),
        "clasificacion_principal": "NO_ODIO",
        "categoria_odio_pred": "",
        "intensidad_pred": "",
        "resumen_motivo": "Texto neutro (desde caché)",
    }

    df = _make_df([txt], uuids=[uuid])
    df_out = labeler.label(df)

    client.chat.completions.create.assert_not_called()
    assert df_out["clasificacion_principal"].iloc[0] == "NO_ODIO"
    assert df_out["resumen_motivo"].iloc[0] == "Texto neutro (desde caché)"


# ---------------------------------------------------------------------------
# Test 3: Cache miss by changed text — MD5 mismatch → LLM called
# ---------------------------------------------------------------------------

def test_cache_miss_on_text_change_calls_llm():
    """If the cached MD5 does not match the current text, the LLM must be called."""
    client = _make_mock_client(_VALID_RESULT)
    labeler = LLMLabeler(platform="x", openai_client=client, cache_path=None)

    uuid = "uuid-changed-001"
    old_txt = "texto original"
    new_txt = "texto diferente modificado"  # MD5 will differ

    # Cache stores the hash of the OLD text
    labeler._cache[uuid] = {
        "text_hash": _md5(old_txt),
        "clasificacion_principal": "NO_ODIO",
        "categoria_odio_pred": "",
        "intensidad_pred": "",
        "resumen_motivo": "Vieja etiqueta",
    }

    df = _make_df([new_txt], uuids=[uuid])
    df_out = labeler.label(df)

    # LLM must have been called because the text changed
    client.chat.completions.create.assert_called_once()
    # Result reflects fresh LLM response, not the stale cache
    assert df_out["clasificacion_principal"].iloc[0] == "ODIO"
    # Cache is updated with new text's MD5
    assert labeler._cache[uuid]["text_hash"] == _md5(new_txt)


# ---------------------------------------------------------------------------
# Test 4: Coherence rule — NO_ODIO → categoria and intensidad must be empty
# ---------------------------------------------------------------------------

def test_coherence_rule_no_odio_clears_fields():
    """When clasificacion_principal is NO_ODIO, category and intensity must be cleared."""
    llm_payload = {
        "clasificacion_principal": "NO_ODIO",
        "categoria_odio_pred": "odio_ideologico_politico",  # LLM erroneously filled this
        "intensidad_pred": "1",                              # and this
        "resumen_motivo": "Crítica política sin odio",
    }
    client = _make_mock_client(llm_payload)
    labeler = LLMLabeler(platform="youtube", openai_client=client, cache_path=None)

    df = _make_df(["este gobierno es corrupto"], uuids=["uuid-coherence-001"])
    df_out = labeler.label(df)

    assert df_out["clasificacion_principal"].iloc[0] == "NO_ODIO"
    assert df_out["categoria_odio_pred"].iloc[0] == "", "Category must be cleared for NO_ODIO"
    assert df_out["intensidad_pred"].iloc[0] == "", "Intensity must be cleared for NO_ODIO"


# ---------------------------------------------------------------------------
# Test 5: Retry — invalid JSON exhausts retries → degrades to DUDOSO
# ---------------------------------------------------------------------------

def test_retry_exhaustion_degrades_to_dudoso():
    """When all retries return invalid JSON, result must degrade to DUDOSO."""
    # Return non-JSON on every attempt
    bad_message = Mock()
    bad_message.content = "Lo siento, no puedo clasificar este texto ahora mismo."

    bad_choice = Mock()
    bad_choice.message = bad_message

    bad_resp = Mock()
    bad_resp.choices = [bad_choice]

    client = Mock()
    client.chat.completions.create.return_value = bad_resp

    max_retries = 2
    labeler = LLMLabeler(
        platform="x",
        openai_client=client,
        cache_path=None,
        max_retries=max_retries,
    )

    df = _make_df(["texto con respuesta inválida"], uuids=["uuid-retry-001"])
    df_out = labeler.label(df)

    # Must have called the API exactly max_retries times
    assert client.chat.completions.create.call_count == max_retries
    # Must degrade gracefully
    assert df_out["clasificacion_principal"].iloc[0] == "DUDOSO"
    assert "parseo" in df_out["resumen_motivo"].iloc[0].lower()
    assert df_out["categoria_odio_pred"].iloc[0] == ""
    assert df_out["intensidad_pred"].iloc[0] == ""


# ---------------------------------------------------------------------------
# Test 6: Few-shot — examples appear in the prompt
# ---------------------------------------------------------------------------

def test_few_shot_examples_appear_in_prompt(tmp_path):
    """When few_shot_path is set, the prompt must include the few-shot block."""
    few_shot_data = [
        {
            "comentario": "Esos inmigrantes nos quitan el trabajo",
            "clasificacion_correcta": "ODIO",
            "categoria_correcta": "odio_etnico_cultural_religioso",
            "intensidad_correcta": "2",
            "error_del_llm": "NO_ODIO",
        },
        {
            "comentario": "Me parece bien que haya diversidad",
            "clasificacion_correcta": "NO_ODIO",
            "categoria_correcta": "",
            "intensidad_correcta": "",
            "error_del_llm": "NO_ODIO",
        },
    ]
    few_shot_file = tmp_path / "few_shot.json"
    few_shot_file.write_text(json.dumps(few_shot_data, ensure_ascii=False), encoding="utf-8")

    client = _make_mock_client(_VALID_RESULT)
    labeler = LLMLabeler(
        platform="x",
        openai_client=client,
        few_shot_path=str(few_shot_file),
        cache_path=None,
    )

    df = _make_df(["texto de prueba para few-shot"], uuids=["uuid-fewshot-001"])
    labeler.label(df)

    # Inspect the user message that was sent to the API
    assert client.chat.completions.create.called
    call_kwargs = client.chat.completions.create.call_args
    messages_sent = call_kwargs.kwargs.get("messages") or call_kwargs.args[0]
    user_content = next(m["content"] for m in messages_sent if m["role"] == "user")

    assert "EJEMPLOS DE REFERENCIA" in user_content, "Few-shot header missing from prompt"
    assert "Esos inmigrantes nos quitan el trabajo" in user_content, "Example 1 missing"
    assert "Me parece bien que haya diversidad" in user_content, "Example 2 missing"
    assert "clasificar como NO_ODIO sería incorrecto" in user_content, "Error note missing"
