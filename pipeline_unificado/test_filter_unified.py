#!/usr/bin/env python3
"""pipeline_unificado/test_filter_unified.py

Tests for filter_and_anonymize_unified.py
"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent))

from filter_and_anonymize_unified import (
    detect_hate_terms,
    detect_target,
    filter_and_anonymize,
    normalize_text,
    prune_terms_by_corpus_frequency,
    sha256_hex,
)

# ── Shared fixtures ──────────────────────────────────────────────────────────

SALT = "test_salt_reto_2024"

CANONICAL_COLS = [
    "message_uuid", "platform", "content_original", "published_at",
    "source_media", "author_username", "author_id", "url", "language",
    "parent_id", "likes", "retweet_count", "reply_count", "quote_count",
    "ingestion_batch_id", "ingestion_date", "platform_native_id",
]


def make_canonical_df(platform: str, texts) -> pd.DataFrame:
    """Build a minimal canonical DataFrame for testing."""
    if isinstance(texts, str):
        texts = [texts]
    rows = [
        {
            "message_uuid": f"uuid-test-{i}",
            "platform": platform,
            "content_original": text,
            "published_at": "2024-01-01T00:00:00Z",
            "source_media": "test_media",
            "author_username": f"user_{i}",
            "author_id": f"id_{i}",
            "url": f"https://example.com/{i}",
            "language": "es",
            "parent_id": "",
            "likes": 0,
            "retweet_count": 0,
            "reply_count": 0,
            "quote_count": 0,
            "ingestion_batch_id": "batch_test",
            "ingestion_date": "2024-01-01T00:00:00Z",
            "platform_native_id": f"native_{i}",
        }
        for i, text in enumerate(texts)
    ]
    return pd.DataFrame(rows, columns=CANONICAL_COLS)


# ── Test 1: Happy path X ──────────────────────────────────────────────────────

def test_happy_path_x():
    """Message with hate term + group target passes filter (X platform)."""
    # "imbecil" in the hate dictionary; "inmigrantes" is in GROUP_TERMS
    single_terms = {"imbecil"}
    df = make_canonical_df("x", "imbecil vuelve con los inmigrantes a tu pais")

    df_full, df_cands = filter_and_anonymize(df, single_terms, [], {}, salt=SALT)

    assert len(df_cands) == 1, "Should produce exactly one candidate"
    assert bool(df_full["is_candidate"].iloc[0]), "Row must be marked as candidate"
    assert df_full["hate_candidate"].iloc[0] == 1
    assert df_full["directed_target"].iloc[0] == 1
    assert df_full["target_type"].iloc[0] == "group"


# ── Test 2: Happy path YouTube ───────────────────────────────────────────────

def test_happy_path_youtube():
    """Message with typed hate term + group target passes filter (YouTube platform)."""
    # "odio" in dictionary with hate_type="racial"; "moros" in GROUP_TERMS
    single_terms = {"odio"}
    term_to_type = {"odio": "racial"}
    df = make_canonical_df("youtube", "puro odio hacia los moros")

    df_full, df_cands = filter_and_anonymize(
        df, single_terms, [], term_to_type, salt=SALT
    )

    assert len(df_cands) == 1, "Should produce exactly one candidate"
    assert df_cands["hate_types"].iloc[0] == "racial"
    assert df_cands["target_type"].iloc[0] == "group"
    assert df_cands["platform"].iloc[0] == "youtube"


# ── Test 3: Hate term present but no target → not a candidate ────────────────

def test_no_target_not_candidate():
    """Hate term present but no identifiable target: must NOT pass the filter."""
    single_terms = {"basura"}
    # "inmigracion" is NOT in GROUP_TERMS (only "inmigrante"/"inmigrantes" are)
    # No second-person pronouns, no @mention
    df = make_canonical_df("x", "la politica de inmigracion es una basura absoluta")

    df_full, df_cands = filter_and_anonymize(df, single_terms, [], {}, salt=SALT)

    assert len(df_cands) == 0, "Must not produce a candidate without a detectable target"
    assert df_full["hate_candidate"].iloc[0] == 1, "Hate term must still be recorded"
    assert df_full["directed_target"].iloc[0] == 0, "No target must be found"
    assert not bool(df_full["is_candidate"].iloc[0])


# ── Test 4: Anonymisation uses salt ──────────────────────────────────────────

def test_anonymisation_uses_salt():
    """SHA-256 with salt must differ from SHA-256 without salt and be applied to output."""
    value = "user_reto_test_123"
    salt = "secreto_reto_test"

    with_salt = sha256_hex(value, salt)
    no_salt = sha256_hex(value, "")

    assert with_salt != no_salt, "Salted hash must differ from unsalted hash"
    assert len(with_salt) == 64, "SHA-256 hex digest must be 64 characters"
    # deterministic
    assert sha256_hex(value, salt) == sha256_hex(value, salt)

    # verify the DataFrame output drops raw columns and applies the salt
    df = make_canonical_df("x", "texto de prueba inmigrante")
    df_full, _ = filter_and_anonymize(df, {"basura"}, [], {}, salt=salt)

    assert "author_username" not in df_full.columns, "Raw username must be removed"
    assert "author_id" not in df_full.columns, "Raw author_id must be removed"
    assert "author_username_anon" in df_full.columns
    assert "author_id_anon" in df_full.columns
    # author_username for row 0 is "user_0" (set by make_canonical_df)
    expected = sha256_hex("user_0", salt)
    assert df_full["author_username_anon"].iloc[0] == expected


# ── Test 5: Corpus pruning removes overly frequent terms ─────────────────────

def test_corpus_pruning_removes_frequent_term():
    """Term in >50% of documents must be pruned; rare term (10%) must be kept."""
    # "comun" in 90/100 docs = 90%  → above df_max_ratio=0.5 → pruned
    # "odio"  in 10/100 docs = 10%  → below df_max_ratio=0.5 → kept
    texts = ["comun texto"] * 90 + ["odio inmigrante"] * 10
    single_terms = {"comun", "odio"}

    pruned = prune_terms_by_corpus_frequency(
        texts,
        single_terms,
        df_max_ratio=0.5,
        df_min_count=3,
    )

    assert "comun" not in pruned, "High-frequency term (90%) must be removed"
    assert "odio" in pruned, "Low-frequency term (10%) must be retained"


# ── Test 6: Multi-word phrase detection ──────────────────────────────────────

def test_multi_word_phrase_detected():
    """A two-word phrase from the dictionary is detected via substring match."""
    single_terms: set = set()
    multi_terms = ["odio racial"]
    term_to_type = {"odio racial": "racism"}

    text_norm = normalize_text("esto es un caso de odio racial flagrante en la comunidad")
    hate_cand, matched_str, count, hate_types = detect_hate_terms(
        text_norm, single_terms, multi_terms, term_to_type
    )

    assert hate_cand == 1
    assert "odio racial" in matched_str
    assert count == 1
    assert hate_types == "racism"
