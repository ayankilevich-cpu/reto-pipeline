"""Tests del adapter canónico (Fase 1.1)."""

from __future__ import annotations

import pandas as pd
import pytest

from pipeline_unificado.adapter import (
    CANONICAL_SCHEMA,
    adapt_x,
    adapt_youtube,
    validate_canonical,
    yt_comment_to_uuid,
)


def _x_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "message_uuid": ["u1", "u2", "u3"],
            "platform": ["x", "X", "x"],
            "tweet_id": ["t1", "t2", "t3"],
            "created_at": ["2024-01-15T10:00:00Z", "2024-02-01 11:00:00", pd.NaT],
            "content_original": ["hola", "mundo", "test"],
            "author_username": ["a1", pd.NA, "a3"],
            "author_id": ["1", "2", "3"],
            "source_media": ["M1", "M2", "M3"],
            "batch_id": ["b1", "b2", "b3"],
            "scrape_date": ["2024-01-20", "2024-01-21", "2024-01-22"],
            "language": ["es", "", "en"],
            "url": ["http://x/1", "", "http://x/3"],
            "retweet_count": [1, pd.NA, 3],
            "reply_count": [0, 1, 2],
            "like_count": [10, float("nan"), float("nan")],
            "quote_count": [0, 0, 1],
            "extra_ignored": [1, 2, 3],
        }
    )


def _yt_sample_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "medio": ["MedioA", "MedioB"],
            "provincia": ["p1", "p2"],
            "channel_url": ["c1", "c2"],
            "channel_id": ["ch1", "ch2"],
            "video_id": ["abc", "vid2"],
            "video_title": ["t1", "t2"],
            "video_published_at": ["2024-01-01", "2024-01-02"],
            "comment_id": ["xyz", "c2id"],
            "comment_text": ["comentario", "otro"],
            "comment_published_at": ["2024-06-01T12:00:00+00:00", pd.NaT],
            "like_count": [5, pd.NA],
            "author_display_name": ["User1", "User2"],
            "author_channel_id": ["ac1", "ac2"],
            "hate_terms_matched": ["x", "y"],
        }
    )


def test_happy_path_x():
    out = adapt_x(_x_sample_df())
    assert list(out.columns) == CANONICAL_SCHEMA
    assert not out.isna().any().any()
    assert out["platform"].tolist() == ["x", "x", "x"]
    assert out["likes"].tolist() == [10, 0, 0]
    assert out.loc[1, "author_username"] == ""
    assert out.loc[1, "likes"] == 0  # like_count era NaN → 0


def test_happy_path_youtube():
    out = adapt_youtube(_yt_sample_df())
    assert list(out.columns) == CANONICAL_SCHEMA
    assert not out.isna().any().any()
    assert (out["platform"] == "youtube").all()
    assert out["retweet_count"].sum() == 0
    assert out["ingestion_batch_id"].eq("").all()
    assert out.loc[0, "parent_id"] == "abc"
    assert out.loc[0, "platform_native_id"] == "xyz"


def test_yt_uuid_deterministic():
    a = yt_comment_to_uuid("abc123")
    b = yt_comment_to_uuid("abc123")
    assert a == b
    assert len(a) == 36


def test_missing_content_original_x_raises():
    df = _x_sample_df().drop(columns=["content_original"])
    with pytest.raises(ValueError, match="content_original"):
        adapt_x(df)


def test_nan_author_username_becomes_empty_string():
    out = adapt_x(_x_sample_df())
    assert out.loc[1, "author_username"] == ""


def test_validate_canonical_ok_and_fail():
    out = adapt_x(_x_sample_df())
    validate_canonical(out)  # no raise

    bad = out.drop(columns=["url"])
    with pytest.raises(ValueError, match="Columnas"):
        validate_canonical(bad)


def test_youtube_url_format():
    df = pd.DataFrame(
        [
            {
                "medio": "M",
                "provincia": "p",
                "channel_url": "u",
                "channel_id": "ci",
                "video_id": "abc",
                "video_title": "t",
                "video_published_at": "2020-01-01",
                "comment_id": "xyz",
                "comment_text": "txt",
                "comment_published_at": "2020-01-02",
                "like_count": 0,
                "author_display_name": "U",
                "author_channel_id": "A",
                "hate_terms_matched": "",
            }
        ]
    )
    out = adapt_youtube(df)
    assert out.loc[0, "url"] == "https://www.youtube.com/watch?v=abc&lc=xyz"


def test_platform_lowercase_x_input_uppercase():
    df = _x_sample_df()
    df.loc[0, "platform"] = "X"
    out = adapt_x(df)
    assert out.loc[0, "platform"] == "x"
