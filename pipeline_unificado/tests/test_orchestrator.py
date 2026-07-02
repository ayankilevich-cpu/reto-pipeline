"""
test_orchestrator.py — Smoke tests del orquestador con datos sintéticos.

No requiere BD, modelos reales ni credenciales.
Verifica que el pipeline completo (dry-run y ejecución real con mocks) no lanza
excepciones y genera los outputs esperados.

Ejecutar:
  pytest pipeline_unificado/tests/test_orchestrator.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

# Raíz del repo en el path
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline_unificado.orchestrator import main


# ---------------------------------------------------------------------------
# Helpers para generar CSVs sintéticos
# ---------------------------------------------------------------------------

def _make_x_raw_csv(path: Path, n: int = 5) -> Path:
    """CSV sintético con columnas mínimas que adapt_x() requiere."""
    df = pd.DataFrame({
        "message_uuid":    [f"uuid-x-{i}" for i in range(n)],
        "content_original": [f"Texto de prueba número {i} con odio gitano negro" for i in range(n)],
        "created_at":      ["2026-07-01T10:00:00Z"] * n,
        "source_media":    ["test_medio"] * n,
        "author_username": [f"user_{i}" for i in range(n)],
        "author_id":       [f"id_{i}" for i in range(n)],
        "url":             [f"https://x.com/i/status/{i}" for i in range(n)],
        "language":        ["es"] * n,
        "like_count":      [0] * n,
        "retweet_count":   [0] * n,
        "reply_count":     [0] * n,
        "quote_count":     [0] * n,
        "batch_id":        ["batch_001"] * n,
        "scrape_date":     ["2026-07-01T00:00:00Z"] * n,
        "tweet_id":        [f"native_{i}" for i in range(n)],
    })
    df.to_csv(path, index=False)
    return path


def _make_youtube_raw_csv(path: Path, n: int = 5) -> Path:
    """CSV sintético con columnas mínimas que adapt_youtube() requiere."""
    df = pd.DataFrame({
        "comment_id":           [f"yt-comment-{i}" for i in range(n)],
        "comment_text":         [f"Comentario de prueba {i} inmigrantes" for i in range(n)],
        "video_id":             [f"video_{i}" for i in range(n)],
        "medio":                ["test_canal"] * n,
        "comment_published_at": ["2026-07-01T10:00:00Z"] * n,
        "author_display_name":  [f"autor_{i}" for i in range(n)],
        "author_channel_id":    [f"channel_{i}" for i in range(n)],
        "like_count":           [0] * n,
    })
    df.to_csv(path, index=False)
    return path


def _make_terms_csv(path: Path) -> Path:
    """CSV mínimo de términos de odio para load_terms()."""
    pd.DataFrame({"Lemas": ["odio", "gitano", "negro", "inmigrante", "negro sudaca"]}).to_csv(
        path, index=False
    )
    return path


# ---------------------------------------------------------------------------
# Tests: dry-run (sin modelos, sin BD, sin LLM)
# ---------------------------------------------------------------------------

def test_orchestrator_x_dry_run(tmp_path):
    """dry-run para X no debe lanzar excepciones."""
    raw_csv = _make_x_raw_csv(tmp_path / "raw_x.csv")

    with patch.object(sys, "argv", [
        "orchestrator",
        "--platform", "x",
        "--input-file", str(raw_csv),
        "--out-dir", str(tmp_path),
        "--dry-run",
    ]):
        main()  # no debe lanzar


def test_orchestrator_youtube_dry_run(tmp_path):
    """dry-run para YouTube no debe lanzar excepciones."""
    raw_csv = _make_youtube_raw_csv(tmp_path / "raw_yt.csv")

    with patch.object(sys, "argv", [
        "orchestrator",
        "--platform", "youtube",
        "--input-file", str(raw_csv),
        "--out-dir", str(tmp_path),
        "--dry-run",
    ]):
        main()


def test_orchestrator_x_dry_run_with_max_rows(tmp_path):
    """dry-run con --max-rows recorta filas y no falla."""
    raw_csv = _make_x_raw_csv(tmp_path / "raw_x.csv", n=20)

    with patch.object(sys, "argv", [
        "orchestrator",
        "--platform", "x",
        "--input-file", str(raw_csv),
        "--out-dir", str(tmp_path),
        "--dry-run",
        "--max-rows", "5",
    ]):
        main()


# ---------------------------------------------------------------------------
# Tests: pipeline completo con scorer mock (sin modelos reales)
# ---------------------------------------------------------------------------

def _make_mock_scorer(n_rows: int) -> MagicMock:
    """Scorer mock que devuelve un DataFrame con columnas prefilter_* reales."""
    def _score(df: pd.DataFrame) -> pd.DataFrame:
        df_out = df.copy()
        df_out["prefilter_score"]    = [0.7] * len(df_out)
        df_out["prefilter_pass"]     = [True] * len(df_out)
        df_out["prefilter_priority"] = ["alta"] * len(df_out)
        df_out["prefilter_method"]   = ["mock_tfidf"] * len(df_out)
        df_out["prefilter_reason"]   = ["mock"] * len(df_out)
        df_out["prefilter_date"]     = ["2026-07-01T00:00:00+00:00"] * len(df_out)
        return df_out

    mock = MagicMock()
    mock.score.side_effect = _score
    return mock


def test_orchestrator_x_full_with_mock_scorer(tmp_path):
    """Pipeline X completo (adapter → filter → scorer) con scorer mockeado.

    Verifica que los 4 CSVs de output se generan con el número correcto de filas.
    """
    raw_csv    = _make_x_raw_csv(tmp_path / "raw_x.csv", n=10)
    terms_csv  = _make_terms_csv(tmp_path / "terms.csv")
    mock_scorer = _make_mock_scorer(10)

    with (
        patch("pipeline_unificado.orchestrator.get_scorer", return_value=mock_scorer),
        patch.object(sys, "argv", [
            "orchestrator",
            "--platform", "x",
            "--input-file", str(raw_csv),
            "--out-dir", str(tmp_path),
            "--terms-files", str(terms_csv),
            "--salt", "test_salt",
            "--skip-pruning",
            "--vectorizer", str(tmp_path / "fake_vec.joblib"),  # existencia checkeada post-mock
            "--model-path", str(tmp_path / "fake_model.joblib"),
        ]),
    ):
        # Saltear la comprobación de existencia de los modelos falsos
        with patch("pipeline_unificado.orchestrator.Path.exists", return_value=True):
            main()

    assert (tmp_path / "x_canonical.csv").exists(),   "falta x_canonical.csv"
    assert (tmp_path / "x_full_anon.csv").exists(),   "falta x_full_anon.csv"
    assert (tmp_path / "x_candidates.csv").exists(),  "falta x_candidates.csv"
    assert (tmp_path / "x_scored.csv").exists(),      "falta x_scored.csv"

    df_canonical = pd.read_csv(tmp_path / "x_canonical.csv")
    assert len(df_canonical) == 10, f"canonical tiene {len(df_canonical)} filas, esperadas 10"

    df_full = pd.read_csv(tmp_path / "x_full_anon.csv")
    assert len(df_full) == 10, f"full_anon tiene {len(df_full)} filas, esperadas 10"
    # author_username y author_id no deben estar (anonimizados)
    assert "author_username" not in df_full.columns
    assert "author_id"       not in df_full.columns


def test_orchestrator_youtube_full_pipeline(tmp_path):
    """Pipeline YouTube completo (adapter → filter, sin scorer).

    Verifica que se generan los 3 CSVs de output (sin x_scored.csv).
    """
    raw_csv   = _make_youtube_raw_csv(tmp_path / "raw_yt.csv", n=8)
    terms_csv = _make_terms_csv(tmp_path / "terms.csv")

    with patch.object(sys, "argv", [
        "orchestrator",
        "--platform", "youtube",
        "--input-file", str(raw_csv),
        "--out-dir", str(tmp_path),
        "--terms-files", str(terms_csv),
        "--salt", "test_salt",
        "--skip-pruning",
    ]):
        main()

    assert (tmp_path / "youtube_canonical.csv").exists(), "falta youtube_canonical.csv"
    assert (tmp_path / "youtube_full_anon.csv").exists(), "falta youtube_full_anon.csv"
    assert (tmp_path / "youtube_candidates.csv").exists(), "falta youtube_candidates.csv"
    assert not (tmp_path / "youtube_scored.csv").exists(), "youtube no debe tener scored.csv"

    df_canonical = pd.read_csv(tmp_path / "youtube_canonical.csv")
    assert len(df_canonical) == 8


def test_orchestrator_pipeline_aborts_on_missing_input(tmp_path):
    """El pipeline debe fallar con sys.exit(1) si el input no existe."""
    with pytest.raises(SystemExit) as exc_info:
        with patch.object(sys, "argv", [
            "orchestrator",
            "--platform", "x",
            "--input-file", str(tmp_path / "no_existe.csv"),
            "--out-dir", str(tmp_path),
        ]):
            main()
    assert exc_info.value.code == 1
