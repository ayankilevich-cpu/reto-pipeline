"""
Entrenamiento baseline sobre dataset mixto (X + YouTube) generado por build_train_mixed_v1.py.

Arreglo: sklearn NO soporta stop_words="spanish". Solo acepta "english", None o una lista.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, recall_score
from sklearn.model_selection import train_test_split
import joblib


BASE_DIR = Path(__file__).resolve().parent
MIX_PATH_DEFAULT = BASE_DIR / "reto_train_mixed_v1.csv"


def main() -> None:
    print("Script train_odio_mixed_v1 iniciado\n")

    mix_path = MIX_PATH_DEFAULT
    if not mix_path.exists():
        raise FileNotFoundError(
            f"No se encontró el dataset mixto en {mix_path}. Primero ejecuta build_train_mixed_v1.py"
        )

    df = pd.read_csv(mix_path, encoding="utf-8")
    required = {"message_text", "y_odio", "source"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas en {mix_path.name}: {sorted(missing)}")

    df = df.dropna(subset=["message_text", "y_odio"]).copy()
    df["message_text"] = df["message_text"].astype(str)
    df["y_odio"] = df["y_odio"].astype(int)

    print("Distribución general:")
    print(df["y_odio"].value_counts())
    print("\nDistribución por source:")
    print(df["source"].value_counts())
    print()

    X = df["message_text"]
    y = df["y_odio"]
    source = df["source"].astype(str)

    X_train, X_test, y_train, y_test, source_train, source_test = train_test_split(
        X,
        y,
        source,
        test_size=0.25,
        random_state=42,
        stratify=y,
    )

    # sklearn NO soporta 'spanish' como stop_words (solo 'english', None o lista)
    vectorizer = TfidfVectorizer(
        lowercase=True,
        strip_accents="unicode",
        stop_words=None,
        ngram_range=(1, 2),
        min_df=2,
        max_df=0.9,
    )

    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)

    model = LogisticRegression(max_iter=1000, class_weight="balanced")
    model.fit(X_train_vec, y_train)

    # =========================
    # Evaluación GLOBAL
    # =========================
    y_pred = model.predict(X_test_vec)
    print("=== Evaluación GLOBAL (X + YT) ===")
    print("Confusion matrix:")
    print(confusion_matrix(y_test, y_pred))
    print("\nClassification report:")
    print(classification_report(y_test, y_pred, digits=3))

    # =========================
    # Evaluación SOLO YOUTUBE
    # =========================
    # Ojo: para indexar matrices sparse (X_test_vec) necesitamos un boolean mask tipo numpy,
    # no una pandas.Series (si no, SciPy falla con ".nonzero()").
    yt_mask = (source_test == "YT")
    yt_mask_np = yt_mask.to_numpy() if hasattr(yt_mask, "to_numpy") else yt_mask
    n_yt = int(yt_mask_np.sum())
    print("\n=== Evaluación SOLO YOUTUBE ===")
    print(f"Casos YT en test: {n_yt}")
    if n_yt == 0:
        print("No hay casos YT en el set de test (raro). Prueba cambiando random_state.")
    else:
        X_test_yt = X_test_vec[yt_mask_np]
        # y_test puede ser Series; el boolean mask debe estar alineado y en formato numpy también
        y_test_yt = y_test[yt_mask_np]
        y_pred_yt = model.predict(X_test_yt)
        print("Confusion matrix (YT):")
        print(confusion_matrix(y_test_yt, y_pred_yt))
        print("\nClassification report (YT):")
        print(classification_report(y_test_yt, y_pred_yt, digits=3))

        # =========================
        # Sweep de thresholds (YT)
        # =========================
        y_probs = model.predict_proba(X_test_vec)[:, 1]
        y_probs_yt = y_probs[yt_mask_np]
        print("\nRecall ODIO en YT por threshold:")
        for th in [0.30, 0.40, 0.50, 0.60]:
            y_pred_th_yt = (y_probs_yt >= th).astype(int)
            rec = recall_score(y_test_yt, y_pred_th_yt)
            print(f"  th={th:.2f} -> recall_odio={rec:.3f}")

    # =========================
    # Guardar artefactos
    # =========================
    joblib.dump(model, BASE_DIR / "modelo_odio_mixed_v1.pkl")
    joblib.dump(vectorizer, BASE_DIR / "vectorizer_tfidf_mixed_v1.pkl")
    config = {
        "dataset": str(mix_path),
        "model": "LogisticRegression",
        "vectorizer": "TF-IDF (1,2)",
        "stop_words": None,
        "test_size": 0.25,
        "random_state": 42,
        "note": "Entrenado con X+YT; métricas reportadas global y YT; thresholds sugeridos para YT"
    }
    joblib.dump(config, BASE_DIR / "config_modelo_mixed_v1.pkl")
    print("\nOK: guardados modelo_odio_mixed_v1.pkl, vectorizer_tfidf_mixed_v1.pkl, config_modelo_mixed_v1.pkl")


if __name__ == "__main__":
    main()
