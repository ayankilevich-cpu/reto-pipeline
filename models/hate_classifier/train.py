"""
Entrenamiento y evaluación de clasificador de odio no-LLM (TF-IDF + Regresión Logística).

Reemplaza los CSV estáticos del análisis del 31-ago-2026 por consulta directa
a Postgres (Neon) — cada corrida usa el gold dataset más actualizado.

Uso:
    export DATABASE_URL="postgresql://usuario:pass@host/reto_db?sslmode=require&channel_binding=require"
    python train.py --platforms x,youtube
    python train.py --platforms x --output-dir results/

Requiere: ver requirements.txt
"""
import argparse
import os
import re
import sys
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
from scipy.sparse import hstack, csr_matrix
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import GridSearchCV, StratifiedKFold, cross_val_predict
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (roc_auc_score, accuracy_score, precision_recall_fscore_support,
                              confusion_matrix, f1_score, balanced_accuracy_score)

QUERY = """
SELECT
  g.message_uuid,
  m.platform,
  m.content_original,
  m.relevante_score,
  m.relevante_llm,
  m.match_count,
  m.strong_phrase,
  m.has_hate_terms_match,
  m.matched_terms,
  g.y_odio_bin,
  g.y_odio_final,
  g.split
FROM processed.gold_dataset g
JOIN processed.mensajes m ON m.message_uuid = g.message_uuid
WHERE g.y_odio_bin IS NOT NULL;
"""

SPANISH_STOP = set("""
de la que el en y a los del se las por un para con no una su al lo como más pero sus le ya o
este sí porque esta entre cuando muy sin sobre también me hasta hay donde quien desde todo nos
durante todos uno les ni contra otros ese eso ante ellos e esto mí antes algunos qué unos yo
otro otras otra él tanto esa estos mucho quienes nada muchos cual poco ella estar estas algunas
algo nosotros mi mis tú te ti tu tus ellas nosotras vosotros vosotras os mío mía míos mías
tuyo tuya tuyos tuyas suyo suya suyos suyas nuestro nuestra nuestros nuestras vuestro vuestra
vuestros vuestras esos esas era eres soy eramos es son fue fueron ser siendo sido tener tiene
tengo va van vas voy ir es está están estoy estamos había han he has hemos habrá será serán
""".split())


def fetch_gold(database_url: str) -> pd.DataFrame:
    with psycopg2.connect(database_url) as conn:
        df = pd.read_sql(QUERY, conn)
    return df


def clean_text(t):
    t = str(t).lower()
    t = re.sub(r'http\S+|www\.\S+', ' URLTOKEN ', t)
    t = re.sub(r'@\w+', ' MENTIONTOKEN ', t)
    t = re.sub(r'#(\w+)', r' \1 ', t)
    t = re.sub(r'[^a-záéíóúüñ0-9\s]', ' ', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def to_bool_num(s):
    return s.astype(str).str.lower().map({'true': 1, 't': 1, '1': 1, 'false': 0, 'f': 0, '0': 0}).fillna(0)


def build_features(df, vectorizer=None, scaler=None, fit=False):
    texts = df['content_original'].apply(clean_text)
    if fit:
        vectorizer = TfidfVectorizer(max_features=6000, ngram_range=(1, 2), min_df=3, max_df=0.85,
                                      sublinear_tf=True, stop_words=list(SPANISH_STOP))
        X_text = vectorizer.fit_transform(texts)
    else:
        X_text = vectorizer.transform(texts)

    eng = pd.DataFrame({
        'relevante_score': pd.to_numeric(df['relevante_score'], errors='coerce').fillna(-1),
        'match_count': pd.to_numeric(df['match_count'], errors='coerce').fillna(0),
        'strong_phrase': to_bool_num(df['strong_phrase']),
        'has_hate_terms_match': to_bool_num(df['has_hate_terms_match']),
        'text_len': texts.str.len(),
        'n_tokens': texts.str.split().apply(len),
    })
    if fit:
        scaler = StandardScaler()
        X_eng = scaler.fit_transform(eng.values)
    else:
        X_eng = scaler.transform(eng.values)

    X_full = hstack([X_text, csr_matrix(X_eng)]).tocsr()
    return X_full, vectorizer, scaler


def evaluate(y_true, y_pred, y_proba, label, verbose=True):
    acc = accuracy_score(y_true, y_pred)
    bal_acc = balanced_accuracy_score(y_true, y_pred)
    auc = roc_auc_score(y_true, y_proba)
    prec, rec, f1, _ = precision_recall_fscore_support(y_true, y_pred, average='binary', zero_division=0)
    if verbose:
        print(f"[{label}] n={len(y_true)}  Accuracy={acc:.3f}  BalancedAcc={bal_acc:.3f}  AUC={auc:.3f}  "
              f"Precision={prec:.3f}  Recall={rec:.3f}  F1={f1:.3f}")
        print("  Matriz confusión [[TN,FP],[FN,TP]]:\n", confusion_matrix(y_true, y_pred))
    return dict(n=len(y_true), accuracy=acc, balanced_accuracy=bal_acc, auc=auc,
                precision=prec, recall=rec, f1=f1)


def run_platform(df, platform, random_state=42):
    print("\n" + "=" * 70)
    print(f"PLATAFORMA: {platform}")
    print("=" * 70)
    sub = df[df['platform'] == platform].copy()
    train = sub[sub['split'] == 'TRAIN'].reset_index(drop=True)
    test = sub[sub['split'] == 'TEST'].reset_index(drop=True)
    print(f"TRAIN n={len(train)}  TEST n={len(test)}")
    if len(train) < 50 or len(test) < 20:
        print(f"AVISO: muy pocos datos para {platform}, salteando.")
        return None

    X_train, vec, scaler = build_features(train, fit=True)
    y_train = train['y_odio_bin'].astype(int).values
    X_test, _, _ = build_features(test, vectorizer=vec, scaler=scaler, fit=False)
    y_test = test['y_odio_bin'].astype(int).values

    skf = StratifiedKFold(n_splits=5, shuffle=True, random_state=random_state)
    param_grid = {'C': [0.03, 0.1, 0.3, 1, 3, 10]}
    gs = GridSearchCV(LogisticRegression(max_iter=2000, class_weight='balanced', solver='liblinear'),
                       param_grid, scoring='roc_auc', cv=skf, n_jobs=-1)
    gs.fit(X_train, y_train)
    best_C = gs.best_params_['C']
    print(f"Mejor C (CV AUC={gs.best_score_:.3f}): {best_C}")

    model = LogisticRegression(max_iter=2000, class_weight='balanced', solver='liblinear', C=best_C)
    model.fit(X_train, y_train)

    oof_proba = cross_val_predict(model, X_train, y_train, cv=skf, method='predict_proba')[:, 1]
    best_thr, best_f1 = 0.5, -1
    for thr in np.arange(0.1, 0.9, 0.02):
        f1 = f1_score(y_train, (oof_proba >= thr).astype(int))
        if f1 > best_f1:
            best_f1, best_thr = f1, thr
    print(f"Umbral calibrado en TRAIN (CV out-of-fold, maximiza F1): {best_thr:.2f} (F1 oof={best_f1:.3f})")

    proba_test = model.predict_proba(X_test)[:, 1]
    res_05 = evaluate(y_test, (proba_test >= 0.5).astype(int), proba_test, 'TEST (umbral 0.5)')
    res_cal = evaluate(y_test, (proba_test >= best_thr).astype(int), proba_test, f'TEST (umbral calibrado {best_thr:.2f})')

    return {
        'run_date': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'platform': platform, 'best_C': best_C, 'cv_auc': round(gs.best_score_, 4),
        'best_thr': round(best_thr, 2),
        'test_auc': round(res_05['auc'], 4),
        'test_acc_05': round(res_05['accuracy'], 4),
        'test_balanced_acc_05': round(res_05['balanced_accuracy'], 4),
        'test_acc_cal': round(res_cal['accuracy'], 4),
        'test_balanced_acc_cal': round(res_cal['balanced_accuracy'], 4),
        'test_f1_cal': round(res_cal['f1'], 4),
        'test_prec_cal': round(res_cal['precision'], 4),
        'test_rec_cal': round(res_cal['recall'], 4),
        'n_train': len(train), 'n_test': len(test),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--platforms', default='x,youtube',
                         help='Plataformas a entrenar, separadas por coma (default: x,youtube)')
    parser.add_argument('--output-dir', default=os.path.join(os.path.dirname(__file__), 'results'),
                         help='Carpeta donde guardar el log de resultados')
    parser.add_argument('--log-file', default='training_log.csv',
                         help='Nombre del CSV donde se acumulan los resultados de cada corrida')
    parser.add_argument('--random-state', type=int, default=42)
    args = parser.parse_args()

    database_url = os.environ.get('DATABASE_URL')
    if not database_url:
        sys.exit("ERROR: falta la variable de entorno DATABASE_URL. Ver README.md / .env.example.")

    os.makedirs(args.output_dir, exist_ok=True)
    log_path = os.path.join(args.output_dir, args.log_file)

    print("Descargando gold dataset desde Postgres...")
    df = fetch_gold(database_url)
    print(f"Gold descargado: {len(df)} mensajes.")

    results = []
    for plat in args.platforms.split(','):
        plat = plat.strip()
        r = run_platform(df, plat, random_state=args.random_state)
        if r:
            results.append(r)

    if not results:
        print("Sin resultados para guardar.")
        return

    resdf = pd.DataFrame(results)
    print("\n\n" + "#" * 70)
    print("RESUMEN COMPARATIVO DE ESTA CORRIDA")
    print("#" * 70)
    print(resdf.to_string(index=False))

    # Append al log histórico (no sobreescribe corridas anteriores)
    write_header = not os.path.exists(log_path)
    resdf.to_csv(log_path, mode='a', header=write_header, index=False)
    print(f"\nResultados agregados a {log_path}")


if __name__ == '__main__':
    main()
