import pandas as pd
import joblib
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report, confusion_matrix, recall_score

print("Script iniciado")

# =========================
# 1. Cargar datos
# =========================
CSV_PATH = "reto_train_x_v1.csv"  # CSV en la misma carpeta que este script

df = pd.read_csv(CSV_PATH)

df = df.dropna(subset=["message_text"])
df["message_text"] = df["message_text"].astype(str)

X = df["message_text"]
y = df["y_odio"]

print("\nDistribución de clases:")
print(y.value_counts())

# =========================
# 2. Split train / test
# =========================
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# =========================
# 3. Vectorización TF-IDF
# =========================
vectorizer = TfidfVectorizer(
    lowercase=True,
    strip_accents="unicode",
    stop_words=None,  # sklearn no soporta 'spanish', usar None o lista personalizada
    ngram_range=(1, 2),
    min_df=2,
    max_df=0.9,
)

X_train_vec = vectorizer.fit_transform(X_train)
X_test_vec = vectorizer.transform(X_test)

# =========================
# 4. Modelo
# =========================
model = LogisticRegression(
    max_iter=1000,
    class_weight="balanced",
)

model.fit(X_train_vec, y_train)

# =========================
# 5. Evaluación (threshold 0.5)
# =========================
y_pred = model.predict(X_test_vec)

print("\n=== Reporte (threshold 0.5) ===")
print(classification_report(y_test, y_pred))
print("Matriz de confusión:")
print(confusion_matrix(y_test, y_pred))

# =========================
# 6. Ajuste de threshold (priorizar recall ODIO)
# =========================
y_probs = model.predict_proba(X_test_vec)[:, 1]

threshold = 0.30
# Predicción con umbral ajustado
y_pred_thresh = (y_probs >= threshold).astype(int)

print(f"\n=== Reporte (threshold {threshold}) ===")
print(classification_report(y_test, y_pred_thresh))
print("Recall ODIO:", recall_score(y_test, y_pred_thresh))

# =========================
# 7. Guardar artefactos
# =========================
joblib.dump(model, "modelo_odio_logreg.pkl")
joblib.dump(vectorizer, "vectorizer_tfidf.pkl")

config = {
    "threshold": threshold,
    "model": "LogisticRegression",
    "vectorizer": "TF-IDF (1,2)",
    "dataset": "reto_train_x_v1.csv",
}

joblib.dump(config, "config_modelo.pkl")

print("\nModelo y vectorizador guardados correctamente.")
