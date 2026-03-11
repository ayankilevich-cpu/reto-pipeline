"""LLM pre-etiquetado local (sin Google Sheets todavía).

- Lee textos (hardcodeados por ahora)
- Llama a OpenAI
- Devuelve un JSON con columnas *_pred para ODIO y POSITIVO
- Guarda un .jsonl de salida para auditoría

Requisitos:
  pip install openai python-dotenv

Uso:
  # Opción 1: Desde el directorio del script
  cd /path/to/Medios/ML/etiquetado_llm
  ../../../../X_Mensajes/venv/bin/python3 etiquetar_local.py
  
  # Opción 2: Usando el script helper
  ./ejecutar.sh

Notas:
- NO escribe en columnas manuales. Esto es solo predicción.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List

try:
    from dotenv import load_dotenv
    from openai import OpenAI
except ImportError as e:
    print(
        f"❌ Error: Faltan dependencias requeridas.\n"
        f"   Instala con: pip install python-dotenv openai\n"
        f"   O activa el entorno virtual: source ../../X_Mensajes/venv/bin/activate\n"
        f"   Error: {e}",
        file=sys.stderr,
    )
    sys.exit(1)


# -------------------------
# Config
# -------------------------
MODEL = os.getenv("OPENAI_MODEL", "gpt-5.2")  # podés cambiarlo en .env
OUTPUT_DIR = os.getenv("LLM_OUTPUT_DIR", "./outputs")


# -------------------------
# Esquema de salida (las columnas que agregaste al Sheet)
# -------------------------
@dataclass
class Pred:
    odio_pred: str  # "SI" | "NO"
    odio_score: float  # 0..1
    labels_pred: str  # "label1;label2" o ""
    resumen_motivo: str  # 1 frase

    positivo_pred: str  # "SI" | "NO"
    positivo_labels: str  # "label1;label2" o ""
    positivo_resumen: str  # 1 frase

    def to_dict(self) -> Dict[str, Any]:
        return {
            "odio_pred": self.odio_pred,
            "odio_score": self.odio_score,
            "labels_pred": self.labels_pred,
            "resumen_motivo": self.resumen_motivo,
            "positivo_pred": self.positivo_pred,
            "positivo_labels": self.positivo_labels,
            "positivo_resumen": self.positivo_resumen,
        }


SYSTEM_PROMPT = (
    "Sos un asistente de etiquetado para un proyecto de análisis de discurso. "
    "Tu tarea es pre-etiquetar si un mensaje contiene ODIO (o deshumanización/ataque por grupo/atributo) "
    "y también detectar mensajes POSITIVOS (apoyo/convivencia/antiodio). "
    "Devolvé SOLO JSON válido, sin texto extra. "
    "Reglas:"
    "\n- odio_pred: 'SI' solo si hay ataque/hostilidad hacia un grupo protegido o colectivo (nacionalidad, etnia, religión, género, orientación, migrantes, etc.) "
    "o deshumanización o incitación. Si es insulto general a políticos/servicios sin grupo -> normalmente NO."
    "\n- odio_score: 0..1 (confianza)."
    "\n- labels_pred: lista corta separada por ';' (ej: 'inmigracion;religion;deshumanizacion;amenaza'). Si NO hay odio, puede ser ''."
    "\n- resumen_motivo: 1 frase breve explicando el porqué."
    "\n- positivo_pred: 'SI' si el mensaje promueve respeto, convivencia, apoyo a un colectivo atacado, o condena el odio."
    "\n- positivo_labels: ej 'apoyo;convivencia;antirracismo' (separado por ';')"
    "\n- positivo_resumen: 1 frase."
)


def _safe_float(x: Any) -> float:
    try:
        f = float(x)
    except Exception:
        return 0.0
    # clamp
    if f < 0:
        return 0.0
    if f > 1:
        return 1.0
    return f


def _normalize_yesno(x: Any) -> str:
    s = str(x).strip().upper()
    if s in {"SI", "SÍ", "YES", "Y", "1", "TRUE"}:
        return "SI"
    return "NO"


def _normalize_labels(x: Any) -> str:
    if x is None:
        return ""
    s = str(x).strip()
    if not s:
        return ""
    # normalizar separadores comunes a ';'
    s = s.replace(",", ";").replace("|", ";")
    parts = [p.strip() for p in s.split(";") if p.strip()]
    return ";".join(parts)


def parse_pred(raw_text: str) -> Pred:
    """Parsea el JSON devuelto por el modelo."""
    obj = json.loads(raw_text)

    return Pred(
        odio_pred=_normalize_yesno(obj.get("odio_pred")),
        odio_score=_safe_float(obj.get("odio_score")),
        labels_pred=_normalize_labels(obj.get("labels_pred")),
        resumen_motivo=str(obj.get("resumen_motivo", "")).strip(),
        positivo_pred=_normalize_yesno(obj.get("positivo_pred")),
        positivo_labels=_normalize_labels(obj.get("positivo_labels")),
        positivo_resumen=str(obj.get("positivo_resumen", "")).strip(),
    )


def predict_one(client: OpenAI, text: str) -> Pred:
    prompt = (
        "Devolvé un JSON con estas claves EXACTAS: "
        "odio_pred, odio_score, labels_pred, resumen_motivo, "
        "positivo_pred, positivo_labels, positivo_resumen.\n\n"
        f"TEXTO: {text}"
    )

    resp = client.responses.create(
        model=MODEL,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )

    out = (resp.output_text or "").strip()

    # fallback: intentar recortar a bloque JSON si viene con basura
    if not out.startswith("{"):
        start = out.find("{")
        end = out.rfind("}")
        if start != -1 and end != -1 and end > start:
            out = out[start : end + 1]

    return parse_pred(out)


def main() -> None:
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "Falta OPENAI_API_KEY. Creá un .env junto a este proyecto con OPENAI_API_KEY=..."
        )

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(
        OUTPUT_DIR, f"predicciones_local_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    )

    client = OpenAI()

    # 10 textos de prueba (reemplazalos por lo que quieras)
    samples: List[str] = [
        "Hay que echar a estos inmigrantes, son una plaga.",
        "Qué vergüenza de tren, siempre averiado.",
        "Los gays son enfermos, habría que prohibirlos.",
        "No al racismo. Todos merecemos respeto.",
        "Este político es un inútil, no sirve para nada.",
        "Esa gente no es humana, son ratas.",
        "Fuerza a la comunidad afectada, estamos con ustedes.",
        "Odio a todo el mundo, qué asco de día.",
        "Hay que quemar a todos los X (mensaje incitador).",
        "Qué buena iniciativa para integrar a migrantes en el barrio.",
    ]

    print(f"Modelo: {MODEL}")
    print(f"Salida: {out_path}\n")

    with open(out_path, "w", encoding="utf-8") as f:
        for i, text in enumerate(samples, start=1):
            pred = predict_one(client, text)
            row = {"id": i, "text": text, **pred.to_dict()}
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

            print(f"[{i}] odio={pred.odio_pred} ({pred.odio_score:.2f}) | pos={pred.positivo_pred} | labels={pred.labels_pred}")

    print("\nListo. Si ves que el formato está OK, el próximo paso es leer/escribir Google Sheets.")


if __name__ == "__main__":
    main()
