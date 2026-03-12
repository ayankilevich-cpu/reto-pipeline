"""consolidar_csv.py

Consolida exportaciones CSV de Apify (X/Twitter) descargadas localmente y normaliza
las columnas a un esquema fijo para etiquetado manual.

Entrada:
- Un directorio local con múltiples CSV (por ejemplo, data/raw/)
  (idealmente sincronizados desde Drive con sync_drive_csvs.py)

Salida:
- Un CSV maestro acumulativo (por ejemplo, data/master/reto_x_master.csv)

Características:
- Modo INCREMENTAL: solo lee CSVs nuevos o modificados desde la última ejecución
  (usa .consolidar_state.json para rastrear archivos ya procesados).
- Selecciona / renombra columnas al esquema definido por el proyecto ReTo.
- Genera uuid estable por fila si no viene (uuid5 sobre platform+tweet_id).
- Dedup por (platform, tweet_id) quedándose con el registro más reciente (scrape_date/created_at).
- No pisa histórico: si el master existe, agrega solo nuevos.

Esquema final (orden exacto):
  message_uuid
  platform
  tweet_id
  created_at
  content_original
  author_username
  author_id
  source_media
  batch_id
  scrape_date
  language
  url
  retweet_count
  reply_count
  like_count
  quote_count

Uso:
  python consolidar_csv.py --in-dir data/raw --out-file data/master/reto_x_master.csv
  python consolidar_csv.py --force   # reprocesa todos los archivos ignorando el estado

Notas:
- Los CSV de Apify pueden cambiar nombres de columnas. Ajusta el diccionario COLUMN_ALIASES
  si detectas variantes.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid as uuidlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd

CONSOLIDAR_STATE_FILE = ".consolidar_state.json"


SCHEMA: List[str] = [
    "message_uuid",
    "platform",
    "tweet_id",
    "created_at",
    "content_original",
    "author_username",
    "author_id",
    "source_media",
    "batch_id",
    "scrape_date",
    "language",
    "url",
    "retweet_count",
    "reply_count",
    "like_count",
    "quote_count",
]

# Mapea nombres alternativos a los nombres del esquema.
# Ajustar según los encabezados reales del CSV de Apify (si cambian).
COLUMN_ALIASES: Dict[str, str] = {
    # uuid/mensajes
    "uuid": "message_uuid",
    "mensajes": "message_uuid",
    "message_uuid": "message_uuid",
    
    # ids
    "id": "tweet_id",
    "tweetId": "tweet_id",
    "tweet_id": "tweet_id",

    # fechas
    "createdAt": "created_at",
    "created_at": "created_at",

    # texto
    "text": "content_original",
    "full_text": "content_original",
    "content_original": "content_original",

    # autor
    "author_username": "author_username",
    "username": "author_username",
    "userName": "author_username",

    "author_id": "author_id",
    "userId": "author_id",

    # meta
    "language": "language",
    "lang": "language",

    "url": "url",
    "twitterUrl": "url",

    # métricas
    "retweetCount": "retweet_count",
    "retweet_count": "retweet_count",

    "replyCount": "reply_count",
    "reply_count": "reply_count",

    "likeCount": "like_count",
    "like_count": "like_count",

    "quoteCount": "quote_count",
    "quote_count": "quote_count",

    # campos ReTo adicionales (a veces vienen como customMapFunction o añadidos)
    "platform": "platform",
    "source_media": "source_media",
    "batch_id": "batch_id",
    "scrape_date": "scrape_date",
}


def _load_consolidar_state(state_path: Path) -> Dict[str, Dict]:
    """Carga el estado de archivos ya procesados."""
    if state_path.exists():
        try:
            with open(state_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_consolidar_state(state_path: Path, state: Dict[str, Dict]) -> None:
    """Guarda el estado de archivos procesados."""
    with open(state_path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def _file_fingerprint(p: Path) -> Dict:
    """Huella de un archivo: tamaño + mtime para detectar cambios."""
    st = p.stat()
    return {"size": st.st_size, "mtime": st.st_mtime}


def _filter_new_csvs(csv_files: List[Path], state: Dict[str, Dict]) -> List[Path]:
    """Devuelve solo los CSVs nuevos o modificados respecto al estado previo."""
    new_files = []
    for p in csv_files:
        fp = _file_fingerprint(p)
        prev = state.get(p.name)
        if prev is None or prev.get("size") != fp["size"] or prev.get("mtime") != fp["mtime"]:
            new_files.append(p)
    return new_files


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).parent
    default_in_dir = Path(os.getenv("DATA_RAW_DIR", str(script_dir / "data" / "raw")))
    default_out_file = Path(os.getenv("CONSOLIDAR_OUT_FILE", str(script_dir / "data" / "master" / "reto_x_master.csv")))
    
    p = argparse.ArgumentParser(description="Consolidar CSVs de Apify a un master normalizado")
    p.add_argument(
        "--in-dir",
        type=str,
        default=str(default_in_dir) if default_in_dir.exists() else None,
        help=f"Directorio con CSVs (ej: data/raw) (default: {default_in_dir})",
    )
    p.add_argument(
        "--out-file",
        type=str,
        default=str(default_out_file),
        help=f"Ruta del master (ej: data/master/reto_x_master.csv) (default: {default_out_file})",
    )
    p.add_argument(
        "--platform",
        default="x",
        help="Valor por defecto para platform si no viene en el CSV (default: x)",
    )
    p.add_argument(
        "--source-media",
        default="",
        help="Valor por defecto para source_media si no viene (opcional)",
    )
    p.add_argument(
        "--batch-id",
        default="",
        help="Valor por defecto para batch_id si no viene (opcional)",
    )
    p.add_argument(
        "--scrape-date",
        default="",
        help="Valor por defecto para scrape_date si no viene (ISO yyyy-mm-dd o datetime)",
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Reprocesar todos los archivos ignorando el estado incremental",
    )
    return p.parse_args()


def _safe_int(x) -> int:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return 0
    try:
        # Apify a veces entrega floats con .0
        return int(float(x))
    except Exception:
        return 0


def _parse_datetime_like(val: str) -> Optional[datetime]:
    if not val or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).strip()

    # Intento ISO primero
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        pass

    # Ejemplo X API: "Sun Dec 21 23:59:43 +0000 2025"
    try:
        return datetime.strptime(s, "%a %b %d %H:%M:%S %z %Y")
    except Exception:
        return None


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    # Renombrar columnas conocidas
    rename_map: Dict[str, str] = {}
    for col in df.columns:
        key = col
        if key in COLUMN_ALIASES:
            rename_map[col] = COLUMN_ALIASES[key]
    df = df.rename(columns=rename_map)

    # Asegurar columnas del esquema
    for col in SCHEMA:
        if col not in df.columns:
            df[col] = "" if col not in {"retweet_count", "reply_count", "like_count", "quote_count"} else 0

    # Normalizar tipos
    for m in ["retweet_count", "reply_count", "like_count", "quote_count"]:
        df[m] = df[m].apply(_safe_int)

    # created_at y scrape_date como string (preservamos), pero usamos datetime auxiliar para dedup
    df["_created_at_dt"] = df["created_at"].apply(_parse_datetime_like)
    df["_scrape_date_dt"] = df["scrape_date"].apply(_parse_datetime_like)

    return df


def ensure_uuid(df: pd.DataFrame) -> pd.DataFrame:
    """Genera uuid estable si falta.

    - Si viene uuid no vacío, se respeta.
    - Si no, se crea uuid5(NAMESPACE_URL, f"{platform}:{tweet_id}")
    """

    def make_uuid(row) -> str:
        # Buscar en uuid o message_uuid (compatibilidad)
        u = str(row.get("message_uuid", "") or row.get("uuid", "") or "").strip()
        if u:
            return u
        platform = str(row.get("platform", "") or "x").strip() or "x"
        tid = str(row.get("tweet_id", "") or "").strip()
        base = f"{platform}:{tid}"
        return str(uuidlib.uuid5(uuidlib.NAMESPACE_URL, base))

    df["message_uuid"] = df.apply(make_uuid, axis=1)
    return df


def apply_defaults(df: pd.DataFrame, args: argparse.Namespace) -> pd.DataFrame:
    if args.platform:
        df["platform"] = df["platform"].astype(str).replace({"": args.platform})
        df.loc[df["platform"].str.strip() == "", "platform"] = args.platform

    if args.source_media and (df["source_media"].astype(str).str.strip() == "").any():
        df.loc[df["source_media"].astype(str).str.strip() == "", "source_media"] = args.source_media

    if args.batch_id and (df["batch_id"].astype(str).str.strip() == "").any():
        df.loc[df["batch_id"].astype(str).str.strip() == "", "batch_id"] = args.batch_id

    if args.scrape_date and (df["scrape_date"].astype(str).str.strip() == "").any():
        df.loc[df["scrape_date"].astype(str).str.strip() == "", "scrape_date"] = args.scrape_date

    return df


USEFUL_COLUMNS: set = set(COLUMN_ALIASES.keys())


def _detect_usecols(csv_path: Path) -> Optional[List[str]]:
    """Read only the header to pick columns we actually need (Apify CSVs can have 2000+ cols)."""
    try:
        header_df = pd.read_csv(csv_path, nrows=0)
        keep = [c for c in header_df.columns if c in USEFUL_COLUMNS]
        return keep if keep else None
    except Exception:
        return None


def read_input_csvs_from_list(csv_files: List[Path]) -> pd.DataFrame:
    """Lee solo los CSVs de la lista proporcionada, cargando únicamente columnas relevantes."""
    print(f"Leyendo {len(csv_files)} archivos CSV...")
    frames: List[pd.DataFrame] = []
    for p in csv_files:
        try:
            usecols = _detect_usecols(p)
            print(f"  - Leyendo: {p.name} ({len(usecols) if usecols else 'todas'} cols)")
            df = pd.read_csv(p, dtype=str, keep_default_na=False, usecols=usecols)
            print(f"    ✓ {len(df)} filas leídas")
        except UnicodeDecodeError:
            print(f"  - Leyendo (con encoding utf-8): {p.name}")
            df = pd.read_csv(p, dtype=str, keep_default_na=False, encoding="utf-8",
                             errors="replace", usecols=usecols)
            print(f"    ✓ {len(df)} filas leídas")
        except Exception as e:
            print(f"  ✗ Error al leer {p.name}: {e}", file=sys.stderr)
            continue

        if len(df) == 0:
            print(f"    ⚠ Archivo vacío: {p.name}")
            continue

        df["_source_file"] = p.name
        frames.append(df)

    if not frames:
        raise ValueError(f"No se pudieron leer archivos CSV válidos de la lista proporcionada")

    print(f"Total: {len(frames)} archivos procesados")
    return pd.concat(frames, ignore_index=True)


def dedup_keep_latest(df: pd.DataFrame) -> pd.DataFrame:
    """Dedup por (platform, tweet_id) conservando el registro más reciente."""

    # Preferimos scrape_date si existe, si no created_at
    df["_rank_dt"] = df["_scrape_date_dt"].where(df["_scrape_date_dt"].notna(), df["_created_at_dt"])

    # Orden: más nuevo primero
    df = df.sort_values(by=["platform", "tweet_id", "_rank_dt"], ascending=[True, True, False])

    # drop_duplicates conserva el primero por orden
    df = df.drop_duplicates(subset=["platform", "tweet_id"], keep="first")
    return df


def append_to_master(new_df: pd.DataFrame, out_file: Path) -> Tuple[int, int]:
    """Agrega sólo filas nuevas al master (por platform+tweet_id)."""

    out_file.parent.mkdir(parents=True, exist_ok=True)

    if out_file.exists():
        print(f"  Master existente encontrado: {out_file}")
        master = pd.read_csv(out_file, dtype=str, keep_default_na=False)
        print(f"  Filas en master existente: {len(master)}")
        
        # Eliminar columnas duplicadas si existen
        master = master.loc[:, ~master.columns.duplicated()]
        
        # Renombrar columnas antiguas a nuevas (compatibilidad hacia atrás)
        rename_old_to_new = {}
        if "uuid" in master.columns and "message_uuid" not in master.columns:
            rename_old_to_new["uuid"] = "message_uuid"
        if "text" in master.columns and "content_original" not in master.columns:
            rename_old_to_new["text"] = "content_original"
        if rename_old_to_new:
            print(f"  Renombrando columnas: {rename_old_to_new}")
            master = master.rename(columns=rename_old_to_new)
        
        # Asegurar schema en master por si se creó con columnas de más/menos
        for col in SCHEMA:
            if col not in master.columns:
                master[col] = "" if col not in {"retweet_count", "reply_count", "like_count", "quote_count"} else "0"

        # Seleccionar solo columnas del esquema que existen en master
        master_schema_cols = [col for col in SCHEMA if col in master.columns]
        master_filtered = master[master_schema_cols].copy()

        # Normalizar key
        master_key = (master_filtered["platform"].astype(str) + "|" + master_filtered["tweet_id"].astype(str)).tolist()
        master_key_set = set(master_key)
        print(f"  Claves únicas en master: {len(master_key_set)}")

        # Asegurar que new_df tiene todas las columnas necesarias
        for col in SCHEMA:
            if col not in new_df.columns:
                new_df[col] = "" if col not in {"retweet_count", "reply_count", "like_count", "quote_count"} else "0"

        new_key = (new_df["platform"].astype(str) + "|" + new_df["tweet_id"].astype(str)).tolist()
        new_key_set = set(new_key)
        print(f"  Claves únicas en nuevos datos: {len(new_key_set)}")
        
        mask = [k not in master_key_set for k in new_key]
        to_add = new_df.loc[mask, SCHEMA].copy()
        print(f"  Filas nuevas a agregar: {len(to_add)}")

        if len(to_add) > 0:
            # Asegurar que ambas tienen las mismas columnas en el mismo orden
            # Resetear índices y asegurar columnas únicas
            master_clean = master_filtered[SCHEMA].copy()
            to_add_clean = to_add[SCHEMA].copy()
            
            # Resetear índices para evitar problemas de concatenación
            master_clean = master_clean.reset_index(drop=True)
            to_add_clean = to_add_clean.reset_index(drop=True)
            
            # Asegurar que las columnas son únicas
            master_clean = master_clean.loc[:, ~master_clean.columns.duplicated()]
            to_add_clean = to_add_clean.loc[:, ~to_add_clean.columns.duplicated()]
            
            # Seleccionar solo las columnas comunes en el orden del esquema
            common_cols = [col for col in SCHEMA if col in master_clean.columns and col in to_add_clean.columns]
            
            combined = pd.concat([master_clean[common_cols], to_add_clean[common_cols]], ignore_index=True)
            combined.to_csv(out_file, index=False)
            print(f"  ✓ Master actualizado con {len(to_add)} filas nuevas")
        else:
            # Aún si no hay filas nuevas, guardar el master con los nombres de columnas actualizados
            combined = master_filtered[SCHEMA].copy()
            combined.to_csv(out_file, index=False)
            print(f"  ⚠ No hay filas nuevas para agregar (todas ya están en el master)")

        return len(to_add), len(combined)

    # Si no existe, crearlo
    print(f"  Creando nuevo master: {out_file}")
    # Asegurar que todas las columnas del esquema existen
    for col in SCHEMA:
        if col not in new_df.columns:
            new_df[col] = "" if col not in {"retweet_count", "reply_count", "like_count", "quote_count"} else "0"
    
    new_df[SCHEMA].to_csv(out_file, index=False)
    print(f"  ✓ Master creado con {len(new_df)} filas")
    return len(new_df), len(new_df)


def main() -> int:
    args = parse_args()

    if not args.in_dir:
        script_dir = Path(__file__).parent
        default_path = script_dir / "data" / "raw"
        print(
            f"ERROR: Se requiere --in-dir.\n"
            f"  El directorio predeterminado no existe: {default_path}\n"
            f"  Ejemplo: --in-dir data/raw",
            file=sys.stderr,
        )
        return 2

    in_dir = Path(args.in_dir).expanduser().resolve()
    out_file = Path(args.out_file).expanduser().resolve()
    
    if not in_dir.exists():
        print(f"ERROR: El directorio de entrada no existe: {in_dir}", file=sys.stderr)
        return 2

    # --- Estado incremental ---
    state_path = in_dir / CONSOLIDAR_STATE_FILE
    state = {} if args.force else _load_consolidar_state(state_path)

    all_csvs = sorted([p for p in in_dir.glob("*.csv") if p.is_file()])
    if not all_csvs:
        print("No se encontraron CSV en el directorio de entrada.")
        return 0

    if args.force:
        csvs_to_read = all_csvs
        print(f"\n=== Consolidando CSVs (FORCE: reprocesando todos) ===")
    else:
        csvs_to_read = _filter_new_csvs(all_csvs, state)
        print(f"\n=== Consolidando CSVs (INCREMENTAL) ===")
        print(f"Archivos totales en carpeta: {len(all_csvs)}")
        print(f"Archivos ya procesados:      {len(all_csvs) - len(csvs_to_read)}")
        print(f"Archivos nuevos/modificados:  {len(csvs_to_read)}")

    if not csvs_to_read:
        print("\n✅ No hay archivos nuevos. Master ya está actualizado.")
        return 0

    print(f"Directorio de entrada: {in_dir}")
    print(f"Archivo de salida: {out_file}\n")
    
    df = read_input_csvs_from_list(csvs_to_read)
    print(f"\nFilas totales leídas: {len(df)}")
    
    print("\nNormalizando columnas...")
    df = normalize_columns(df)
    print(f"Filas después de normalización: {len(df)}")
    
    print("Aplicando valores por defecto...")
    df = apply_defaults(df, args)
    
    print("Generando UUIDs...")
    df = ensure_uuid(df)

    print("Filtrando filas sin tweet_id...")
    initial_count = len(df)
    df["tweet_id"] = df["tweet_id"].astype(str).str.strip()
    df = df[df["tweet_id"] != ""].copy()
    filtered_count = initial_count - len(df)
    if filtered_count > 0:
        print(f"  ⚠ {filtered_count} filas sin tweet_id fueron filtradas")

    print("Eliminando duplicados internos...")
    before_dedup = len(df)
    df = dedup_keep_latest(df)
    after_dedup = len(df)
    if before_dedup != after_dedup:
        print(f"  {before_dedup - after_dedup} duplicados eliminados")

    df = df[SCHEMA + ["_source_file", "_created_at_dt", "_scrape_date_dt", "_rank_dt"]]

    print(f"\nAgregando al master...")
    added, total = append_to_master(df, out_file)

    # Actualizar estado con TODOS los archivos actuales (nuevos + previos)
    new_state: Dict[str, Dict] = {}
    for p in all_csvs:
        new_state[p.name] = _file_fingerprint(p)
    _save_consolidar_state(state_path, new_state)
    print(f"  Estado incremental guardado ({len(new_state)} archivos registrados)")

    print(f"\n=== Resumen ===")
    print(f"Directorio de entrada: {in_dir}")
    print(f"Archivo de salida: {out_file}")
    print(f"Archivos procesados: {len(csvs_to_read)} de {len(all_csvs)}")
    print(f"Filas candidatas (después de dedup interno): {len(df)}")
    print(f"Filas agregadas al master: {added}")
    print(f"Total en master: {total}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
