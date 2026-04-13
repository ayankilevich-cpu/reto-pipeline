"""sync_drive_csvs.py

Descarga automáticamente los CSV exportados por Apify desde una carpeta de Google Drive
usando Service Account.

- Lista archivos en una carpeta (por FOLDER_ID)
- Filtra por nombre/extensión (por defecto *.csv)
- Descarga a un directorio local
- Evita re-descargas si el archivo no cambió (pide md5Checksum vía files.get si list() no lo trae).
- Si cambia solo el nombre local (p. ej. --prefix-with-date) pero el id de Drive y el contenido
  son los mismos, copia desde la ruta guardada en .drive_sync_state.json en lugar de volver a bajar.

Requisitos:
  pip install google-api-python-client google-auth

Uso (ejemplos):
  # Con argumentos de línea de comandos:
  python sync_drive_csvs.py --credentials "service-account.json" --folder-id "<ID_CARPETA>" --out-dir "data/raw"
  python sync_drive_csvs.py --credentials "service-account.json" --folder-id "<ID_CARPETA>" --out-dir "data/raw" --pattern "Scrap_Batch_*.csv"
  
  # Con variables de entorno (opcional):
  export GOOGLE_DRIVE_CREDENTIALS="service-account.json"
  export GOOGLE_DRIVE_FOLDER_ID="<ID_CARPETA>"
  export GOOGLE_DRIVE_OUT_DIR="data/raw"
  python sync_drive_csvs.py

Notas:
- Para obtener el folder id: es la parte entre /folders/ y el ? en la URL.
- La carpeta debe estar compartida con el email de la Service Account.
- Los argumentos de línea de comandos tienen prioridad sobre las variables de entorno.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import os
import shutil
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


DRIVE_SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]
DEFAULT_PATTERN = "*.csv"
STATE_FILENAME = ".drive_sync_state.json"


def build_drive_service(credentials_json: Path):
    creds = service_account.Credentials.from_service_account_file(
        str(credentials_json), scopes=DRIVE_SCOPES
    )
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def list_files_in_folder(service, folder_id: str) -> List[dict]:
    """Lista todos los archivos (no carpetas) dentro del folder_id."""
    q = (
        f"'{folder_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
    )
    fields = "nextPageToken, files(id, name, mimeType, modifiedTime, md5Checksum, size)"

    files: List[dict] = []
    page_token: Optional[str] = None
    while True:
        resp = (
            service.files()
            .list(q=q, fields=fields, pageSize=1000, pageToken=page_token)
            .execute()
        )
        files.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    return files


def load_state(state_path: Path) -> Dict[str, dict]:
    if state_path.exists():
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def save_state(state_path: Path, state: Dict[str, dict]) -> None:
    state_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def enrich_metadata_if_needed(service, meta: dict) -> None:
    """files.list a veces no devuelve md5Checksum; sin él el caché falla más a menudo."""
    if meta.get("md5Checksum"):
        return
    full = (
        service.files()
        .get(fileId=meta["id"], fields="md5Checksum,modifiedTime,size")
        .execute()
    )
    if full.get("md5Checksum"):
        meta["md5Checksum"] = full["md5Checksum"]
    if full.get("modifiedTime") and not meta.get("modifiedTime"):
        meta["modifiedTime"] = full["modifiedTime"]
    if full.get("size") is not None and meta.get("size") is None:
        meta["size"] = full.get("size")


def _content_unchanged_on_drive(meta: dict, prev: dict) -> bool:
    """True si el archivo en Drive no cambió respecto al último sync guardado en state."""
    m_now, p_now = meta.get("md5Checksum"), prev.get("md5Checksum")
    if m_now and p_now:
        return m_now == p_now
    return meta.get("modifiedTime") == prev.get("modifiedTime")


def plan_sync_action(
    meta: dict, state: Dict[str, dict], dest_path: Path
) -> Tuple[str, Literal["download", "skip", "copy_local"]]:
    """
    Devuelve (motivo_log, acción).

    - skip: ya está en dest y Drive no cambió.
    - copy_local: Drive sin cambios; hay copia en otra ruta local → copiar sin API.
    - download: hace falta bajar de Drive.
    """
    fid = meta["id"]
    prev = state.get(fid)
    if not prev:
        return ("nuevo (sin entrada en estado)", "download")

    unchanged = _content_unchanged_on_drive(meta, prev)
    if not unchanged:
        return ("contenido en Drive cambió (md5 o fecha)", "download")

    if dest_path.exists():
        return ("omitido (ya sincronizado)", "skip")

    prev_path_str = prev.get("downloaded_to")
    if prev_path_str:
        prev_path = Path(prev_path_str)
        if prev_path != dest_path and prev_path.exists():
            return ("misma versión en Drive; copia local desde ruta anterior", "copy_local")

    return ("falta archivo local o ruta previa inexistente", "download")


def download_file(service, file_id: str, dest_path: Path) -> None:
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    request = service.files().get_media(fileId=file_id)

    with dest_path.open("wb") as f:
        downloader = MediaIoBaseDownload(f, request, chunksize=1024 * 1024)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # status puede ser None al inicio
            if status is not None:
                pct = int(status.progress() * 100)
                print(f"  - descargando... {pct}%", end="\r")
    print(f"  - descargado: {dest_path}")


def main() -> int:
    # Rutas predeterminadas a las credenciales (en orden de prioridad)
    script_dir = Path(__file__).parent
    DEFAULT_CREDENTIALS_OPTIONS = [
        script_dir / "credentials.json",
        Path(os.getenv("GOOGLE_DRIVE_CREDENTIALS", "credentials.json")),
    ]
    
    # Buscar la primera ruta que exista
    default_cred_path = None
    for cred_path in DEFAULT_CREDENTIALS_OPTIONS:
        if cred_path.exists():
            default_cred_path = str(cred_path)
            break
    
    parser = argparse.ArgumentParser(description="Sync CSVs from Google Drive folder")
    parser.add_argument(
        "--credentials",
        type=str,
        default=os.getenv("GOOGLE_DRIVE_CREDENTIALS", default_cred_path),
        help="Ruta al JSON de Service Account (o usar env var GOOGLE_DRIVE_CREDENTIALS)",
    )
    parser.add_argument(
        "--folder-id",
        type=str,
        default=os.getenv("GOOGLE_DRIVE_FOLDER_ID", "1sA5HaxAYcWant1MevcALXC8np7XH5YVp"),
        help="ID de la carpeta de Drive (o usar env var GOOGLE_DRIVE_FOLDER_ID)",
    )
    parser.add_argument(
        "--out-dir",
        type=str,
        default=os.getenv("GOOGLE_DRIVE_OUT_DIR", str(script_dir / "data" / "raw")),
        help="Directorio local destino (o usar env var GOOGLE_DRIVE_OUT_DIR, default: /Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Data/raw)",
    )
    parser.add_argument("--pattern", default=DEFAULT_PATTERN, type=str, help="Patrón glob (ej: Scrap_Batch_*.csv)")
    parser.add_argument(
        "--keep-drive-name",
        action="store_true",
        help="Guardar con el nombre original de Drive (default).",
    )
    parser.add_argument(
        "--prefix-with-date",
        action="store_true",
        help="Prefijar nombre con YYYYMMDD_ para evitar pisado local.",
    )
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Solo descargar archivos nuevos (que no están en el estado de sincronización)",
    )
    parser.add_argument(
        "--max-new-files",
        type=int,
        default=None,
        help="Máximo número de archivos nuevos a descargar (útil para limitar descargas grandes). Solo se aplica con --only-new",
    )
    parser.add_argument(
        "--no-enrich",
        action="store_true",
        help="No llamar a files.get para rellenar md5Checksum (menos llamadas API; el caché es menos fiable).",
    )

    args = parser.parse_args()

    # Validar argumentos requeridos
    if not args.credentials:
        parser.error(
            "Se requiere --credentials o la variable de entorno GOOGLE_DRIVE_CREDENTIALS. "
            "Ejemplo: --credentials /ruta/a/service-account.json"
        )
    if not args.folder_id:
        parser.error(
            "Se requiere --folder-id o la variable de entorno GOOGLE_DRIVE_FOLDER_ID. "
            "Ejemplo: --folder-id 'TU_FOLDER_ID'"
        )

    credentials_json = Path(args.credentials).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve()
    state_path = out_dir / STATE_FILENAME

    if not credentials_json.exists():
        error_msg = f"❌ No se encontró el archivo de credenciales:\n   {credentials_json}\n\n"
        error_msg += "Por favor verifica:\n"
        error_msg += "1. Que la ruta sea correcta\n"
        error_msg += "2. Que el archivo JSON de Service Account exista\n"
        error_msg += "3. Que tengas permisos de lectura\n\n"
        error_msg += "Puedes especificar otra ruta con: --credentials /ruta/alternativa.json"
        raise FileNotFoundError(error_msg)

    service = build_drive_service(credentials_json)

    print(f"Listando archivos en folder: {args.folder_id}")
    files = list_files_in_folder(service, args.folder_id)

    # Filtrar por patrón
    selected = [f for f in files if fnmatch.fnmatch(f.get("name", ""), args.pattern)]
    print(f"Encontrados {len(files)} archivos, seleccionados {len(selected)} por patrón '{args.pattern}'")

    state = load_state(state_path)

    # Si --only-new está activado, filtrar solo archivos nuevos (no en el estado)
    if args.only_new:
        initial_count = len(selected)
        selected = [f for f in selected if f["id"] not in state]
        new_count = len(selected)
        print(f"Modo --only-new: {initial_count - new_count} archivos ya sincronizados, {new_count} archivos nuevos encontrados")
        
        # Ordenar por fecha de modificación (más recientes primero)
        def get_modified_time(meta):
            try:
                mt = meta.get("modifiedTime", "")
                if mt:
                    return datetime.fromisoformat(mt.replace("Z", "+00:00"))
            except Exception:
                pass
            return datetime.min
        
        selected.sort(key=get_modified_time, reverse=True)
        
        # Limitar cantidad si se especificó --max-new-files
        if args.max_new_files and args.max_new_files > 0:
            selected = selected[:args.max_new_files]
            print(f"Limitado a los {len(selected)} archivos más recientes (--max-new-files={args.max_new_files})")

    # Detectar archivos con nombres duplicados para generar nombres únicos
    name_counts = defaultdict(int)
    for meta in selected:
        name_counts[meta["name"]] += 1
    
    downloads = 0
    copies = 0
    for meta in selected:
        name = meta["name"]
        
        # Generar nombre local ANTES de verificar si descargar
        # Esto es necesario para verificar si el archivo realmente existe
        local_name = name
        
        # Si hay múltiples archivos con el mismo nombre, usar prefijo con fecha de modificación
        if name_counts[name] > 1 or args.prefix_with_date:
            # Extraer fecha de modificación del archivo en Drive
            modified_time = meta.get("modifiedTime", "")
            if modified_time:
                try:
                    # Parsear fecha ISO y convertir a YYYYMMDD
                    dt = datetime.fromisoformat(modified_time.replace("Z", "+00:00"))
                    date_prefix = dt.strftime("%Y%m%d")
                except Exception:
                    # Fallback a fecha actual si no se puede parsear
                    date_prefix = datetime.now().strftime("%Y%m%d")
            else:
                date_prefix = datetime.now().strftime("%Y%m%d")
            
            # Si hay duplicados, también agregar un sufijo con los últimos 8 caracteres del ID
            if name_counts[name] > 1:
                file_id_suffix = meta["id"][-8:]
                base_name = Path(name).stem
                ext = Path(name).suffix
                local_name = f"{date_prefix}_{base_name}_{file_id_suffix}{ext}"
            else:
                local_name = f"{date_prefix}_{name}"

        dest = out_dir / local_name

        # Mejorar metadatos para caché: list() a menudo omite md5Checksum
        if not args.no_enrich and meta["id"] in state:
            enrich_metadata_if_needed(service, meta)

        reason, action = plan_sync_action(meta, state, dest)

        if action == "skip":
            print(f"  - omitido (ya sincronizado): {name} -> {local_name} (id={meta['id']})")
            continue

        if action == "copy_local":
            prev_path = Path(state[meta["id"]]["downloaded_to"])
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(prev_path, dest)
            state[meta["id"]]["downloaded_to"] = str(dest)
            copies += 1
            print(f"  - copiado local (sin API): {name} -> {local_name} (id={meta['id']})")
            continue

        print(f"Descargando: {name} (id={meta['id']}) — {reason}")
        download_file(service, meta["id"], dest)

        if not args.no_enrich:
            enrich_metadata_if_needed(service, meta)

        # Actualizar estado
        state[meta["id"]] = {
            "name": name,
            "md5Checksum": meta.get("md5Checksum"),
            "modifiedTime": meta.get("modifiedTime"),
            "size": meta.get("size"),
            "downloaded_to": str(dest),
        }
        downloads += 1
 
    save_state(state_path, state)
    print(
        f"Sync completo. Descargas desde Drive: {downloads}. "
        f"Copias locales (sin red): {copies}. Estado: {state_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
