# Guía de Uso: sync_drive_csvs.py

## 📋 Requisitos Previos

1. **Archivo de credenciales JSON**: Service Account de Google Cloud
   - Ruta configurada: `/Users/alejandroyankilevich/Library/Containers/com.apple.Notes/Data/Library/CoreData/Attachments/67E6CA2F-84C7-4511-BF9A-0DD973132D12/gen-lang-client-0962946576-cb7a6b166a7b.json`

2. **ID de la carpeta de Google Drive**: 
   - Obtener de la URL: `https://drive.google.com/drive/folders/ESTE_ES_EL_ID`
   - La carpeta debe estar compartida con el email de la Service Account

3. **Directorio de salida**: Donde se descargarán los CSV (por defecto: `data/raw`)

## 🚀 Formas de Ejecutar

### Opción 1: Con argumentos mínimos (usa ruta predeterminada de credenciales)

```bash
python sync_drive_csvs.py --folder-id "TU_FOLDER_ID" --out-dir "data/raw"
```

### Opción 2: Especificando todo explícitamente

```bash
python sync_drive_csvs.py \
  --credentials "/ruta/completa/a/credenciales.json" \
  --folder-id "TU_FOLDER_ID" \
  --out-dir "data/raw"
```

### Opción 3: Usando el script helper

```bash
./ejecutar_sync.sh --folder-id "TU_FOLDER_ID" --out-dir "data/raw"
```

### Opción 4: Con variables de entorno

```bash
export GOOGLE_DRIVE_CREDENTIALS="/ruta/a/credenciales.json"
export GOOGLE_DRIVE_FOLDER_ID="TU_FOLDER_ID"
export GOOGLE_DRIVE_OUT_DIR="data/raw"
python sync_drive_csvs.py
```

## 📝 Ejemplos Completos

### Ejemplo 1: Descarga básica
```bash
python sync_drive_csvs.py \
  --folder-id "1aBCdEfGhijkLmnOpQrStUvWxYzZ" \
  --out-dir "data/raw"
```

### Ejemplo 2: Con patrón específico
```bash
python sync_drive_csvs.py \
  --folder-id "1aBCdEfGhijkLmnOpQrStUvWxYzZ" \
  --out-dir "data/raw" \
  --pattern "Scrap_Batch_*.csv"
```

### Ejemplo 3: Con prefijo de fecha
```bash
python sync_drive_csvs.py \
  --folder-id "1aBCdEfGhijkLmnOpQrStUvWxYzZ" \
  --out-dir "data/raw" \
  --prefix-with-date
```

## ⚠️ Solución de Problemas

### Error: "No se encontró el archivo de credenciales"
- Verifica que el archivo JSON existe en la ruta especificada
- Si el archivo está en otra ubicación, usa `--credentials` para especificarlo
- Asegúrate de tener permisos de lectura

### Error: "Se requiere --folder-id"
- Debes proporcionar el ID de la carpeta de Google Drive
- Obtén el ID de la URL de la carpeta en Drive
- O usa la variable de entorno `GOOGLE_DRIVE_FOLDER_ID`

### Error de permisos en Google Drive
- Asegúrate de que la carpeta esté compartida con el email de la Service Account
- Verifica que la Service Account tenga permisos de lectura

## 🔍 Obtener el Folder ID

1. Abre la carpeta en Google Drive
2. Copia la URL completa
3. El ID es la parte entre `/folders/` y el `?` o el final de la URL

Ejemplo:
- URL: `https://drive.google.com/drive/folders/1aBCdEfGhijkLmnOpQrStUvWxYzZ?usp=sharing`
- Folder ID: `1aBCdEfGhijkLmnOpQrStUvWxYzZ`




