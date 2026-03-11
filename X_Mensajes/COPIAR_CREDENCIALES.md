# 📋 Instrucciones para Copiar las Credenciales

El archivo JSON de credenciales está en un directorio protegido de Notes. Necesitas copiarlo manualmente a una ubicación accesible.

## Opción 1: Copiar desde Notes (Recomendado)

1. Abre la aplicación **Notes** en tu Mac
2. Busca la nota que contiene el archivo `gen-lang-client-0962946576-cb7a6b166a7b.json`
3. Haz clic derecho en el archivo adjunto
4. Selecciona **"Revelar en Finder"** o **"Guardar como..."**
5. Copia el archivo a: `/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/X_Mensajes/credentials.json`

## Opción 2: Usar Finder directamente

1. Abre **Finder**
2. Presiona `Cmd + Shift + G` (Ir a carpeta)
3. Pega esta ruta:
   ```
   ~/Library/Containers/com.apple.Notes/Data/Library/CoreData/Attachments/67E6CA2F-84C7-4511-BF9A-0DD973132D12/
   ```
4. Busca el archivo JSON y cópialo al directorio del proyecto

## Opción 3: Usar Terminal (si tienes permisos)

```bash
# Crear directorio si no existe
mkdir -p "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/X_Mensajes"

# Intentar copiar (puede requerir permisos)
cp "/Users/alejandroyankilevich/Library/Containers/com.apple.Notes/Data/Library/CoreData/Attachments/67E6CA2F-84C7-4511-BF9A-0DD973132D12/gen-lang-client-0962946576-cb7a6b166a7b.json" \
   "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/X_Mensajes/credentials.json"
```

## Después de copiar

Una vez que tengas el archivo en `credentials.json` en el directorio del proyecto, puedes ejecutar:

```bash
cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/X_Mensajes"
source venv/bin/activate
python sync_drive_csvs.py --folder-id "1sA5HaxAYcWant1MevcALXC8np7XH5YVp" --out-dir "data/raw"
```

O simplemente:

```bash
cd "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/X_Mensajes"
source venv/bin/activate
python sync_drive_csvs.py --credentials "credentials.json" --folder-id "1sA5HaxAYcWant1MevcALXC8np7XH5YVp" --out-dir "data/raw"
```




