# Automatización del Script youtube_extract_hate.py

Este documento explica cómo automatizar la ejecución diaria del script `youtube_extract_hate.py` a las 9:30 AM.

## Archivos Creados

1. **`run_youtube_extract_daily.sh`**: Script wrapper que ejecuta `youtube_extract_hate.py`
2. **`com.retoscraper.youtube_extract.plist`**: Configuración de Launchd para automatización

## Instalación (macOS)

### Paso 1: Verificar que el script sea ejecutable

```bash
chmod +x "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/run_youtube_extract_daily.sh"
```

### Paso 2: Instalar el servicio de Launchd

```bash
# Copiar el archivo plist a LaunchAgents
cp "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/com.retoscraper.youtube_extract.plist" ~/Library/LaunchAgents/

# Cargar el servicio (macOS moderno)
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube_extract.plist
```

### Paso 3: Verificar que está instalado

```bash
launchctl list | grep youtube_extract
```

## Comandos Útiles

### Ver logs del servicio

```bash
# Logs estándar
tail -f ~/Library/Logs/com.retoscraper.youtube_extract.log

# Logs del script (en la carpeta logs del proyecto)
tail -f "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/logs/youtube_extract_daily_*.log"
```

### Detener el servicio

```bash
launchctl bootout gui/$(id -u)/com.retoscraper.youtube_extract
```

### Reiniciar el servicio

```bash
launchctl bootout gui/$(id -u)/com.retoscraper.youtube_extract
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube_extract.plist
```

### Ejecutar manualmente (sin esperar a las 9:30 AM)

```bash
"/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/run_youtube_extract_daily.sh"
```

### Cambiar la hora de ejecución

1. Editar el archivo `com.retoscraper.youtube_extract.plist`
2. Modificar las líneas:
   ```xml
   <key>Hour</key>
   <integer>9</integer>
   <key>Minute</key>
   <integer>30</integer>
   ```
3. Recargar el servicio:
   ```bash
   launchctl bootout gui/$(id -u)/com.retoscraper.youtube_extract
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube_extract.plist
   ```

## Notas Importantes

- **Rotación automática**: El script ya tiene implementada la rotación de medios, así que cada día procesará diferentes medios comenzando desde donde quedó.
- **Aleatorización**: Si `RANDOMIZE_ORDER = True`, cada día tendrá un orden diferente de medios.
- **Estado guardado**: El script guarda el estado en `youtube_extract_state.json`, así que si se interrumpe por cuota, continuará desde donde quedó.
- **Logs**: Todos los logs se guardan en `logs/youtube_extract_daily_*.log` con fecha y hora.

## Solución de Problemas

### El servicio no se ejecuta

1. Verificar que el script sea ejecutable:
   ```bash
   ls -l "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/run_youtube_extract_daily.sh"
   ```

2. Verificar permisos del archivo plist:
   ```bash
   ls -l ~/Library/LaunchAgents/com.retoscraper.youtube_extract.plist
   ```

3. Ver errores en los logs:
   ```bash
   tail -f ~/Library/Logs/com.retoscraper.youtube_extract.log
   ```

### El servicio se ejecuta pero falla

Revisa los logs del script en:
```bash
ls -lt "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/Medios/logs/" | head
```


















