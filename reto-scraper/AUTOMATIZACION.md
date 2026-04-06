# Automatización del Scraper de YouTube

Este documento explica cómo automatizar la ejecución diaria del scraper de YouTube a las 9:30 AM.

## Cambios Realizados

### 1. Filtro de 24 horas
El script `run_pending_youtube.py` ahora filtra automáticamente solo los jobs de videos de las últimas 24 horas.

### 2. Script Wrapper
Se creó `jobs/run_youtube_daily.sh` que:
- Genera los jobs para las últimas 24 horas
- Ejecuta los jobs pendientes
- Guarda logs con fecha en la carpeta `logs/`

### 3. Configuración de Launchd
Se creó `com.retoscraper.youtube.plist` para ejecutar el script automáticamente todos los días a las 9:30 AM.

## Instalación de la Automatización (macOS)

### Paso 1: Verificar que el script sea ejecutable

```bash
chmod +x "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/reto-scraper/jobs/run_youtube_daily.sh"
```

### Paso 2: Instalar el servicio de Launchd

```bash
# Copiar el archivo plist a la carpeta de LaunchAgents del usuario
cp "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/reto-scraper/com.retoscraper.youtube.plist" ~/Library/LaunchAgents/

# Cargar el servicio (usar bootstrap en macOS modernos)
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube.plist
```

**Nota:** En macOS modernos (Big Sur+), usar `bootstrap` en lugar de `load`. Si tienes problemas, también puedes intentar:
```bash
launchctl load ~/Library/LaunchAgents/com.retoscraper.youtube.plist
```

### Paso 3: Verificar que está instalado

```bash
# Ver el estado del servicio
launchctl list | grep retoscraper

# Ver los próximos eventos programados
launchctl list com.retoscraper.youtube
```

## Comandos Útiles

### Ver logs del servicio

```bash
# Logs estándar
tail -f ~/Library/Logs/com.retoscraper.youtube.log

# Logs del script (en la carpeta logs del proyecto)
tail -f "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/reto-scraper/logs/youtube_daily_*.log"
```

### Detener el servicio

```bash
# Para macOS modernos (Big Sur+)
launchctl bootout gui/$(id -u)/com.retoscraper.youtube

# Para versiones anteriores
launchctl unload ~/Library/LaunchAgents/com.retoscraper.youtube.plist
```

### Reiniciar el servicio

```bash
# Detener
launchctl bootout gui/$(id -u)/com.retoscraper.youtube 2>/dev/null || launchctl unload ~/Library/LaunchAgents/com.retoscraper.youtube.plist

# Iniciar
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube.plist
```

### Ejecutar manualmente (sin esperar a las 9:30 AM)

```bash
# Ejecutar el script directamente
"/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/reto-scraper/jobs/run_youtube_daily.sh"
```

### Cambiar la hora de ejecución

1. Editar el archivo `com.retoscraper.youtube.plist`
2. Modificar las líneas:
   ```xml
   <key>Hour</key>
   <integer>9</integer>
   <key>Minute</key>
   <integer>30</integer>
   ```
3. Recargar el servicio:
   ```bash
   launchctl bootout gui/$(id -u)/com.retoscraper.youtube 2>/dev/null || launchctl unload ~/Library/LaunchAgents/com.retoscraper.youtube.plist
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.retoscraper.youtube.plist
   ```

## Configuración

### Variables de entorno en el script

Puedes editar `jobs/run_youtube_daily.sh` para ajustar:

- `RUN_YT_LIMIT`: Número máximo de jobs a ejecutar (default: 100)
- `RUN_YT_SLEEP`: Segundos de pausa entre jobs (default: 2)
- `RUN_YT_HOURS_BACK`: Horas hacia atrás para filtrar (default: 24)

### Ruta del entorno virtual

Si tu entorno virtual está en otra ubicación, edita la sección en `run_youtube_daily.sh`:

```bash
# Activar entorno virtual
if [ -d "venv" ]; then
    source venv/bin/activate
elif [ -d ".venv" ]; then
    source .venv/bin/activate
fi
```

## Notas

- El servicio solo se ejecuta si tu Mac está encendido a las 9:30 AM
- Los logs se guardan automáticamente con fecha y hora en la carpeta `logs/`
- Si necesitas que se ejecute siempre aunque el Mac esté apagado, considera usar un servidor remoto con cron o systemd

## Solución de Problemas

### El servicio no se ejecuta

1. Verificar que el script sea ejecutable:
   ```bash
   ls -l jobs/run_youtube_daily.sh
   ```

2. Verificar permisos del archivo plist:
   ```bash
   ls -l ~/Library/LaunchAgents/com.retoscraper.youtube.plist
   ```

3. Ver errores en los logs:
   ```bash
   tail -f ~/Library/Logs/com.retoscraper.youtube.log
   ```

### El servicio se ejecuta pero falla

Revisa los logs del script en:
```bash
ls -lt "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/reto-scraper/logs/" | head
```

