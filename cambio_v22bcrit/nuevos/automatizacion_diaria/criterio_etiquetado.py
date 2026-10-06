"""
criterio_etiquetado.py — Criterio de etiquetado LLM por plataforma y semana (v1 → v22bcrit).

Una sola fuente de verdad para:
  - la semana de corte (CRITERIO_CORTE_SEMANA),
  - qué criterio corresponde a un mensaje (según plataforma y semana de PUBLICACIÓN),
  - el umbral de spike provisional / definitivo.

Sin dependencias de BD ni de OpenAI: se puede importar y probar en cualquier sitio.

Regla de oro: todos los mensajes de una misma semana (lunes-domingo) en la misma
plataforma usan el mismo criterio. El criterio depende de `created_at` (fecha de
publicación) y de la plataforma — v22bcrit solo se validó en X. YouTube siempre v1.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional, Tuple

CRITERIO_V1 = "v1"
CRITERIO_V22BCRIT = "v22bcrit"

# Lunes (inicio de semana) a partir del cual los mensajes NUEVOS de X se etiquetan con
# v22bcrit. Debe ser SIEMPRE lunes. Si se cambia aquí, hay que cambiarlo también en los
# dashboards (copia local en dashboard_v3.py y components/aviso_criterio.py):
# pruebas/test_replay_umbral.py::test_corte_consistente comprueba que coinciden.
CRITERIO_CORTE_SEMANA = date(2026, 10, 19)

# Umbral de spike (en % de odio semanal) mientras no haya suficientes semanas con el
# criterio nuevo. Procede del informe de calibración de v22bcrit:
# media 5.70 %, IC 95 % [5.13, 6.10].
UMBRAL_PROVISIONAL_PCT = 5.70

# Cuántas semanas CERRADAS con el criterio nuevo hacen falta para pasar del umbral
# provisional al umbral propio (promedio de semanas post-corte × SPIKE_MULTIPLICADOR).
MIN_SEMANAS_POSTCORTE = 12

SPIKE_MULTIPLICADOR = 1.5
MIN_MENSAJES_SPIKE = 300


def inicio_semana(d: date) -> date:
    """Lunes de la semana de `d` (igual que DATE_TRUNC('week', ...) de PostgreSQL)."""
    return d - timedelta(days=d.weekday())


def a_fecha(valor: Any) -> Optional[date]:
    """
    Convierte date / datetime / texto ISO a `date`. Devuelve None si no hay fecha válida.

    Los datetime con zona horaria se pasan a UTC antes de tomar la fecha: es la zona
    de sesión de Neon, que es la que usa `created_at::date` en las consultas semanales.
    """
    if valor is None:
        return None
    if isinstance(valor, datetime):
        if valor.tzinfo is not None:
            valor = valor.astimezone(timezone.utc)
        return valor.date()
    if isinstance(valor, date):
        return valor
    texto = str(valor).strip()
    if not texto or texto.lower() in {"nan", "nat", "none", "null"}:
        return None
    try:
        return a_fecha(datetime.fromisoformat(texto.replace("Z", "+00:00")))
    except ValueError:
        try:
            return date.fromisoformat(texto[:10])
        except ValueError:
            return None


def criterio_para_fecha(
    fecha_publicacion: Any,
    corte: date = CRITERIO_CORTE_SEMANA,
    *,
    platform: str = "x",
) -> str:
    """
    Criterio que le toca a un mensaje según su plataforma y fecha de PUBLICACIÓN.

    - platform != "x"          → v1  (v22bcrit solo validado en X; YouTube siempre v1).
    - Semana (lunes) >= corte  → v22bcrit
    - Semana anterior al corte → v1  (incluye mensajes que llegan tarde: un mensaje
      publicado antes del corte pero ingestado/etiquetado después sigue siendo v1,
      para no mezclar criterios dentro de una semana ya cerrada).
    - Sin fecha                → v1  (no cuenta en ninguna semana del análisis).
    """
    if platform != "x":
        return CRITERIO_V1
    d = a_fecha(fecha_publicacion)
    if d is None:
        return CRITERIO_V1
    return CRITERIO_V22BCRIT if inicio_semana(d) >= corte else CRITERIO_V1


def umbral_spike(
    n_semanas_postcorte: int, promedio_postcorte_pct: Optional[float]
) -> Tuple[float, bool]:
    """
    Umbral de spike (%) para una semana post-corte y si es provisional.

    - Menos de MIN_SEMANAS_POSTCORTE semanas previas con el criterio nuevo →
      UMBRAL_PROVISIONAL_PCT (provisional=True).
    - Desde ahí → promedio de las semanas post-corte × SPIKE_MULTIPLICADOR.
    """
    if n_semanas_postcorte < MIN_SEMANAS_POSTCORTE or promedio_postcorte_pct is None:
        return round(UMBRAL_PROVISIONAL_PCT, 2), True
    return round(float(promedio_postcorte_pct) * SPIKE_MULTIPLICADOR, 2), False


def es_spike(pct_odio: float, total_mensajes: int, umbral_pct: float) -> bool:
    """Misma regla que antes: pct >= umbral y al menos MIN_MENSAJES_SPIKE mensajes."""
    return pct_odio >= umbral_pct and total_mensajes >= MIN_MENSAJES_SPIKE
