"""Constantes compartidas del dashboard RETO (paletas, etiquetas, alias)."""


CATEGORIAS_LABELS = {
    "odio_etnico_cultural_religioso": "Étnico / Cultural / Religioso",
    "odio_genero_identidad_orientacion": "Género / Identidad / Orientación",
    "odio_condicion_social_economica_salud": "Condición Social / Económica / Salud",
    "odio_ideologico_politico": "Ideológico / Político",
    "odio_personal_generacional": "Personal / Generacional",
    "odio_profesiones_roles_publicos": "Profesiones / Roles Públicos",
}


EXCLUDED_SOURCE_MEDIA = {"grok", "Podcast"}


COLORS = {
    "primary": "#1F4E79",
    "accent": "#4F81BD",
    "danger": "#C0392B",
    "warning": "#F39C12",
    "success": "#27AE60",
    "muted": "#95A5A6",
    "current_week": "#EAB308",
}


# Paleta fija por categoría de odio (orden estable, vinculada a las etiquetas visibles)
# Mapea la LABEL visible (no la key interna) para funcionar con cualquier gráfico que use el label.
CAT_COLOR_MAP = {
    "Étnico / Cultural / Religioso": "#C0392B",
    "Género / Identidad / Orientación": "#8E44AD",
    "Condición Social / Económica / Salud": "#2E86AB",
    "Ideológico / Político": "#D97706",
    "Personal / Generacional": "#059669",
    "Profesiones / Roles Públicos": "#6B7280",
}


CAT_COLORS = list(CAT_COLOR_MAP.values())


# Paleta semántica unificada (mismos colores en toda la app para Odio/No Odio/Dudoso)
SEMANTIC_COLORS = {
    "Odio": "#C0392B",
    "No Odio": "#2F855A",
    "Dudoso": "#D69E2E",
}


# Intensidad (gradación coherente: ámbar claro → ámbar → rojo)
INTENSITY_COLORS = {"1": "#FBD38D", "2": "#F59E0B", "3": "#C0392B"}


# Colores fijos por plataforma (mixto: X = azul ReTo, YouTube = rojo oficial)
PLATFORM_COLORS = {
    "X": "#1F4E79",
    "YouTube": "#FF0000",
    "x": "#1F4E79",
    "twitter": "#1F4E79",
    "youtube": "#FF0000",
}


# Mapeo de nombres de plataforma para mostrar
PLATFORM_DISPLAY = {
    "x": "X",
    "twitter": "X",
    "youtube": "YouTube",
}


# "twitter" y "x" son la misma plataforma en distintas épocas de scraping
_PLATFORM_ALIASES = {"x": ("x", "twitter"), "twitter": ("x", "twitter")}


def platform_label(val: str) -> str:
    """Convierte el valor interno de plataforma a su nombre visible."""
    return PLATFORM_DISPLAY.get(val, val)


DELITOS_COLORS = [
    "#E74C3C", "#3498DB", "#2ECC71", "#F39C12", "#9B59B6",
    "#1ABC9C", "#E67E22", "#34495E", "#E91E63", "#00BCD4",
    "#8BC34A", "#FF5722",
]
