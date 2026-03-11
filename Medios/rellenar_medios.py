import pandas as pd
import requests
from bs4 import BeautifulSoup
from pathlib import Path

# Directorio base del script
BASE_DIR = Path(__file__).parent

# Intentar ambos nombres posibles
INPUT_FILE_CANDIDATES = [
    BASE_DIR / "Medios_Andalucia_Web_Redes.xlsx",
    BASE_DIR / "Medios_Andalucia_Web.Redes.xlsx",
]

INPUT_FILE = None
for candidate in INPUT_FILE_CANDIDATES:
    if candidate.exists():
        INPUT_FILE = candidate
        break

if INPUT_FILE is None:
    raise FileNotFoundError(
        f"No se encontró el archivo Excel. Buscado en: {[str(c) for c in INPUT_FILE_CANDIDATES]}"
    )

OUTPUT_FILE = BASE_DIR / "Medios_Andalucia_Web_Redes_completo.xlsx"

SOCIAL_DOMAINS = {
    "Facebook": ["facebook.com"],
    "Instagram": ["instagram.com"],
    "X": ["x.com", "twitter.com"],
    "YouTube": ["youtube.com", "youtu.be"],
    "TikTok": ["tiktok.com"],
}

def find_social_links(url):
    links_found = {k: None for k in SOCIAL_DOMAINS.keys()}

    if not isinstance(url, str):
        return links_found
    url = url.strip()
    if not url or url.lower() == "nan":
        return links_found

    try:
        if not url.startswith("http"):
            url = "https://" + url.lstrip("/")

        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
    except Exception:
        return links_found

    soup = BeautifulSoup(resp.text, "html.parser")

    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith("#") or href.startswith("mailto:"):
            continue

        for network, patterns in SOCIAL_DOMAINS.items():
            if links_found[network] is not None:
                continue
            for patt in patterns:
                if patt in href:
                    links_found[network] = href
                    break

    return links_found


def main():
    df = pd.read_excel(str(INPUT_FILE))

    # Asegurar columnas de redes
    for col in ["Facebook", "Instagram", "X", "YouTube", "TikTok"]:
        if col not in df.columns:
            df[col] = None

    for idx, row in df.iterrows():
        web = row.get("Web", "")
        medio = row.get("Medio", "")
        print(f"Procesando {idx+1}: {medio} - {web}")

        social_links = find_social_links(web)

        for col in ["Facebook", "Instagram", "X", "YouTube", "TikTok"]:
            current_value = row.get(col, "")
            if (pd.isna(current_value) or str(current_value).strip() == "") and social_links[col]:
                df.at[idx, col] = social_links[col]

    df.to_excel(str(OUTPUT_FILE), index=False)
    print(f"Archivo generado: {OUTPUT_FILE}")


if __name__ == "__main__":
    main()