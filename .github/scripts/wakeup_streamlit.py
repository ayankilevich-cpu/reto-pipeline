"""Visita la app de Streamlit Cloud y hace clic en el botón de
'despertar' si la app está dormida. Si ya está despierta, no hace nada."""
from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

APP_URL = "https://proyectoreto.streamlit.app"
# Match parcial / insensible a mayúsculas (texto típico de Streamlit Cloud)
WAKE_BUTTON_TEXT = "get this app back up"


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        try:
            page.goto(APP_URL, timeout=60_000, wait_until="domcontentloaded")
            # Dar margen a que cargue el iframe / banner de hibernación
            page.wait_for_timeout(5_000)

            wake_button = page.get_by_text(WAKE_BUTTON_TEXT, exact=False)
            if wake_button.count() > 0:
                print("App dormida, haciendo clic en el botón de wake-up...")
                wake_button.first.click()
                # Esperar a que la app termine de levantar
                page.wait_for_timeout(30_000)
                print("Listo, la app debería estar despertando.")
            else:
                print("La app ya estaba despierta, no hizo falta hacer nada.")

            return 0
        except Exception as exc:
            print(f"Error visitando la app: {exc}", file=sys.stderr)
            return 1
        finally:
            browser.close()


if __name__ == "__main__":
    sys.exit(main())
