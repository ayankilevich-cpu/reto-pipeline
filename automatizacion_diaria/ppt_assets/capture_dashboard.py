from playwright.sync_api import sync_playwright
import time

BASE = "http://localhost:8515"
OUT = "/Users/alejandroyankilevich/Documents/MASTER DATA SCIENCE/Clases/RETO/automatizacion_diaria/ppt_assets"


def go_section(page, name):
    page.get_by_text(name, exact=True).click()
    time.sleep(2.5)


with sync_playwright() as p:
    browser = p.webkit.launch(headless=True)
    context = browser.new_context(viewport={"width": 1600, "height": 1000})
    page = context.new_page()

    page.goto(BASE, wait_until="domcontentloaded", timeout=60000)
    page.wait_for_selector("text=Usuario", timeout=30000)
    page.get_by_label("Usuario").fill("Admin")
    page.get_by_label("Contraseña").fill("2026")
    page.get_by_role("button", name="Ingresar").click()
    page.wait_for_selector("text=Panel general", timeout=60000)
    time.sleep(4)

    # 1 Panel general
    go_section(page, "Panel general")
    page.screenshot(path=f"{OUT}/screenshot_1_panel.png", full_page=True)

    # 2 Ranking
    go_section(page, "Ranking de medios")
    page.screenshot(path=f"{OUT}/screenshot_2_ranking.png", full_page=True)

    # 3 Anotacion
    go_section(page, "Anotación y validación")
    time.sleep(2)
    try:
        page.get_by_text("Anotación odio YouTube", exact=True).click()
        time.sleep(1.5)
    except Exception:
        pass
    page.screenshot(path=f"{OUT}/screenshot_3_anotacion.png", full_page=True)

    # 4 Art 510
    go_section(page, "Análisis Art. 510")
    page.screenshot(path=f"{OUT}/screenshot_4_art510.png", full_page=True)

    browser.close()

print("capturas_ok")
