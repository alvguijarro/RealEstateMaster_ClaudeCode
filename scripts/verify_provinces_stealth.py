import asyncio
import sys
import os
import json
import random
import unicodedata
import re
from pathlib import Path
from playwright.async_api import async_playwright

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

PROVINCES_LIST = [
    {"id": "0-EU-ES-01", "name": "Alava"}, {"id": "0-EU-ES-02", "name": "Albacete"}, {"id": "0-EU-ES-03", "name": "Alicante"}, 
    {"id": "0-EU-ES-04", "name": "Almeria"}, {"id": "0-EU-ES-05", "name": "Avila"}, {"id": "0-EU-ES-06", "name": "Badajoz"},
    {"id": "0-EU-ES-07", "name": "Baleares"}, {"id": "0-EU-ES-08", "name": "Barcelona"}, {"id": "0-EU-ES-09", "name": "Burgos"},
    {"id": "0-EU-ES-10", "name": "Caceres"}, {"id": "0-EU-ES-11", "name": "Cadiz"}, {"id": "0-EU-ES-12", "name": "Castellon"},
    {"id": "0-EU-ES-13", "name": "Ciudad Real"}, {"id": "0-EU-ES-14", "name": "Cordoba"}, {"id": "0-EU-ES-15", "name": "A Coruna"},
    {"id": "0-EU-ES-16", "name": "Cuenca"}, {"id": "0-EU-ES-17", "name": "Girona"}, {"id": "0-EU-ES-18", "name": "Granada"},
    {"id": "0-EU-ES-19", "name": "Guadalajara"}, {"id": "0-EU-ES-20", "name": "Guipuzcoa"}, {"id": "0-EU-ES-21", "name": "Huelva"},
    {"id": "0-EU-ES-22", "name": "Huesca"}, {"id": "0-EU-ES-23", "name": "Jaen"}, {"id": "0-EU-ES-24", "name": "Leon"},
    {"id": "0-EU-ES-25", "name": "Lleida"}, {"id": "0-EU-ES-26", "name": "La Rioja"}, {"id": "0-EU-ES-27", "name": "Lugo"},
    {"id": "0-EU-ES-28", "name": "Madrid"}, {"id": "0-EU-ES-29", "name": "Malaga"}, {"id": "0-EU-ES-30", "name": "Murcia"},
    {"id": "0-EU-ES-31", "name": "Navarra"}, {"id": "0-EU-ES-32", "name": "Ourense"}, {"id": "0-EU-ES-33", "name": "Asturias"},
    {"id": "0-EU-ES-34", "name": "Palencia"}, {"id": "0-EU-ES-35", "name": "Las Palmas"}, {"id": "0-EU-ES-36", "name": "Pontevedra"},
    {"id": "0-EU-ES-37", "name": "Salamanca"}, {"id": "0-EU-ES-38", "name": "Santa Cruz de Tenerife"},
    {"id": "0-EU-ES-39", "name": "Cantabria"}, {"id": "0-EU-ES-40", "name": "Segovia"}, {"id": "0-EU-ES-41", "name": "Sevilla"},
    {"id": "0-EU-ES-42", "name": "Soria"}, {"id": "0-EU-ES-43", "name": "Tarragona"}, {"id": "0-EU-ES-44", "name": "Teruel"},
    {"id": "0-EU-ES-45", "name": "Toledo"}, {"id": "0-EU-ES-46", "name": "Valencia"}, {"id": "0-EU-ES-47", "name": "Valladolid"},
    {"id": "0-EU-ES-48", "name": "Vizcaya"}, {"id": "0-EU-ES-49", "name": "Zamora"}, {"id": "0-EU-ES-50", "name": "Zaragoza"},
    {"id": "0-EU-ES-51", "name": "Ceuta"}, {"id": "0-EU-ES-52", "name": "Melilla"}
]

# Stealth Args (from scraper_wrapper.py)
CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process", 
    "--disable-site-isolation-trials",
    "--disable-features=VizDisplayCompositor",
    "--disable-ipc-flooding-protection",
    "--enable-features=NetworkService,NetworkServiceInProcess",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--password-store=basic",
    "--use-mock-keychain",
    "--export-tagged-pdf",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--no-first-run",
    "--no-zygote",
    "--disable-gpu",
    "--hide-scrollbars",
    "--mute-audio",
    "--disable-background-networking",
    "--disable-background-timer-throttling",
    "--disable-backgrounding-occluded-windows",
    "--disable-breakpad",
    "--disable-component-extensions-with-background-pages",
    "--disable-extensions",
    "--disable-features=TranslateUI",
    "--disable-ipc-flooding-protection",
    "--disable-renderer-backgrounding",
    "--enable-features=NetworkService,NetworkServiceInProcess",
    "--force-color-profile=srgb",
    "--metrics-recording-only",
    "--password-store=basic",
    "--use-mock-keychain",
    "--export-tagged-pdf",
]

def slugify(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    return re.sub(r'[-\s]+', '-', text)

async def check_url(page, url):
    try:
        # Navigate without failing on error (handle manually)
        response = await page.goto(url, wait_until='domcontentloaded', timeout=15000)
        await asyncio.sleep(0.5)
        
        content = await page.content()
        title = await page.title()
        
        status_code = response.status if response else 0
        
        # Check for block
        if "uso indebido" in content.lower() or "access denied" in content.lower() or status_code == 403:
            return 403, "Blocked"
        
        # Check for 404 (Idealista soft 404)
        if "no corresponde a ninguna página" in content.lower() or "404" in title or status_code == 404:
            return 404, "Not Found"
            
        return 200, "OK"
        
    except Exception as e:
        return 500, str(e)

async def main():
    print("Initializing Stealth Browser...")
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=os.path.join(os.path.dirname(__file__), 'stealth_verify_profile'),
            headless=True, # Use headless for speed, args help stealth
            args=CHROMIUM_ARGS,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = browser.pages[0] if browser.pages else await browser.new_page()
        
        # Add stealth script
        await page.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            window.navigator.chrome = {runtime: {}};
            Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        """)
        
        print("| Province | Slug Tested | Alquiler (-provincia) | Alquiler (slug) | Recommendation |")
        print("|---|---|---|---|---|")
        
        for prov in PROVINCES_LIST:
            name = prov['name']
            slug = slugify(name)
            
            # Special case mappings if needed
            if slug == 'alava': slug = 'alava' # already done
            if slug == 'baleares': slug = 'balears-illes' # override
            if slug == 'vizcaya': slug = 'vizcaya' # check
            
            # Test 1: -provincia (ALQUILER)
            url_prov = f"https://www.idealista.com/alquiler-viviendas/{slug}-provincia/"
            code_prov, msg_prov = await check_url(page, url_prov)
            
            # Test 2: plain slug (ALQUILER)
            url_plain = f"https://www.idealista.com/alquiler-viviendas/{slug}/"
            code_plain, msg_plain = await check_url(page, url_plain)
            
            rec = "???"
            if code_prov == 200 and code_plain != 200:
                rec = f"{slug}-provincia"
            elif code_plain == 200:
                rec = f"{slug}" # Prefer shorter if both work, or plain if only plain works
            elif code_prov == 200:
                rec = f"{slug}-provincia" # Fallback
            else:
                rec = "MANUAL CHECK"
                
            status_prov = "✅" if code_prov == 200 else f"❌ {code_prov}"
            status_plain = "✅" if code_plain == 200 else f"❌ {code_plain}"
            
            print(f"| {name} | {slug} | {status_prov} | {status_plain} | {rec} |")
            sys.stdout.flush() # Ensure visible output
            
            # Small random delay
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
