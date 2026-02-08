import asyncio
import json
import os
import sys
import random
import re
from pathlib import Path
from playwright.async_api import async_playwright

# Add 'scraper' to sys.path to import project modules
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR / "scraper"))

try:
    from idealista_scraper.utils import solve_slider_captcha, log
except ImportError:
    def log(kind, msg): print(f"[{kind}] {msg}")
    solve_slider_captcha = None

# Windows Hack: Force UTF-8 output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

# Config
MD_FILE = ROOT_DIR / "scraper" / "documentation" / "idealista_urls_venta.md"
OUTPUT_FILE = ROOT_DIR / "scraper" / "province_zones_complete.json"
PROFILE_PATH = ROOT_DIR / "scraper" / "stealth_profile_extract_zones"

def parse_provinces_from_md(file_path):
    provinces = []
    if not file_path.exists():
        return provinces
    
    content = file_path.read_text(encoding='utf-8')
    # Match table rows: | ID | Name | URL |
    # Example: | 0-EU-ES-15 | A Coruña | [https://...](https://...) |
    matches = re.finditer(r'\| (0-EU-ES-\d+) \| ([^|]+) \| \[.*?\]\((.*?)\) \|', content)
    for m in matches:
        provinces.append({
            "id": m.group(1),
            "name": m.group(2).strip(),
            "url": m.group(3).strip()
        })
    return provinces

async def extract_zones(page, province_name):
    # Check for CAPTCHA/Block
    title = (await page.title()).lower()
    content = (await page.content()).lower()
    if "attention" in title or "robot" in title or "captcha" in title or "verificación" in title or "verificación" in content:
        log("WARN", f"Verification/CAPTCHA detected for {province_name}. Trying automatic solve...")
        if solve_slider_captcha:
            await solve_slider_captcha(page)
            await asyncio.sleep(5)
        
        # If still blocked, wait for manual solve (as it's non-headless)
        log("INFO", "Waiting up to 60s for manual CAPTCHA/Verification resolution if needed...")
        for _ in range(12):
            await asyncio.sleep(5)
            new_title = (await page.title()).lower()
            if "idealista" in new_title and "verificación" not in new_title and "attention" not in new_title:
                log("OK", "Bypassed block!")
                break


    # Hover logic to reveal dropdown
    trigger_selector = ".breadcrumb-dropdown-element-highlighted"
    try:
        if await page.query_selector(trigger_selector):
            await page.hover(trigger_selector)
            await asyncio.sleep(2)
    except:
        pass

    # Extract
    zones = await page.evaluate('''() => {
        const listContainer = document.querySelector('.breadcrumb-dropdown-subitem-list');
        if (!listContainer) return null;
        
        const items = Array.from(listContainer.querySelectorAll('li.breadcrumb-dropdown-subitem-element-list'));
        return items.map(li => {
            const link = li.querySelector('a');
            return {
                'id': li.getAttribute('data-location-id'),
                'name': link ? link.innerText.trim() : null,
                'href': link ? link.getAttribute('href') : null
            };
        }).filter(item => item.name);
    }''')
    return zones

async def run_extraction():
    provinces = parse_provinces_from_md(MD_FILE)
    if not provinces:
        log("ERR", f"No provinces found in {MD_FILE}")
        return

    # Load existing to resume
    results = {}
    if OUTPUT_FILE.exists():
        try:
            results = json.loads(OUTPUT_FILE.read_text(encoding='utf-8'))
            log("INFO", f"Loaded {len(results)} provinces from checkpoint.")
        except:
            pass

    async with async_playwright() as p:
        log("INFO", "Launching Firefox (non-headless)...")
        context = await p.firefox.launch_persistent_context(
            user_data_dir=str(PROFILE_PATH),
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            viewport={"width": 1280, "height": 800}
        )
        
        # Add stealth scripts
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = context.pages[0] if context.pages else await context.new_page()

        for i, prov in enumerate(provinces):
            name = prov['name']
            url = prov['url']
            
            if name in results and results[name]:
                log("INFO", f"[{i+1}/{len(provinces)}] Skipping {name} (already extracted)")
                continue

            log("INFO", f"[{i+1}/{len(provinces)}] Processing {name}...")
            
            try:
                # Add random sleep before navigation
                await asyncio.sleep(random.uniform(3, 7))
                
                await page.goto(url, wait_until="load", timeout=90000)
                zones = await extract_zones(page, name)
                
                if zones:
                    results[name] = {
                        "id": prov['id'],
                        "zones": zones
                    }
                    log("OK", f"Extracted {len(zones)} zones for {name}")
                else:
                    log("WARN", f"No zones found for {name}. Check debug info.")
                    # Optional: Take screenshot on failure
                    await page.screenshot(path=f"scraper/fail_{name}.png")
            
            except Exception as e:
                log("ERR", f"Failed {name}: {e}")
            
            # Save progress after each province
            OUTPUT_FILE.write_text(json.dumps(results, indent=2, ensure_ascii=False), encoding='utf-8')

        await context.close()
    
    log("OK", "Zone extraction complete!")

if __name__ == "__main__":
    asyncio.run(run_extraction())
