"""
Comprehensive Province URL Verification Script
Tests all 52 Spanish provinces for both VENTA and ALQUILER operations.
Uses Playwright with stealth to avoid blocking.
Outputs a markdown inventory of results.

ENHANCED: Better rate-limit handling (429), longer delays, exponential backoff.
"""
import asyncio
import sys
import os
import json
import random
import unicodedata
import re
from pathlib import Path
from datetime import datetime
from playwright.async_api import async_playwright

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

# All 52 Spanish provinces
PROVINCES = [
    "Alava", "Albacete", "Alicante", "Almeria", "Avila", "Badajoz",
    "Baleares", "Barcelona", "Burgos", "Caceres", "Cadiz", "Castellon",
    "Ciudad Real", "Cordoba", "A Coruna", "Cuenca", "Girona", "Granada",
    "Guadalajara", "Guipuzcoa", "Huelva", "Huesca", "Jaen", "Leon",
    "Lleida", "La Rioja", "Lugo", "Madrid", "Malaga", "Murcia",
    "Navarra", "Ourense", "Asturias", "Palencia", "Las Palmas", "Pontevedra",
    "Salamanca", "Santa Cruz de Tenerife", "Cantabria", "Segovia", "Sevilla",
    "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "Valladolid",
    "Vizcaya", "Zamora", "Zaragoza", "Ceuta", "Melilla"
]

# Known special slug mappings (province name -> idealista slug)
# Based on Idealista's actual URL structure
SPECIAL_SLUGS = {
    "Alava": ["alava", "araba-alava"],
    "Baleares": ["balears-illes", "baleares", "illes-balears"],
    "A Coruna": ["a-coruna", "coruna-a"],
    "Guipuzcoa": ["gipuzkoa", "guipuzcoa"],
    "Vizcaya": ["bizkaia", "vizcaya"],
    "Navarra": ["navarra", "nafarroa"],
    "La Rioja": ["la-rioja", "rioja-la"],
    "Las Palmas": ["las-palmas", "palmas-las"],
    "Santa Cruz de Tenerife": ["santa-cruz-de-tenerife", "tenerife"],
    "Asturias": ["asturias", "asturias-provincia"],
    "Cantabria": ["cantabria", "cantabria-provincia"],
    "Murcia": ["murcia", "murcia-provincia"],
    "Ciudad Real": ["ciudad-real"],
}

# Stealth browser args
CHROMIUM_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-features=IsolateOrigins,site-per-process",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-dev-shm-usage",
    "--disable-accelerated-2d-canvas",
    "--no-first-run",
    "--disable-gpu",
    "--disable-extensions",
]

def slugify(text):
    """Convert province name to URL slug."""
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    return re.sub(r'[-\s]+', '-', text)

async def check_url(page, url, max_retries=3):
    """Check if a URL returns 200 OK with exponential backoff."""
    for attempt in range(max_retries):
        try:
            response = await page.goto(url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(1)
            
            content = await page.content()
            title = await page.title()
            status_code = response.status if response else 0
            
            # Rate limited - exponential backoff
            if status_code == 429:
                wait_time = (2 ** attempt) * 5  # 5, 10, 20 seconds
                print(f"  [Rate limited - waiting {wait_time}s...]", file=sys.stderr)
                await asyncio.sleep(wait_time)
                continue
            
            # Check for blocks
            if "uso indebido" in content.lower() or "access denied" in content.lower() or status_code == 403:
                return 403, "Blocked"
            
            # Check for 404
            if "no corresponde a ninguna página" in content.lower() or "404" in title or status_code == 404:
                return 404, "Not Found"
            
            # Check for valid listing page
            if status_code == 200:
                return 200, "OK"
                    
            return status_code, f"HTTP {status_code}"
            
        except Exception as e:
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
                continue
            return 500, str(e)[:50]
    
    return 429, "Rate limited"

async def find_working_url(page, province, operation):
    """
    Try different URL patterns to find a working one for a province.
    Returns (working_url, status_code, message) or (None, 0, error) if none work.
    """
    base_slug = slugify(province)
    
    # Build list of slugs to try
    slugs_to_try = [base_slug]
    
    # Add special mappings if available
    if province in SPECIAL_SLUGS:
        for special in SPECIAL_SLUGS[province]:
            if special not in slugs_to_try:
                slugs_to_try.append(special)
    
    # Also try -provincia suffix as last resort
    slugs_to_try.append(f"{base_slug}-provincia")
    
    # Build URL patterns
    if operation == "venta":
        patterns = [
            lambda s: f"https://www.idealista.com/venta-viviendas/{s}/",
        ]
    else:  # alquiler
        patterns = [
            lambda s: f"https://www.idealista.com/alquiler-viviendas/{s}/",
        ]
    
    last_error = "No patterns tried"
    
    for slug in slugs_to_try:
        for pattern in patterns:
            url = pattern(slug)
            code, msg = await check_url(page, url)
            
            if code == 200:
                return url, code, msg
            elif code == 403:
                # Blocked - longer wait
                print(f"  [Blocked on {slug}, waiting 10s...]", file=sys.stderr)
                await asyncio.sleep(10)
            elif code == 429:
                # Rate limited - wait longer
                print(f"  [429 on {slug}, waiting 15s...]", file=sys.stderr)
                await asyncio.sleep(15)
            
            last_error = f"{code}: {msg}"
            
            # Small delay between URL attempts
            await asyncio.sleep(random.uniform(2.0, 4.0))
    
    return None, 0, last_error

async def main():
    print(f"# Province URL Verification Report")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results = {
        "venta": {"success": [], "failed": []},
        "alquiler": {"success": [], "failed": []}
    }
    
    async with async_playwright() as p:
        browser = await p.chromium.launch_persistent_context(
            user_data_dir=os.path.join(os.path.dirname(__file__), 'verify_profile'),
            headless=True,
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
        """)
        
        # ============ VENTA ============
        print("## VENTA (Viviendas en Venta)")
        print()
        print("| Province | Status | Working URL |")
        print("|----------|--------|-------------|")
        
        for i, province in enumerate(PROVINCES):
            print(f"  [{i+1}/52] Testing {province}...", file=sys.stderr)
            url, code, msg = await find_working_url(page, province, "venta")
            
            if url and code == 200:
                results["venta"]["success"].append({"province": province, "url": url})
                print(f"| {province} | ✅ OK | `{url}` |")
            else:
                results["venta"]["failed"].append({"province": province, "error": msg})
                print(f"| {province} | ❌ {msg} | -- |")
            
            sys.stdout.flush()
            # Longer delay between provinces to avoid rate limiting
            await asyncio.sleep(random.uniform(3.0, 5.0))
        
        print()
        
        # ============ ALQUILER ============
        print("## ALQUILER (Viviendas en Alquiler)")
        print()
        print("| Province | Status | Working URL |")
        print("|----------|--------|-------------|")
        
        for i, province in enumerate(PROVINCES):
            print(f"  [{i+1}/52] Testing {province} (alquiler)...", file=sys.stderr)
            url, code, msg = await find_working_url(page, province, "alquiler")
            
            if url and code == 200:
                results["alquiler"]["success"].append({"province": province, "url": url})
                print(f"| {province} | ✅ OK | `{url}` |")
            else:
                results["alquiler"]["failed"].append({"province": province, "error": msg})
                print(f"| {province} | ❌ {msg} | -- |")
            
            sys.stdout.flush()
            await asyncio.sleep(random.uniform(3.0, 5.0))
        
        await browser.close()
    
    # ============ SUMMARY ============
    print()
    print("## Summary")
    print()
    print(f"**VENTA:** {len(results['venta']['success'])}/52 provinces verified")
    print(f"**ALQUILER:** {len(results['alquiler']['success'])}/52 provinces verified")
    print()
    
    if results["venta"]["failed"]:
        print("### Failed VENTA provinces (require manual check):")
        for item in results["venta"]["failed"]:
            print(f"- {item['province']}: {item['error']}")
        print()
    
    if results["alquiler"]["failed"]:
        print("### Failed ALQUILER provinces (require manual check):")
        for item in results["alquiler"]["failed"]:
            print(f"- {item['province']}: {item['error']}")
    
    # Save results to JSON for later use
    output_path = Path(__file__).parent / "province_url_inventory.json"
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print()
    print(f"Results saved to: {output_path}")

if __name__ == "__main__":
    asyncio.run(main())
