"""
Quick sample verification - tests 5 random provinces for both venta/alquiler
to confirm URL patterns work before presenting full inventory.
"""
import asyncio
import sys
import os
import random
from playwright.async_api import async_playwright

# Sample provinces to test
SAMPLE = [
    {"name": "Madrid", "slug": "madrid"},
    {"name": "Barcelona", "slug": "barcelona"},
    {"name": "Albacete", "slug": "albacete-provincia"},
    {"name": "Burgos", "slug": "burgos-provincia"},
    {"name": "Baleares", "slug": "balears-illes"},
]

async def check_url(page, url):
    try:
        response = await page.goto(url, wait_until='domcontentloaded', timeout=20000)
        await asyncio.sleep(0.5)
        return response.status if response else 0
    except Exception as e:
        return 500

async def main():
    print("Sample Province URL Verification")
    print("=" * 50)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        
        print("\n| Province | Venta Status | Alquiler Status |")
        print("|----------|--------------|-----------------|")
        
        for prov in SAMPLE:
            venta_url = f"https://www.idealista.com/venta-viviendas/{prov['slug']}/"
            alquiler_url = f"https://www.idealista.com/alquiler-viviendas/{prov['slug']}/"
            
            venta_code = await check_url(page, venta_url)
            await asyncio.sleep(2)
            alquiler_code = await check_url(page, alquiler_url)
            await asyncio.sleep(2)
            
            v_status = "✅ 200" if venta_code == 200 else f"❌ {venta_code}"
            a_status = "✅ 200" if alquiler_code == 200 else f"❌ {alquiler_code}"
            
            print(f"| {prov['name']} | {v_status} | {a_status} |")
            sys.stdout.flush()
        
        await browser.close()
    
    print("\nSample verification complete.")

if __name__ == "__main__":
    asyncio.run(main())
