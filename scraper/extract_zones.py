import asyncio
import json
import os
import sys
import random

# Add current directory to path to find idealista_scraper
sys.path.append(os.path.join(os.getcwd(), 'scraper'))

from playwright.async_api import async_playwright
# Import solve_slider_captcha if possible
try:
    from idealista_scraper.utils import solve_slider_captcha
except ImportError:
    solve_slider_captcha = None

async def run():
    async with async_playwright() as p:
        # Use a persistent context to look more like a real user
        user_data_dir = os.path.join(os.getcwd(), "scraper", "stealth_profile_temp_02")
        
        # Launching Firefox as requested
        context = await p.firefox.launch_persistent_context(
            user_data_dir,
            headless=False,
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
            viewport={"width": 1920, "height": 1080}
        )
        
        # Add stealth scripts
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = context.pages[0] if context.pages else await context.new_page()
        
        url = "https://www.idealista.com/venta-viviendas/a-coruna-provincia/"
        print(f"Navigating to {url}...")
        
        try:
            # First go to home page to maybe drop some cookies
            await page.goto("https://www.idealista.com/", wait_until="domcontentloaded", timeout=60000)
            await asyncio.sleep(2)
            
            response = await page.goto(url, wait_until="load", timeout=90000)
            print(f"Page response status: {response.status if response else 'N/A'}")
            
            # Check for CAPTCHA
            title = await page.title()
            if "attention" in title.lower() or "robot" in title.lower() or "captcha" in title.lower() or (response and response.status == 403):
                print("CAPTCHA or Block detected (403)!")
                if solve_slider_captcha:
                    print("Attempting to solve slider...")
                    if await solve_slider_captcha(page):
                        print("Slider solve attempt finished.")
                        await asyncio.sleep(5)
                
                # If still blocked, wait for manual help
                print("Waiting 15 seconds for potential manual bypass/solve...")
                await asyncio.sleep(15)

            # Wait for content to load
            await asyncio.sleep(5)
            
            # Hover to trigger the breadcrumb dropdown
            # Idealista structure for breadcrumb dropdowns:
            # <li> with class "breadcrumb-dropdown-element-highlighted" usually contains the current location
            try:
                trigger = await page.query_selector(".breadcrumb-dropdown-element-highlighted")
                if trigger:
                    await trigger.hover()
                    print("Hovered over breadcrumb.")
                    await asyncio.sleep(2)
            except:
                pass

            # Extract data
            zones = await page.evaluate('''() => {
                const listContainer = document.querySelector('.breadcrumb-dropdown-subitem-list');
                if (!listContainer) return null;
                
                const items = Array.from(listContainer.querySelectorAll('li.breadcrumb-dropdown-subitem-element-list'));
                return items.map(li => {
                    const link = li.querySelector('a');
                    return {
                        'data-location-id': li.getAttribute('data-location-id'),
                        'href': link ? link.getAttribute('href') : null,
                        'name': link ? link.innerText.trim() : null
                    };
                }).filter(item => item.name);
            }''')
            
            if zones:
                print("SUCCESS: Extracted zones:")
                print(json.dumps(zones, indent=2, ensure_ascii=False))
            else:
                print("No zones found. Saving debug info.")
                await page.screenshot(path="scraper/debug.png")
                content = await page.content()
                with open("scraper/debug.html", "w", encoding="utf-8") as f:
                    f.write(content)
                print("Debug info saved.")

        except Exception as e:
            print(f"Exception: {e}")
        finally:
            await context.close()

if __name__ == "__main__":
    asyncio.run(run())
