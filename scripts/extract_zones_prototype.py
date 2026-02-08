import asyncio
import json
import random
import sys

# Windows Hack: Force UTF-8 output
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from playwright.async_api import async_playwright
try:
    from playwright_stealth import stealth
    HAS_STEALTH = True
except ImportError:
    print("WARNING: playwright_stealth not found! Using limited manual stealth.")
    HAS_STEALTH = False

# Manual fallback if stealth module missing
def generate_manual_stealth():
    return """
        try {
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            Object.defineProperty(navigator, 'languages', {get: () => ['es-ES', 'es', 'en-US', 'en']});
            Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3]});
        } catch (e) {}
    """

# A Coruña Province URL
URL = "https://www.idealista.com/venta-viviendas/a-coruna-provincia/"

# ==================== STEALTH UTILS (Copied from scraper_wrapper.py) ====================
# (Removed generate_stealth_script as we use stealth_async)

async def human_warmup_routine(page):
    print("STEALTH: Starting human warm-up routine...")
    try:
        # Step 1: Visit Google first
        await page.goto('https://www.google.es', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(1, 2))
        
        # Step 2: Random mouse movements
        for _ in range(random.randint(2, 5)):
            x = random.randint(100, 1000)
            y = random.randint(100, 600)
            await page.mouse.move(x, y, steps=random.randint(10, 25))
            await asyncio.sleep(random.uniform(0.1, 0.4))
            
        print("STEALTH: Warm-up complete")
    except Exception as e:
        print(f"STEALTH: Warm-up partial: {e}")

# ==================== MAIN LOGIC ====================

async def main():
    print(f"Starting extraction for: {URL}")
    
    # Path to profile
    from pathlib import Path
    profile_path = Path(__file__).parent.parent / "scraper" / "stealth_profile_firefox"
    print(f"Using Firefox profile: {profile_path}")
    
    async with async_playwright() as p:
        # Use Firefox persistent context
        # Do not pass viewport args as command line arguments, use viewport parameter
        
        try:
            print("Launching HEADED Firefox browser for stealth...")
            context = await p.firefox.launch_persistent_context(
                user_data_dir=str(profile_path),
                headless=False, # HEADED MODE
                args=[], # No extra args for now
                viewport={"width": 1920, "height": 1080}
            )
        except Exception as e:
            print(f"Failed to launch persistent context: {e}")
            return

        page = context.pages[0] if context.pages else await context.new_page()
        
        # Apply Stealth
        stealth_applied = False
        if HAS_STEALTH:
            try:
                # Try async first
                import inspect
                if inspect.iscoroutinefunction(stealth):
                    await stealth(page)
                else:
                    stealth(page)
                stealth_applied = True
                print("Stealth module applied.")
            except Exception as e:
                print(f"Stealth module failed: {e}")
        
        if not stealth_applied:
             await page.add_init_script(generate_manual_stealth())
             print("Manual stealth applied (fallback).")
        
        # 1. Warmup
        await human_warmup_routine(page)
        
        
        try:
            # 2. Navigate to Target
            print(f"Navigating to {URL}...")
            await asyncio.sleep(random.uniform(1, 3))
            
            response = await page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            
            # Wait for meaningful content (breadcrumb) to confirm access
            print("Waiting for breadcrumb navigation (sign of successful load)...")
            try:
                # Wait up to 2 minutes for user to solve captcha if needed
                await page.wait_for_selector('.breadcrumb-navigation-current-level', timeout=120000)
                title = await page.title()
                print(f"[OK] Access granted! Page Title: {title}")
            except Exception:
                print("[ERROR] Timed out waiting for page to load (Captcha not solved?).")
                title = await page.title()
                print(f"Final Title: {title}")
                await page.screenshot(path="blocked_timeout.png")
                return

            # 3. Handle Cookies
            try:
                await page.evaluate("""() => {
                    const btn = document.querySelector('#didomi-notice-agree-button');
                    if (btn) btn.click();
                }""")
                await asyncio.sleep(1)
            except:
                pass

            # 4. Extract Zones
            print("Extracting zones from DOM...")
            zones = await page.evaluate('''() => {
                const extracted = [];
                const items = document.querySelectorAll('li.breadcrumb-dropdown-subitem-element-list');
                
                items.forEach(item => {
                    const id = item.getAttribute('data-location-id');
                    const link = item.querySelector('a');
                    
                    if (id && link) {
                        extracted.push({
                            id: id,
                            name: link.innerText.trim(),
                            href: link.getAttribute('href')
                        });
                    }
                });
                return extracted;
            }''')
            
            # Filter for A Coruña (ID 15)
            filtered_zones = [z for z in zones if "0-EU-ES-15" in z['id'].upper()]
            
            print(f"Total items found: {len(zones)}")
            print(f"Items matching A Coruña (prefix 15): {len(filtered_zones)}")
            
            if filtered_zones:
                print("\nSample (First 5):")
                print(json.dumps(filtered_zones[:5], indent=2, ensure_ascii=False))
                
                # Check for "A Barcala" specific verification
                barcala = next((z for z in filtered_zones if "A Barcala" in z['name']), None)
                if barcala:
                    print(f"\n[OK] Verification: Found 'A Barcala': {barcala}")
                else:
                    print("\n[WARN] Warning: 'A Barcala' NOT found.")
                    
                # Export to JSON
                with open("a_coruna_zones.json", "w", encoding="utf-8") as f:
                    json.dump(filtered_zones, f, indent=2, ensure_ascii=False)
                print("Saved to a_coruna_zones.json")
            else:
                print("No zones matched.")
                # Debug dump
                content = await page.content()
                with open("debug_page.html", "w", encoding="utf-8") as f:
                    f.write(content)
                print("Dumped HTML to debug_page.html")
                
        except Exception as e:
            print(f"Error: {e}")
            await page.screenshot(path="error.png")
        finally:
            await context.close()

if __name__ == "__main__":
    asyncio.run(main())
