import os
import asyncio
from playwright.async_api import async_playwright

async def main():
    os.environ["HOME"] = os.environ.get("USERPROFILE", "")
    
    # Import the stealth script generator from the wrapper
    import sys
    sys.path.append(os.path.abspath("scraper/app"))
    from scraper_wrapper import generate_stealth_script
    
    async with async_playwright() as p:
        # Launch browser (non-headless so we can see if it worked, though here headless is fine)
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        
        # Inject the script
        stealth_script = generate_stealth_script()
        await context.add_init_script(stealth_script)
        
        page = await context.new_page()
        
        # Test 1: Bot detection test site
        print("Testing on bot.sannysoft.com...")
        await page.goto("https://bot.sannysoft.com", wait_until="networkidle")
        await page.screenshot(path="stealth_test_sannysoft.png", full_page=True)
        
        # Test 2: WebGL Check (FPID)
        print("Testing on fingerprint.com/products/bot-detection/...")
        await page.goto("https://fingerprint.com/products/bot-detection/", wait_until="networkidle")
        await page.screenshot(path="stealth_test_fingerprint.png")
        
        # Log some results from JS
        webdriver = await page.evaluate("navigator.webdriver")
        ua_data = await page.evaluate("navigator.userAgentData ? navigator.userAgentData.brands : 'N/A'")
        print(f"Navigator.webdriver: {webdriver}")
        print(f"UserAgentData Brands: {ua_data}")
        
        await browser.close()
        print("Tests complete. Check stealth_test_*.png")

if __name__ == "__main__":
    asyncio.run(main())
