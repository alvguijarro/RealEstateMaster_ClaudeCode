import os
import asyncio
from playwright.async_api import async_playwright

async def main():
    # Ensure HOME is set for this process
    os.environ["HOME"] = os.environ.get("USERPROFILE", "")
    
    async with async_playwright() as p:
        # Launch browser
        browser = await p.chromium.launch()
        # Use stealthy context if possible, but for a test, simple is fine
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        # Go to a test page
        url = "https://www.google.com/search?q=idealista+madrid"
        print(f"Navigating to {url}...")
        await page.goto(url, wait_until="domcontentloaded")
        
        # Take a screenshot
        screenshot_path = os.path.abspath("browser_verification.png")
        await page.screenshot(path=screenshot_path)
        print(f"Screenshot saved to {screenshot_path}")
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
