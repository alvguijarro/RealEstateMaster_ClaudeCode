import os
import asyncio
from playwright.async_api import async_playwright

async def main():
    print(f"HOME: {os.environ.get('HOME')}")
    print(f"USERPROFILE: {os.environ.get('USERPROFILE')}")
    
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto("https://www.google.com")
            print(f"Page title: {await page.title()}")
            await browser.close()
            print("Successfully launched browser and opened page!")
    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    asyncio.run(main())
