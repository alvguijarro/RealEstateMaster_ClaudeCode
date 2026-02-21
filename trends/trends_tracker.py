import os
import sys
import time
import re
import datetime
import sqlite3
import asyncio
from pathlib import Path

# Setup paths
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "market_trends.db"
MAPPING_FILE = PROJECT_ROOT / "scraper" / "documentation" / "province_urls_mapping.md"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
# We will use vanilla playwright to avoid dependency issues
from playwright.async_api import async_playwright

# Parse the markdown mapping
def parse_mapping(file_path):
    """Parses the markdown file to extract a list of (Province, Zone, URL, Operation)"""
    urls_to_scrape = []
    current_operation = None
    current_province = None
    
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            if line.startswith("## 🏠 Alquiler"):
                current_operation = "alquiler"
            elif line.startswith("## 💰 Venta"):
                current_operation = "venta"
                
            elif line.startswith("|") and not line.startswith("| :---"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 4:
                    prov = parts[1].replace("**", "").strip()
                    zone = parts[2].strip()
                    url_md = parts[3].strip()
                    
                    if prov and prov.lower() != "provincia":
                        current_province = prov
                    
                    # Check if it's a markdown url / code block
                    url_match = re.search(r'`(https?://[^`]+)`', url_md)
                    if url_match:
                        url = url_match.group(1)
                        if current_province and current_operation:
                            urls_to_scrape.append((current_province, zone, url, current_operation))
                            
    return urls_to_scrape

async def extract_h1_number(page):
    """Extracts the leading number from the H1 element."""
    try:
        h1_text = await page.inner_text("h1", timeout=5000)
        # e.g., "1.240 casas y pisos" -> 1240
        # or "3 casas" -> 3
        match = re.search(r'^([0-9.,]+)', h1_text)
        if match:
            clean_num = match.group(1).replace(".", "").replace(",", "")
            return int(clean_num)
    except:
        return 0
    return 0
    
async def save_to_db(date_record, iso_year, iso_week, province, zone, operation, total):
    """Saves the extracted total to the SQLite database."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO inventory_trends 
            (date_record, iso_year, iso_week, province, zone, operation, total_properties)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (date_record, iso_year, iso_week, province, zone, operation, total))
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

async def run_tracker():
    print(f"Starting Market Trends Tracker...")
    os.makedirs(DATA_DIR, exist_ok=True)
    
    urls_data = parse_mapping(MAPPING_FILE)
    if not urls_data:
        print("Warning: No URLs found in mapping file.")
        return
        
    print(f"Found {len(urls_data)} URLs to track.")
    
    # Get current Date Data
    now = datetime.datetime.now()
    date_formatted = now.strftime("%d-%m-%Y")
    iso_year, iso_week, _ = now.isocalendar()
    
    import random
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = await context.new_page()
        
        try:
            for index, (province, zone, url, operation) in enumerate(urls_data):
                print(f"[{index+1}/{len(urls_data)}] Tracking {province} ({zone}) - {operation.upper()}...")
                
                try:
                    await page.goto(url, timeout=30000, wait_until="domcontentloaded")
                    await asyncio.sleep(random.uniform(2.0, 4.0)) # Delay to mimic human
                    
                    # CAPTCHA check
                    title = await page.title()
                    if "Pardon Our Interruption" in title or "Captcha" in title:
                        print(f"CAPTCHA detected on {url}.")
                        print("Waiting for manual resolution or 20s timeout...")
                        # Just wait a bit and hope user solves it or it passes, this is a lightweight script
                        await asyncio.sleep(20)
                        
                        # Check again
                        title = await page.title()
                        if "Pardon Our Interruption" in title or "Captcha" in title:
                            print("Still blocked. Skipping and waiting 5 mins before next...")
                            await asyncio.sleep(300)
                            continue

                    total_properties = await extract_h1_number(page)
                    
                    print(f"  -> Found {total_properties} properties.")
                    if total_properties >= 0:
                        await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, total_properties)
                        
                except Exception as e:
                    print(f"Error loading {url}: {e}")
                
        finally:
            await browser.close()
                

if __name__ == "__main__":
    asyncio.run(run_tracker())
    print("Market Trends Tracking Completed!")
