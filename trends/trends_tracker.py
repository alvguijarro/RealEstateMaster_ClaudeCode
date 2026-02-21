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
    
SCRAPER_DIR = PROJECT_ROOT / "scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from playwright.async_api import async_playwright
import random

# Import stealth and captcha utilities from main scraper
from app.scraper_wrapper import get_browser_executable_path
from idealista_scraper.config import VIEWPORT_SIZES, USER_AGENTS, BROWSER_ROTATION_POOL, DEEP_STEALTH_SCRIPT
from idealista_scraper.utils import solve_captcha_advanced
from update_urls import rotate_identity, mark_current_profile_blocked, get_random_gpu, generate_stealth_script, get_profile_dir

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
    print(f"Starting Robust Market Trends Tracker...")
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
    
    start_index = 0
    urls_len = len(urls_data)
    
    while start_index < urls_len:
        # IDENTITY ROTATION
        profile_config, wait_time = rotate_identity()
        if wait_time > 0:
            print(f"WARN: All profiles in cooldown. Waiting {int(wait_time/60)}m...")
            await asyncio.sleep(wait_time)
            continue
            
        profile_dir = get_profile_dir(profile_config["index"])
        os.makedirs(profile_dir, exist_ok=True)
        
        async with async_playwright() as pw:
            print(f"INFO: Launching persistent browser with profile: {profile_config['name']}...")
            
            browser_args = [
                "--start-maximized",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--no-first-run",
            ]
            
            exe_path = get_browser_executable_path(profile_config.get("channel"))
            viewport = random.choice(VIEWPORT_SIZES)
            
            engine_name = profile_config.get("engine", "chromium")
            browser_launcher = getattr(pw, engine_name)
            
            launch_options = {
                "user_data_dir": profile_dir,
                "headless": False, # Keep visible for now
                "viewport": {"width": viewport[0], "height": viewport[1]},
                "user_agent": random.choice(USER_AGENTS)
            }
            
            if engine_name == "chromium":
                launch_options["args"] = browser_args
                launch_options["ignore_default_args"] = ["--enable-automation"]
                if profile_config.get("channel"):
                    launch_options["channel"] = profile_config["channel"]
                if exe_path:
                    launch_options["executable_path"] = exe_path
            elif engine_name == "firefox":
                if exe_path:
                    launch_options["executable_path"] = exe_path
                launch_options["firefox_user_prefs"] = {
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False,
                }
                
            try:
                context = await browser_launcher.launch_persistent_context(**launch_options)
                
                # ADVANCED GPU/DEEP STEALTH
                _GPU_VENDOR, _GPU_RENDERER = get_random_gpu()
                stealth_script = generate_stealth_script().replace('{_GPU_VENDOR}', _GPU_VENDOR).replace('{_GPU_RENDERER}', _GPU_RENDERER)
                await context.add_init_script(stealth_script)
                await context.add_init_script(DEEP_STEALTH_SCRIPT)
                
                page = context.pages[0] if context.pages else await context.new_page()
                
                # PROCESS URLs
                scan_idx = start_index
                for k, data in enumerate(urls_data[start_index:], start_index):
                    province, zone, url, operation = data
                    print(f"[{scan_idx+1}/{urls_len}] Tracking {province} ({zone}) - {operation.upper()}...")
                    
                    try:
                        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(2.5, 5.5)) # Delay to mimic human
                        
                        # CAPTCHA check
                        title = await page.title()
                        if "Pardon Our Interruption" in title or "Captcha" in title:
                            print(f"WARN: CAPTCHA detected on {url}.")
                            resolved = await solve_captcha_advanced(page)
                            if not resolved:
                                print("ERR: Could not resolve Captcha. Burning profile and rotating...")
                                mark_current_profile_blocked()
                                await browser.close()
                                break # Break inner loop, will rotate profile in outer loop
                            
                            # If resolved, wait a bit for Cloudflare to redirect
                            await asyncio.sleep(5)
                            
                        # Double check we are on a listing page
                        title = await page.title()
                        if "Pardon" in title or "Captcha" in title:
                            print("ERR: Still blocked after resolution attempt. Burning profile.")
                            mark_current_profile_blocked()
                            break

                        total_properties = await extract_h1_number(page)
                        print(f"  -> Found {total_properties} properties.")
                        
                        if total_properties >= 0:
                            await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, total_properties)
                            
                        scan_idx += 1
                        
                    except Exception as e:
                        print(f"Error loading {url}: {e}")
                        scan_idx += 1 # proceed to next even on timeout
                        continue
                        
                start_index = scan_idx # Updates outer loop progress
                
            except Exception as e:
                print(f"CRITICAL: Browser instance failed: {e}")
                
            finally:
                if 'context' in locals():
                    await context.close()
                

if __name__ == "__main__":
    asyncio.run(run_tracker())
    print("Market Trends Tracking Completed!")
