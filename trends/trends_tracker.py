import os
import sys
import time
import re
import datetime
import sqlite3
import asyncio
import json
import argparse
import csv
from pathlib import Path

# Setup paths
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "market_trends.db"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
STOP_FLAG_FILE = DATA_DIR / "TRACKER_STOP.flag"
DEBUG_DIR = DATA_DIR / "debug"
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
from idealista_scraper.config import VIEWPORT_SIZES, USER_AGENTS, BROWSER_ROTATION_POOL
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
    """Extracts the leading number from the H1 element with improved robustness."""
    try:
        # Try multiple common selectors for the title count
        selectors = ["h1", ".main-info h1", "#h1-container h1", ".h1-container h1"]
        h1_text = ""
        
        for selector in selectors:
            try:
                # 15s timeout to account for high network load
                h1_handle = await page.wait_for_selector(selector, timeout=15000)
                if h1_handle:
                    h1_text = await h1_handle.inner_text()
                    if h1_text:
                        break
            except:
                continue
        
        if not h1_text:
            return 0
            
        # Match numbers with potential dots/commas as thousands separators
        match = re.search(r'([0-9.,]+)', h1_text)
        if match:
            clean_num = match.group(1).replace(".", "").replace(",", "")
            if clean_num.isdigit():
                return int(clean_num)
    except Exception as e:
        pass
    return 0

async def detect_block(page):
    """Detects if we are hard-blocked or on a specialized captcha page."""
    try:
        title = (await page.title() or "").lower()
        # Simplified block detection to avoid expensive content extraction if possible
        block_keywords = ["pardon our interruption", "captcha", "access denied", "forbidden", "uso indebido", "bloqueado"]
        if any(kw in title for kw in block_keywords):
            return True
            
        # Check for the datadome iframe or specific text
        is_blocked = await page.evaluate("""() => {
            const text = document.body ? document.body.innerText.toLowerCase() : '';
            return text.includes('uso indebido') || 
                   text.includes('pardon our interruption') || 
                   !!document.querySelector('iframe[src*="captcha-delivery.com"]');
        }""")
        return is_blocked
    except:
        return False

async def take_debug_screenshot(page, province, zone):
    """Captures a screenshot when 0 properties are found to diagnose rendering/blocks."""
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_zone = re.sub(r'[^a-zA-Z0-9]', '_', zone)
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        filename = f"0_props_{province}_{safe_zone}_{timestamp}.png"
        filepath = DEBUG_DIR / filename
        await page.screenshot(path=str(filepath))
        print(f"  📸 Debug screenshot saved: trends/data/debug/{filename}")
    except Exception as e:
        print(f"  ⚠️ Could not take debug screenshot: {e}")
    
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

async def record_exists_for_week(iso_year, iso_week, province, zone, operation):
    """Checks if a record already exists for this exact combination to avoid double scraping."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT 1 FROM inventory_trends 
            WHERE iso_year = ? AND iso_week = ? AND province = ? AND zone = ? AND operation = ?
        ''', (iso_year, iso_week, province, zone, operation))
        return cursor.fetchone() is not None
    except Exception as e:
        print(f"DB Check Error: {e}")
        return False
    finally:
        conn.close()

def save_checkpoint(index, iso_year, iso_week):
    """Saves the current progress index to resume later."""
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "last_index": index,
                "iso_year": iso_year,
                "iso_week": iso_week
            }, f)
    except Exception as e:
        print(f"Failed to save checkpoint: {e}")

def load_checkpoint():
    """Loads the checkpoint. Returns (last_index, iso_year, iso_week) or (0, None, None)."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get("last_index", 0), data.get("iso_year"), data.get("iso_week")
        except:
            pass
    return 0, None, None

def auto_export_csv():
    """Generates a CSV export of the entire SQLite DB to disk."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # VENTA
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, operation, total_properties FROM inventory_trends WHERE operation = 'venta' ORDER BY id DESC")
        rows_venta = cursor.fetchall()
        
        # ALQUILER
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, operation, total_properties FROM inventory_trends WHERE operation = 'alquiler' ORDER BY id DESC")
        rows_alquiler = cursor.fetchall()

        conn.close()
        
        ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        file_venta = DATA_DIR / f"market_trends_venta_{ts}.csv"
        file_alquiler = DATA_DIR / f"market_trends_alquiler_{ts}.csv"
        
        headers = ['Fecha', 'Año ISO', 'Semana ISO', 'Provincia', 'Zona', 'Operación', 'Total Propiedades']

        if rows_venta:
            with open(file_venta, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows_venta)
            print(f"✅ Auto-exported {len(rows_venta)} records to {file_venta.name}")

        if rows_alquiler:
            with open(file_alquiler, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows_alquiler)
            print(f"✅ Auto-exported {len(rows_alquiler)} records to {file_alquiler.name}")
            
    except Exception as e:
        print(f"Error auto-exporting CSV: {e}")

async def run_tracker(resume=False, headless=False):
    print(f"Starting Robust Market Trends Tracker (Resume: {resume}, Headless: {headless})...", flush=True)
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
    if resume:
        last_idx, cp_year, cp_week = load_checkpoint()
        if cp_year == iso_year and cp_week == iso_week:
            start_index = last_idx
            print(f"Resuming from index {start_index} for Week {iso_week}")
        else:
            print("Checkpoint is from a previous week. Starting fresh.")
    
    urls_len = len(urls_data)
    
    # Remove old stop flag if exists
    if STOP_FLAG_FILE.exists():
        try: STOP_FLAG_FILE.unlink()
        except: pass
    
    while start_index < urls_len:
        if STOP_FLAG_FILE.exists():
            print("🔴 Stop flag detected. Halting outer loop.")
            break
        
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
                "headless": headless,
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
                
                page = context.pages[0] if context.pages else await context.new_page()
                
                # Close extra tabs potentially restored by portable browsers (like Opera)
                for p in context.pages:
                    if p != page:
                        try: await p.close()
                        except: pass
                
                # PROCESS URLs
                scan_idx = start_index
                for k, data in enumerate(urls_data[start_index:], start_index):
                    if STOP_FLAG_FILE.exists():
                        print("🔴 Stop flag detected. Halting inner loop.")
                        break
                        
                    province, zone, url, operation = data
                    print(f"[{scan_idx+1}/{urls_len}] Tracking {province} ({zone}) - {operation.upper()}...")
                    
                    # Deduplication Check
                    if await record_exists_for_week(iso_year, iso_week, province, zone, operation):
                        print(f"  -> Skipping. Data already exists for Week {iso_week}.")
                        scan_idx += 1
                        continue
                    
                    try:
                        await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(2.5, 5.5)) # Delay to mimic human
                        
                        # Enhanced block detection
                        if await detect_block(page):
                            print(f"WARN: BLOCK detected on {url}. Marking profile blocked.")
                            mark_current_profile_blocked()
                            raise RuntimeError("CAPTCHA_CRITICAL_BLOCK")

                        total_properties = await extract_h1_number(page)
                        
                        if total_properties == 0:
                            # Verify if it's really 0 or a load failure
                            await take_debug_screenshot(page, province, zone)
                            
                        print(f"  -> Found {total_properties} properties.")
                        
                        if total_properties >= 0:
                            await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, total_properties)
                            
                        scan_idx += 1
                        
                    except Exception as e:
                        if "CAPTCHA_CRITICAL_BLOCK" in str(e):
                            break # Exits the URL enumeration to restart rotation
                            
                        print(f"Error loading {url}: {e}")
                        scan_idx += 1 # proceed to next even on timeout
                        continue
                        
                    # Save Checkpoint every 20 urls
                    if scan_idx > 0 and scan_idx % 20 == 0:
                        print(f"💾 Saving Checkpoint at index {scan_idx}...")
                        save_checkpoint(scan_idx, iso_year, iso_week)
                        
                start_index = scan_idx # Updates outer loop progress
                
                if STOP_FLAG_FILE.exists():
                    break
            except Exception as e:
                print(f"CRITICAL: Browser instance failed: {e}")
                
            finally:
                if 'context' in locals():
                    await context.close()
                

    # Final checkpoint update
    save_checkpoint(start_index, iso_year, iso_week)
    if start_index >= urls_len:
        print("Market Trends Tracking Completed Full List!")
    else:
        print(f"Tracker Stopped at index {start_index}.")
        
    # Auto export to CSV locally at the end of run
    print("Initiating automatic database backup to CSV...", flush=True)
    auto_export_csv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    args = parser.parse_args()
    
    asyncio.run(run_tracker(resume=args.resume, headless=args.headless))
