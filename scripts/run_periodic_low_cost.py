"""
Periodic Low-Cost Scraper Runner

Iterates through all 52 Spanish provinces, launching the Scraper Tool for each
using the URLs from low_cost_provinces.json.

Features:
- Runs in Fast Mode for efficiency.
- Targets properties < 300,000€.
- Outputs one Excel file per province: idealista_[Province]_lowcost.xlsx
- Includes resilience: waits and retries on block, moves to next if persistent.
"""
import json
import subprocess
import sys
import time
import os
from pathlib import Path
from datetime import datetime

# Paths
SCRIPT_DIR = Path(__file__).parent.parent / "scraper"
PROVINCES_FILE = SCRIPT_DIR / "low_cost_provinces.json"
OUTPUT_DIR = SCRIPT_DIR / "salidas"
LOG_FILE = OUTPUT_DIR / f"periodic_lowcost_log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

# Config
DELAY_BETWEEN_PROVINCES = (30, 60)  # seconds
MAX_RETRIES_PER_PROVINCE = 2
BLOCK_WAIT_TIME = 900  # 15 minutes

def log(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def run_scraper(province_name: str, url: str) -> bool:
    """
    Launch the Scraper Tool for a single province URL.
    Returns True on success, False on failure/block.
    """
    # Build the command
    # We need to run the main scraper. The existing scraper_wrapper expects a browser.
    # For a CLI batch run, we need to use the server's API or a dedicated runner.
    # Let's use the existing server API by calling it via curl/requests,
    # OR we can invoke the scraper directly via its internal async function.
    
    # For simplicity in V1, I'll use subprocess to call a new dedicated script
    # that wraps the scraper logic. For now, let's invoke server.py endpoint via requests.
    # Actually, the cleanest way is to directly invoke the scraper_wrapper.
    # But that requires async handling that's complex for subprocess.
    
    # Let's use the /api/start endpoint of the running server if available.
    # If server is not running, we skip.
    
    import requests
    
    try:
        # Check if server is running
        health = requests.get("http://localhost:5000/health", timeout=3)
        if health.status_code != 200:
            log(f"[WARN] Scraper server not running. Skipping {province_name}.")
            return False
    except:
        log(f"[WARN] Scraper server not reachable. Skipping {province_name}.")
        return False
    
    # Call start endpoint
    payload = {
        "url": url,
        "mode": "fast",  # Using Fast mode as requested
        "max_pages": 100  # Reasonable limit for low-cost search
    }
    
    try:
        log(f"Starting scrape for {province_name}...")
        resp = requests.post("http://localhost:5000/api/start", json=payload, timeout=10)
        
        if resp.status_code == 200:
            log(f"[OK] Scrape initiated for {province_name}.")
            
            # Poll for completion
            # The server doesn't have a dedicated "wait for completion" endpoint.
            # We'll poll the status endpoint.
            while True:
                time.sleep(10)
                try:
                    status_resp = requests.get("http://localhost:5000/api/status", timeout=5)
                    if status_resp.status_code == 200:
                        status_data = status_resp.json()
                        current_status = status_data.get("status", "")
                        
                        if current_status == "idle" or current_status == "completed":
                            log(f"[OK] Scrape completed for {province_name}.")
                            return True
                        elif current_status == "blocked" or current_status == "captcha":
                            log(f"[WARN] Block/CAPTCHA detected for {province_name}.")
                            return False
                        # else: still running, continue polling
                    else:
                        log(f"[WARN] Status check failed for {province_name}.")
                        return False
                except:
                    log(f"[WARN] Lost connection during scrape for {province_name}.")
                    return False
        else:
            log(f"[ERR] Failed to start scrape for {province_name}: {resp.text}")
            return False
            
    except Exception as e:
        log(f"[ERR] Exception during scrape for {province_name}: {e}")
        return False

def main():
    log("=" * 60)
    log("PERIODIC LOW-COST SCRAPER - Starting")
    log("=" * 60)
    
    # Load provinces
    if not PROVINCES_FILE.exists():
        log(f"[ERR] Provinces file not found: {PROVINCES_FILE}")
        sys.exit(1)
    
    with open(PROVINCES_FILE, "r", encoding="utf-8") as f:
        provinces = json.load(f)
    
    log(f"Loaded {len(provinces)} provinces.")
    
    success_count = 0
    fail_count = 0
    
    for i, prov in enumerate(provinces, 1):
        name = prov["name"]
        url = prov["url"]
        
        log(f"\n[{i}/{len(provinces)}] Processing: {name}")
        
        retries = 0
        success = False
        
        while retries < MAX_RETRIES_PER_PROVINCE and not success:
            success = run_scraper(name, url)
            
            if not success:
                retries += 1
                if retries < MAX_RETRIES_PER_PROVINCE:
                    log(f"[WARN] Retry {retries}/{MAX_RETRIES_PER_PROVINCE} for {name} after {BLOCK_WAIT_TIME}s...")
                    time.sleep(BLOCK_WAIT_TIME)
        
        if success:
            success_count += 1
        else:
            fail_count += 1
            log(f"[ERR] Failed after {MAX_RETRIES_PER_PROVINCE} retries: {name}")
        
        # Delay between provinces
        if i < len(provinces):
            import random
            delay = random.randint(*DELAY_BETWEEN_PROVINCES)
            log(f"Waiting {delay}s before next province...")
            time.sleep(delay)
    
    log("\n" + "=" * 60)
    log(f"PERIODIC SCRAPER COMPLETE")
    log(f"Success: {success_count} | Failed: {fail_count}")
    log("=" * 60)

if __name__ == "__main__":
    main()
