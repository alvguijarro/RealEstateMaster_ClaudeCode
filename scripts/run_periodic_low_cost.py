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
import argparse
from pathlib import Path
from datetime import datetime

# Add project root to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))
from scraper.idealista_scraper.nordvpn import rotate_ip

# Paths
SCRIPT_DIR = Path(__file__).parent.parent / "scraper"
PROVINCES_FILE = SCRIPT_DIR / "low_cost_provinces.json"
OUTPUT_DIR = SCRIPT_DIR / "salidas"
LOG_FILE = OUTPUT_DIR / f"periodic_lowcost_log_{datetime.now().strftime('%Y%m%d_%H%M')}.txt"

# Config
DELAY_BETWEEN_PROVINCES = (30, 60)  # seconds
MAX_RETRIES_PER_PROVINCE = 2
BLOCK_WAIT_TIME = 900  # 15 minutes
NORDVPN_ROTATE_EVERY = 3 # Rotate IP every 3 provinces if enabled

# Signal flags
STOP_FLAG = SCRIPT_DIR / "PERIODIC_STOP.flag"
PAUSE_FLAG = SCRIPT_DIR / "PERIODIC_PAUSE.flag"

def check_signals():
    """Check for pause/stop flags."""
    if STOP_FLAG.exists():
        log("[SIGNAL] Stop flag detected. Exiting...")
        try: os.remove(STOP_FLAG) 
        except: pass
        sys.exit(0)
        
    while PAUSE_FLAG.exists():
        log("[SIGNAL] Paused... Waiting for resume.")
        time.sleep(5)
        if STOP_FLAG.exists():
            return # Exit loop to handle stop

def log(msg: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    line = f"[{timestamp}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except: pass

def run_scraper(province_name: str, url: str) -> bool:
    """
    Launch the Scraper Tool for a single province URL.
    Returns True on success, False on failure/block.
    """
    check_signals()
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
    parser = argparse.ArgumentParser(description="Run periodic low-cost scraper.")
    parser.add_argument("--nordvpn", action="store_true", help="Rotate IP via NordVPN periodically")
    parser.add_argument("--operation", default="sale", choices=["sale", "rent"], help="Operation type to scrape (sale/rent)")
    args = parser.parse_args()

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
        
        # Determine URL based on operation
        if args.operation == "sale":
            url = prov.get("url_venta")
        else:
            url = prov.get("url_alquiler")
            
        if not url:
            log(f"[WARN] No URL found for {name} ({args.operation}). Skipping.")
            continue
        
        log(f"\n[{i}/{len(provinces)}] Processing: {name} ({args.operation})")
        
        # VPN Rotation Logic
        if args.nordvpn and i > 1 and (i - 1) % NORDVPN_ROTATE_EVERY == 0:
            log(f"[VPN] Periodic IP rotation (every {NORDVPN_ROTATE_EVERY} provinces)...")
            try:
                rotate_ip()
            except Exception as e:
                log(f"[VPN] IP rotation failed: {e}")

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
            
            # Sleep with signal check
            for _ in range(delay):
                check_signals()
                time.sleep(1)
    
    log("\n" + "=" * 60)
    log(f"PERIODIC SCRAPER COMPLETE")
    log(f"Success: {success_count} | Failed: {fail_count}")
    log("=" * 60)

if __name__ == "__main__":
    main()
