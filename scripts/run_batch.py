"""
Batch Scraper Runner (Multi-Province) with Multi-Browser Rotation
Reads targets from batch_queue.json and executes them sequentially.
Rotates between Chromium and Firefox, respecting profile cooldowns.
"""
import json
import sys
# Force UTF-8 encoding for stdout/stderr to handle emojis
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
import time
import os
import requests
import random
from pathlib import Path
from datetime import datetime

# Paths
SCRIPT_DIR = Path(__file__).parent.parent / "scraper"
QUEUE_FILE = SCRIPT_DIR / "batch_queue.json"
STOP_FLAG = SCRIPT_DIR / "BATCH_STOP.flag"
PAUSE_FLAG = SCRIPT_DIR / "BATCH_PAUSE.flag"

# Import profile management from scraper
sys.path.insert(0, str(SCRIPT_DIR / "app"))
try:
    from scraper_wrapper import (
        select_next_engine, get_last_engine, get_available_engines,
        get_cooldown_remaining, BROWSER_ENGINES, PROFILE_COOLDOWN_MINUTES
    )
    HAS_PROFILE_MGMT = True
except ImportError:
    HAS_PROFILE_MGMT = False
    print("[WARN] Could not import profile management. Using default engine.")

# Config
DELAY_BETWEEN = (10, 30)  # seconds
MAX_RETRIES = 2
BLOCK_WAIT_TIME = 900  # 15 min if blocked

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = f"[{level}] " if level in ["INFO", "WARN", "ERR", "OK"] else ""
    line = f"{prefix}[{timestamp}] {msg}"
    print(line, flush=True)

def check_signals():
    if STOP_FLAG.exists():
        log("[SIGNAL] Stop flag detected. Sending stop signal to server...")
        try:
            # Tell the server to stop the active scrape
            requests.post("http://localhost:5003/api/stop", timeout=30)
            log("[INFO] Stop command sent to server.")
            time.sleep(2) # Give it a moment
        except Exception as e:
            log(f"[WARN] Failed to send stop command to server: {e}")
            
        log("[INFO] Exiting batch runner.")
        try: os.remove(STOP_FLAG)
        except: pass
        sys.exit(0)
    
    while PAUSE_FLAG.exists():
        log("[SIGNAL] Paused. Waiting for resume...")
        time.sleep(5)
        if STOP_FLAG.exists(): return

def run_single_url(url: str, mode: str, browser_engine: str = "chromium", smart_enrichment: bool = False, target_file: str = None) -> bool:
    target_prov = "Unknown"
    # Basic province extraction for logging
    if "idealista.com" in url:
        try:
            parts = url.split('/')
            for p in parts:
                if "provincia" in p or "madrid" in p or "barcelona" in p: # Heuristic
                    target_prov = p
                    break
        except: pass
    
    check_signals()
    
    # Check server health with retries
    max_health_retries = 3
    for attempt in range(max_health_retries):
        try:
            requests.get("http://localhost:5003/health", timeout=3)
            break
        except:
            if attempt < max_health_retries - 1:
                log(f"[WARN] Scraper server not responding (attempt {attempt+1}/{max_health_retries}). Retrying...")
                time.sleep(2)
            else:
                log("[ERR] Scraper server not running after multiple attempts. Aborting.")
                return False
        
    # Start Scrape via API with browser engine selection
    payload = {
        "seed_url": url,
        "mode": mode,
        "max_pages": 4000, # High limit for batch
        "browser_engine": browser_engine,  # Multi-browser rotation
        "smart_enrichment": smart_enrichment,  # Smart enrichment mode
        "target_file": target_file
    }
    
    try:
        engine_emoji = "🦊" if browser_engine == "firefox" else "🌐"
        log(f"{engine_emoji} Starting [{browser_engine.upper()}]: {target_prov}")
        resp = requests.post("http://localhost:5003/api/start", json=payload, timeout=10)
        
        if resp.status_code != 200:
            log(f"[ERR] Failed to start scrape: {resp.text}")
            return False
            
        # Poll for completion
        while True:
            check_signals()
            time.sleep(5)
            try:
                status_resp = requests.get("http://localhost:5003/api/status", timeout=5)
                if status_resp.status_code == 200:
                    status_data = status_resp.json()
                    current_status = status_data.get("status", "")
                    
                    if current_status == "idle" or current_status == "completed":
                        log(f"[OK] Completed: {target_prov}")
                        return True
                    elif current_status in ["blocked", "captcha", "error", "stopped"] or "CAPTCHA" in str(status_data):
                        log(f"[WARN] Blocked/Captcha/Error/Stopped on {target_prov}")
                        return False
                else:
                    log("[WARN] Status check failed.")
            except:
                pass
                
    except Exception as e:
        log(f"[ERR] Exception: {e}")
        return False

def main():
    log("=== BATCH SCRAPER STARTED ===")
    
    if HAS_PROFILE_MGMT:
        log(f"🔄 Multi-browser rotation enabled: {BROWSER_ENGINES}")
    else:
        log("⚠️ Multi-browser rotation NOT available (using chromium only)")
    
    if not QUEUE_FILE.exists():
        log("[ERR] No batch queue file found.")
        sys.exit(1)
        
    try:
        with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            urls = data.get('urls', [])
            mode = data.get('mode', 'fast')
            smart_enrichment = data.get('smart_enrichment', False)
            target_file = data.get('target_file')
    except Exception as e:
        log(f"[ERR] Failed to read queue: {e}")
        sys.exit(1)
        
    log(f"Queue size: {len(urls)} URLs. Mode: {mode}")
    if smart_enrichment:
        log("🔍 Smart Enrichment Mode: ENABLED")
    
    success_count = 0
    
    for i, url in enumerate(urls, 1):
        if not url:
            log(f"[{i}/{len(urls)}] Skipping invalid URL (None/Empty)...", "WARN")
            continue
            
        log(f"\n[{i}/{len(urls)}] Processing URL: {url}...")
        
        # Retry logic (Infinite retries for blocks, but avoid infinite loop on crashes)
        success = False
        failed_engines = set() # Track engines that failed for THIS specific URL
        
        while not success:
            # Select browser engine with rotation and cooldown checking
            if HAS_PROFILE_MGMT:
                last_engine = get_last_engine()
                selected_engine = select_next_engine(last_engine)
                
                # If the naturally selected engine already failed for this URL, try a different one
                if selected_engine in failed_engines:
                    available_alternatives = [e for e in BROWSER_ENGINES if e not in failed_engines]
                    if available_alternatives:
                        selected_engine = available_alternatives[0]
                        log(f"⚠️ Preferred engine {select_next_engine(last_engine)} failed previously. Switching to {selected_engine.upper()}")
                    else:
                        log("⚠️ All browser engines failed for this URL. Waiting 2 minutes before retrying...", "WARN")
                        # Wait 2 minutes before wiping failed_engines and retrying
                        # This avoids a tight loop of failure-retry-failure
                        for _ in range(120):
                             time.sleep(1)
                             check_signals()
                        
                        failed_engines.clear() # Reset failure tracking to try again
                        continue # Restart loop to select an engine again
                
                # If all engines are in cooldown, wait for the first one to be available
                if selected_engine is None:
                    available = get_available_engines()
                    if not available:
                        # Find minimum remaining cooldown across all engines
                        min_wait = min(get_cooldown_remaining(eng) for eng in BROWSER_ENGINES)
                        log(f"⏳ All browser profiles in cooldown. Waiting {min_wait} min...", "WARN")
                        for _ in range(min_wait * 60):
                            time.sleep(1)
                            check_signals()
                        # Try again
                        selected_engine = select_next_engine(last_engine)
                        if selected_engine is None:
                            selected_engine = "chromium"  # Fallback
                
                log(f"🎯 Selected engine: {selected_engine.upper()}")
            else:
                selected_engine = "chromium"
            
            success = run_single_url(url, mode, selected_engine, smart_enrichment, target_file)
            
            if not success:
                # Mark this engine as failed for this URL attempt
                failed_engines.add(selected_engine)
                
                # If failed (blocked), we don't wait 15 mins unconditionally.
                # The 'select_next_engine' at the top of the loop will handle the wait 
                # if ALL engines are blocked.
                # If we have another engine available, we retry immediately.
                log("⚠️ Scrape failed (Block/Captcha/Crash). Checking for next available browser engine...", "WARN")
                time.sleep(5) # Reduced breathing room
                check_signals()
        
        if success: success_count += 1
        
        # Delay between items
        if i < len(urls):
            delay = random.randint(*DELAY_BETWEEN)
            log(f"Waiting {delay}s before next item...")
            check_signals()
            time.sleep(delay)
            
    log("\n=== BATCH COMPLETE ===")
    log(f"Success: {success_count}/{len(urls)}")

if __name__ == "__main__":
    main()
