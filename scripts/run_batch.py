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
sys.path.insert(0, str(SCRIPT_DIR.parent))
try:
    from scraper_wrapper import (
        get_current_profile_config, BROWSER_ENGINES, PROFILE_COOLDOWN_MINUTES
    )
    HAS_PROFILE_MGMT = True
except ImportError:
    HAS_PROFILE_MGMT = False
    print("[WARN] Could not import profile management. Multi-browser rotation will be handled by the server.")

try:
    from shared.proxy_config import PROXY_LABEL
except ImportError:
    PROXY_LABEL = "[Proxy #?]"

# Config
DELAY_BETWEEN = (15, 35)  # seconds between successful provinces
RETRY_LIMIT_PER_URL = 3   # Raised from 2: extra chance after Firefox hang
BLOCK_WAIT_TIME = 900     # 15 min if blocked
RETRY_WAIT_BASE = 30      # seconds between retries (increased from 10)

def log(msg: str, level: str = "INFO"):
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = f"[{level}] " if level in ["INFO", "WARN", "ERR", "OK"] else ""
    line = f"{prefix}[{timestamp}] {msg}"
    print(line, flush=True)

def check_signals():
    if STOP_FLAG.exists():
        log("[SIGNAL] Stop flag detected. Exiting batch runner.")
        sys.exit(0)
    
    was_paused = False
    while PAUSE_FLAG.exists():
        if not was_paused:
            log("[STATUS] paused")
            log("[SIGNAL] Paused. Waiting for resume...")
            was_paused = True
        time.sleep(5)
        if STOP_FLAG.exists():
            log("[SIGNAL] Stop flag detected during pause. Exiting batch runner.")
            sys.exit(0)
            
    if was_paused:
        log("[STATUS] running")
        log("[SIGNAL] Resumed.")

def extract_province_name(url: str) -> str:
    """Extract a human-readable province name from an Idealista URL slug."""
    try:
        # Match the segment after /venta-viviendas/ or /alquiler-viviendas/
        import re
        m = re.search(r'idealista\.com/(?:venta|alquiler)-viviendas/([^/?]+)', url)
        if m:
            slug = m.group(1)
            return slug.replace('-', ' ').title()
        # Fallback: last meaningful path segment
        parts = [p for p in url.rstrip('/').split('/') if p and 'idealista' not in p and 'http' not in p]
        if parts:
            return parts[-1].replace('-', ' ').title()
    except:
        pass
    return "Unknown"

def run_single_url(url: str, mode: str, browser_engine: str = "chromium", smart_enrichment: bool = False, parallel_enrichment: bool = False, target_file: str = None) -> bool:
    target_prov = extract_province_name(url)

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
    # NOTE: The server now handles identity rotation INTERNALLY via rotate_identity()
    # We pass browser_engine mainly for reverse compatibility if needed
    payload = {
        "seed_url": url,
        "mode": mode,
        "max_pages": 4000, # High limit for batch
        "browser_engine": browser_engine,
        "smart_enrichment": smart_enrichment,
        "parallel_enrichment": parallel_enrichment,
        "target_file": target_file
    }
    
    try:
        # log(f"🚀 Starting scrape for: {target_prov}") # Removed to avoid redundancy with next log
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
                    internal_status = status_data.get("internal_status", "")
                    
                    if current_status == "completed" or internal_status == "completed":
                        log(f"[OK] Completed: {target_prov}")
                        return True
                    elif current_status in ["blocked", "captcha", "error", "stopped"] or \
                         internal_status in ["blocked", "captcha", "error", "stopped"]:
                        # Blocked status means the controller already triggered rotation
                        log(f"[WARN] Session interrupted ({current_status}/{internal_status}) on {target_prov}. Server will handle rotation.")
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
    
    # Identity status for user info
    if HAS_PROFILE_MGMT:
        conf = get_current_profile_config()
        # log(f"🎭 Current Identity: {conf['name']} (Profile {conf['index']})")
    # else:
    #    log("⚠️ Multi-browser rotation NOT available (using chromium only)")
    
    if not QUEUE_FILE.exists():
        log("[ERR] No batch queue file found.")
        sys.exit(1)
        
    try:
        with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            urls = data.get('urls', [])
            mode = data.get('mode', 'fast')
            smart_enrichment = data.get('smart_enrichment', False)
            parallel_enrichment = data.get('parallel_enrichment', False)
            target_file = data.get('target_file')
    except Exception as e:
        log(f"[ERR] Failed to read queue: {e}")
        sys.exit(1)
        
    # log(f"Queue size: {len(urls)} URLs. Mode: {mode}")
    # if smart_enrichment:
    #    log("🔍 Smart Enrichment Mode: ENABLED")
    
    success_count = 0
    
    for i, url in enumerate(urls, 1):
        if not url:
            log(f"[{i}/{len(urls)}] Skipping invalid URL (None/Empty)...", "WARN")
            continue
            
        target_prov = extract_province_name(url)
        
        log(f"{PROXY_LABEL} 🚀 [{i}/{len(urls)}] Processing: {target_prov} ({url})")
        
        success = False
        retries = 0
        
        while not success and retries < RETRY_LIMIT_PER_URL:
            if retries > 0:
                log(f"🔄 Retry {retries}/{RETRY_LIMIT_PER_URL} for {target_prov}...", "INFO")
                
            success = run_single_url(url, mode, "auto", smart_enrichment, parallel_enrichment, target_file)
            
            if not success:
                retries += 1
                if retries >= RETRY_LIMIT_PER_URL:
                    log(f"❌ Giving up on {target_prov} after {RETRY_LIMIT_PER_URL} failed attempts.", "ERR")
                    break
                    
                log(f"⚠️ Scrape interrupted. Waiting {RETRY_WAIT_BASE}s before letting server retry with fresh identity...", "WARN")
                time.sleep(RETRY_WAIT_BASE)
                check_signals()
        
        if success:
            success_count += 1
            log(f"✅ [{i}/{len(urls)}] Finished: {target_prov}", "OK")
        
        # Delay between items
        if i < len(urls):
            delay = random.randint(*DELAY_BETWEEN)
            next_url = urls[i]
            next_prov = extract_province_name(next_url)
                
            # If the previous province failed all retries, add an extended cooldown
            # This helps when the IP is being rate-limited across all browser profiles
            if not success:
                extra_wait = random.randint(90, 150)  # 1.5-2.5 min extra cooldown
                log(f"⏳ Provincia fallida. Cooldown extendido: {extra_wait}s antes de continuar...", "WARN")
                for _ in range(extra_wait):
                    if check_signals(): break
                    time.sleep(1)
                
            log(f"⏱️ Waiting {delay}s before starting NEXT province: {next_prov}...", "INFO")
            
            # Check for signals during sleep
            for _ in range(delay):
                if check_signals(): break 
                time.sleep(1)
            
    log(f"🏁 Batch finished! Successfully processed {success_count}/{len(urls)} URLs.")

if __name__ == "__main__":
    main()
