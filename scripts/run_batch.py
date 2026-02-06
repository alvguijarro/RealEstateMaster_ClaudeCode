"""
Batch Scraper Runner (Multi-Province)
Reads targets from batch_queue.json and executes them sequentially.
"""
import json
import sys
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
        log("[SIGNAL] Stop flag detected. Exiting batch...")
        try: os.remove(STOP_FLAG)
        except: pass
        sys.exit(0)
    
    while PAUSE_FLAG.exists():
        log("[SIGNAL] Paused. Waiting for resume...")
        time.sleep(5)
        if STOP_FLAG.exists(): return

def run_single_url(url: str, mode: str) -> bool:
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
        
    # Start Scrape via API
    payload = {
        "seed_url": url,
        "mode": mode,
        "max_pages": 4000 # High limit for batch
    }
    
    try:
        log(f"Starting batch item: {target_prov}")
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
    
    if not QUEUE_FILE.exists():
        log("[ERR] No batch queue file found.")
        sys.exit(1)
        
    try:
        with open(QUEUE_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            urls = data.get('urls', [])
            mode = data.get('mode', 'fast')
    except Exception as e:
        log(f"[ERR] Failed to read queue: {e}")
        sys.exit(1)
        
    log(f"Queue size: {len(urls)} URLs. Mode: {mode}")
    
    success_count = 0
    
    for i, url in enumerate(urls, 1):
        log(f"\n[{i}/{len(urls)}] Processing URL: {url}...")
        
        # Retry logic (Infinite retries for blocks)
        success = False
        while not success:
            success = run_single_url(url, mode)
            if not success:
                log(f"Esperando {BLOCK_WAIT_TIME // 60} minutos para recuperación de IP...", "WARN")
                # Countdown timer - more responsive signal checking
                for second in range(BLOCK_WAIT_TIME):
                    time.sleep(1)
                    if second % 60 == 0 and second > 0:
                        log(f"Faltan { (BLOCK_WAIT_TIME - second) // 60 } minutos...")
                    check_signals()
                log("Tiempo de espera finalizado. Reintentando...", "OK")
        
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
