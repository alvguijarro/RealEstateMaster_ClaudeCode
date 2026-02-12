
import os
import sys
import json
import time
import requests
import random
import subprocess
import threading
from pathlib import Path
from datetime import datetime, timedelta

# Configuration paths
BASE_DIR = Path(__file__).parent.parent
SCRAPER_DIR = BASE_DIR / "scraper"
SCRIPTS_DIR = BASE_DIR / "scripts"
STATE_FILE = SCRAPER_DIR / "cycle_state.json"
VENTA_FILE = SCRAPER_DIR / "documentation" / "idealista_urls_venta.md"
ALQUILER_FILE = SCRAPER_DIR / "documentation" / "idealista_urls_alquiler.md"
SERVER_URL = "http://localhost:5003"

# Default settings for the cycle
CYCLE_INTERVAL_DAYS = 14
RETRY_DELAY_SEC = 300  # 5 minutes if something fails
API_TIMEOUT = 10

def log(msg, level="INFO"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}", flush=True)

def parse_markdown_table(file_path):
    """Extract Province and URL from the markdown table files."""
    urls = []
    if not os.path.exists(file_path):
        log(f"File not found: {file_path}", "ERR")
        return urls
        
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        
    for line in lines:
        if "|" in line and "http" in line:
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 4:
                prov = parts[2]
                url_raw = parts[3]
                # Extract URL from markdown link [title](url)
                if "(" in url_raw and ")" in url_raw:
                    url = url_raw.split("(")[1].split(")")[0]
                else:
                    url = url_raw
                urls.append({"province": prov, "url": url})
    return urls

def load_state():
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            log(f"Error loading state: {e}", "WARN")
    return {
        "last_completed": None,
        "current_operation": "alquiler", # Start with alquiler
        "current_index": 0,
        "is_active": False,
        "history": []
    }

def save_state(state):
    with open(STATE_FILE, 'w', encoding='utf-8') as f:
        json.dump(state, f, indent=4)

def is_server_running():
    try:
        resp = requests.get(f"{SERVER_URL}/health", timeout=3)
        return resp.status_code == 200
    except:
        return False

def start_server():
    log("Starting scraper server...")
    server_script = SCRAPER_DIR / "app" / "server.py"
    # Launch server as a separate process
    subprocess.Popen([sys.executable, str(server_script)], 
                     cwd=str(SCRAPER_DIR),
                     stdout=subprocess.DEVNULL,
                     stderr=subprocess.DEVNULL,
                     creationflags=subprocess.CREATE_NO_WINDOW if os.name == 'nt' else 0)
    
    # Wait for server to be ready
    for _ in range(15):
        if is_server_running():
            log("Server is UP.")
            return True
        time.sleep(2)
    return False

def run_scrape_task(url, operation):
    """Triggers a scrape task via API and waits for completion."""
    if not is_server_running():
        if not start_server():
            log("Critical: Could not start server.", "ERR")
            return False
            
    payload = {
        "urls": [url],
        "mode": "stealth",
        "use_vpn": True,
        "smart_enrichment": True
    }
    
    try:
        log(f"Launching batch for {url} ({operation})...")
        resp = requests.post(f"{SERVER_URL}/api/start-batch", json=payload, timeout=API_TIMEOUT)
        if resp.status_code != 200:
            log(f"Failed to start task: {resp.text}", "ERR")
            return False
            
        # Poll for completion
        while True:
            time.sleep(30)
            try:
                status_resp = requests.get(f"{SERVER_URL}/api/status", timeout=5)
                if status_resp.status_code == 200:
                    data = status_resp.json()
                    status = data.get("status")
                    internal = data.get("internal_status")
                    
                    if status == "completed" or internal == "completed":
                        log("Task completed successfully.")
                        return True
                    elif status in ["error", "stopped"]:
                        log(f"Task failed with status: {status}", "ERR")
                        return False
                else:
                    log("Status check failed, retrying...", "WARN")
            except Exception as e:
                log(f"Error polling status: {e}", "WARN")
                
    except Exception as e:
        log(f"Exception during task execution: {e}", "ERR")
        return False

def main():
    log("=== AUTOMATED SCRAPER CYCLE ORCHESTRATOR ===")
    
    state = load_state()
    
    # Check if we should start a new cycle
    if state["last_completed"]:
        last_dt = datetime.fromisoformat(state["last_completed"])
        if datetime.now() < last_dt + timedelta(days=CYCLE_INTERVAL_DAYS) and not state.get("is_active"):
            log(f"Too soon to start. Next cycle after {last_dt + timedelta(days=CYCLE_INTERVAL_DAYS)}")
            return

    state["is_active"] = True
    save_state(state)
    
    # Load all URLs
    alquiler_tasks = parse_markdown_table(ALQUILER_FILE)
    venta_tasks = parse_markdown_table(VENTA_FILE)
    
    while True:
        current_op = state["current_operation"]
        tasks = alquiler_tasks if current_op == "alquiler" else venta_tasks
        idx = state["current_index"]
        
        if idx >= len(tasks):
            # Finished current operation
            if current_op == "alquiler":
                log("Finished all ALQUILER provinces. Switching to VENTA.")
                state["current_operation"] = "venta"
                state["current_index"] = 0
                save_state(state)
                continue
            else:
                # Finished everything
                log("=== CYCLE COMPLETE ===")
                state["last_completed"] = datetime.now().isoformat()
                state["current_operation"] = "alquiler" # reset for next time
                state["current_index"] = 0
                state["is_active"] = False
                save_state(state)
                break
        
        task = tasks[idx]
        log(f"--- PROVINCE {idx+1}/{len(tasks)}: {task['province']} ({current_op}) ---")
        
        success = run_scrape_task(task['url'], current_op)
        
        if success:
            state["current_index"] += 1
            save_state(state)
            # Short rest between provinces
            delay = random.randint(30, 90)
            log(f"Resting {delay}s before next province...")
            time.sleep(delay)
        else:
            log(f"Task failed for {task['province']}. Will retry in {RETRY_DELAY_SEC}s...", "WARN")
            time.sleep(RETRY_DELAY_SEC)

if __name__ == "__main__":
    main()
