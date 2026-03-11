"""
Script to update existing URLs status (Active/Inactive) and capture removal dates.
Re-scrapes ALL URLs found in the specified Excel file, bypassing deduplication.
Updates Excel and emits real-time WebSocket events to the UI.

Usage: python update_urls.py <excel_file_path>
"""
import sys
import os
import asyncio
import argparse
import pandas as pd
import time
import shutil
import random
import json
from pathlib import Path

# Worker ID para ejecución paralela (cada worker usa un proxy distinto)
_worker_id   = int(os.environ.get('SCRAPER_WORKER_ID',   '1'))
_num_workers = int(os.environ.get('SCRAPER_NUM_WORKERS', '1'))

# Pause flag file
PAUSE_FLAG_FILE = "update_paused.flag"
STOP_FLAG_FILE = "update_stop.flag"
STEALTH_FLAG_FILE = "update_stealth.flag"
_sfx = f"_w{_worker_id}" if _num_workers > 1 else ""
JOURNAL_FILE = f"update_progress{_sfx}.jsonl"
ENRICHED_HISTORY_FILE = "enriched_history.json" # Local cache of enriched data
STEALTH_PROFILE_DIR = str(Path(__file__).parent.parent / "stealth_profile")

# Add scraper directory and project root to path
SCRAPER_DIR = Path(__file__).parent
PROJECT_ROOT = SCRAPER_DIR.parent
sys.path.insert(0, str(SCRAPER_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

# Force UTF-8 for stdout/stderr to avoid Windows charmap errors
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from shared.proxy_config import PROXY_CONFIG, PROXY_LABEL
from idealista_scraper.scraper import _goto_with_retry
from idealista_scraper.extractors import extract_detail_fields, missing_fields
from idealista_scraper.utils import log, play_captcha_alert, simulate_human_interaction, solve_captcha_advanced, detect_captcha_or_block
from app.scraper_wrapper import get_browser_executable_path
from idealista_scraper.config import (
    FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE,
    STEALTH_CARD_DELAY_RANGE, STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SESSION_LIMIT, EXTRA_STEALTH_REST_DURATION_RANGE,
    EXTRA_STEALTH_COFFEE_BREAK_RANGE, EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY,
    SCROLL_STEPS, EXTRA_STEALTH_SCROLL_PAUSE_RANGE,
    USER_AGENTS, BROWSER_ROTATION_POOL, PROFILE_COOLDOWN_MINUTES,
    VIEWPORT_SIZES, EXTRA_STEALTH_READING_TIME_PER_100_CHARS
)
from playwright.async_api import async_playwright

# Identity Rotation State File
IDENTITY_STATE_FILE = str(Path(__file__).parent / "app" / "identity_state.json")

# GPU fingerprints pool for randomization
GPU_FINGERPRINTS = [
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 3060/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce GTX 1660 Ti/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 2070 SUPER/PCIe/SSE2"),
    ("AMD", "AMD Radeon RX 6700 XT"),
    ("AMD", "AMD Radeon RX 580 Series"),
    ("Intel", "Intel(R) UHD Graphics 630"),
    ("Intel", "Intel(R) Iris(R) Xe Graphics"),
]

def get_random_gpu():
    return random.choice(GPU_FINGERPRINTS)

def generate_stealth_script():
    gpu_vendor, gpu_renderer = get_random_gpu()
    return f'''
// ==================== PHASE 1: DEEP FINGERPRINT SPOOFING ====================
try {{
    if (window.chrome && window.chrome.runtime) {{
        delete window.chrome.runtime;
    }}
}} catch (e) {{}}
try {{
    const getParameterProto = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        if (param === 37445) return '{gpu_vendor}';
        if (param === 37446) return '{gpu_renderer}';
        return getParameterProto.call(this, param);
    }};
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParameter2Proto = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return '{gpu_vendor}';
            if (param === 37446) return '{gpu_renderer}';
            return getParameter2Proto.call(this, param);
        }};
    }}
}} catch (e) {{}}
try {{
    const pluginData = [
        {{type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format', name: 'Chrome PDF Plugin'}},
        {{type: 'application/pdf', suffixes: 'pdf', description: '', name: 'Chrome PDF Viewer'}},
        {{type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable', name: 'Native Client'}}
    ];
    const plugins = Object.create(PluginArray.prototype);
    pluginData.forEach((p, i) => {{ plugins[i] = p; }});
    Object.defineProperty(plugins, 'length', {{value: pluginData.length, writable: false, enumerable: true}});
    plugins[Symbol.iterator] = function*() {{ for (let i = 0; i < this.length; i++) yield this[i]; }};
    plugins.item = function(i) {{ return this[i] || null; }};
    plugins.namedItem = function(name) {{ for (let i = 0; i < this.length; i++) {{ if (this[i].name === name) return this[i]; }} return null; }};
    plugins.refresh = function() {{}};
    Object.defineProperty(navigator, 'plugins', {{
        get: () => plugins
    }});
}} catch (e) {{}}
try {{
    Object.defineProperty(navigator, 'languages', {{
        get: () => ['es-ES', 'es', 'en-US', 'en']
    }});
}} catch (e) {{}}
try {{
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    Object.defineProperty(navigator, 'webdriver', {{
        get: () => undefined
    }});
}} catch (e) {{}}
'''

DEEP_STEALTH_SCRIPT = generate_stealth_script()

def load_identity_state() -> dict:
    if not os.path.exists(IDENTITY_STATE_FILE):
        return {"current_index": 0, "cooldowns": {}}
    try:
        with open(IDENTITY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except:
        return {"current_index": 0, "cooldowns": {}}

def save_identity_state(state: dict) -> None:
    try:
        os.makedirs(os.path.dirname(IDENTITY_STATE_FILE), exist_ok=True)
        with open(IDENTITY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except:
        pass

def rotate_identity():
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    pool_size = len(BROWSER_ROTATION_POOL)
    cooldown_seconds = PROFILE_COOLDOWN_MINUTES * 60
    now = time.time()
    for pid in list(state["cooldowns"].keys()):
        blocked_time = state["cooldowns"][pid]
        if now - blocked_time >= cooldown_seconds:
            del state["cooldowns"][pid]
    available_indices = []
    for i in range(pool_size):
        idx = (current_idx + 1 + i) % pool_size
        config = BROWSER_ROTATION_POOL[idx]
        pid = str(config["index"])
        if pid not in state["cooldowns"]:
            available_indices.append(idx)
    if available_indices:
        next_idx = available_indices[0]
        state["current_index"] = next_idx
        save_identity_state(state)
        return BROWSER_ROTATION_POOL[next_idx], 0
    wait_info = []
    for i in range(pool_size):
        config = BROWSER_ROTATION_POOL[i]
        pid = str(config["index"])
        blocked_time = state["cooldowns"].get(pid, now)
        remaining = max(1, cooldown_seconds - (now - blocked_time))
        wait_info.append((remaining, i))
    wait_info.sort()
    min_wait, next_idx = wait_info[0]
    state["current_index"] = next_idx
    save_identity_state(state)
    return BROWSER_ROTATION_POOL[next_idx], min_wait

def mark_current_profile_blocked() -> None:
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    if current_idx >= len(BROWSER_ROTATION_POOL): current_idx = 0
    config = BROWSER_ROTATION_POOL[current_idx]
    pool_id = str(config["index"])
    state["cooldowns"][pool_id] = time.time()
    save_identity_state(state)

def get_profile_dir(profile_index: int) -> str:
    base_dir = Path(__file__).parent.parent
    return str(base_dir / f"stealth_profile_{profile_index}")

# Imported from app.scraper_wrapper above

async def human_warmup_routine(page, emit_func=None):
    import random
    if emit_func: emit_func("STEALTH", "Starting human warm-up routine...")
    try:
        await page.goto('https://www.google.es', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(2, 4))
        for _ in range(random.randint(2, 5)):
            await page.mouse.move(random.randint(100, 1000), random.randint(100, 600), steps=10)
            await asyncio.sleep(0.2)
        await page.goto('https://www.idealista.com', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(3, 5))
        await page.evaluate("""() => {
            const btn = document.querySelector('#didomi-notice-agree-button, [id*="accept"], .onetrust-accept-btn');
            if (btn) btn.click();
        }""")
        if emit_func: emit_func("OK", "Human warm-up complete")
    except Exception as e:
        if emit_func: emit_func("WARN", f"Warm-up partial: {e}")

# Configure logging to suppress noisy libraries
import logging
logging.getLogger('socketio').setLevel(logging.ERROR)
logging.getLogger('engineio').setLevel(logging.ERROR)

# Optional: Socket client for real-time updates (if available)
import io
_old_stdout = sys.stdout
_old_stderr = sys.stderr
try:
    sys.stdout = io.StringIO()
    sys.stderr = io.StringIO()
    import socketio
    sio = socketio.Client(logger=False, engineio_logger=False)
    HAS_SOCKET = True
except ImportError:
    sio = None
    HAS_SOCKET = False
finally:
    sys.stdout = _old_stdout
    sys.stderr = _old_stderr


class BlockedException(Exception):
    """Raised when Idealista blocks access due to 'uso indebido'."""
    pass


def emit_to_ui(level: str, message: str, **kwargs):
    """Emit log message to both console and UI (if connected)."""
    # print(f"[{level}] {message}")
    if HAS_SOCKET and sio.connected:
        try:
            sio.emit('log_message', {'level': level, 'message': message, **kwargs})
        except: pass
    else:
        print(f"[{level}] {message}")

def save_merged_excel(output_path, dfs, updated_rows_list):
    """
    Saves data to Excel by merging updated rows into the original dataframes.
    Ensures ALL original sheets and rows are preserved.
    """
    # 1. Group updated rows by URL for fast lookup
    updates_by_url = {row['URL']: row for row in updated_rows_list if row.get('URL')}
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 2. Iterate over ALL original sheets from the source file
        for sheet_name, df_orig in dfs.items():
            # Convert original dataframe to list of dicts
            original_data = df_orig.to_dict('records')
            merged_data = []
            
            # 3. Merge process
            for row in original_data:
                url = row.get('URL')
                # If we have an update for this URL, use the updated row
                # But ensure we keep any original columns that might not be in the update?
                # In this scraper logic, 'final_row' is constructed carefully to include preserved cols.
                # So we can safely use the updated row.
                if url and url in updates_by_url:
                    merged_data.append(updates_by_url[url])
                else:
                    merged_data.append(row)
            
            # 4. Write sheet
            if merged_data:
                pd.DataFrame(merged_data).to_excel(writer, sheet_name=sheet_name, index=False)
            else:
                # Should not happen if original had data, but just in case
                pd.DataFrame(original_data).to_excel(writer, sheet_name=sheet_name, index=False)
    return True

def emit_progress(current, total, sheet_name=None, excel_file=None):
    """Emit progress event to UI."""
    if HAS_SOCKET and sio.connected:
        try:
            sio.emit('progress', {
                'current_properties': current,
                'total_properties': total,
                'current_page': 1,
                'total_pages': 1,
                'sheet_name': sheet_name,
                'excel_file': excel_file
            })
        except:
            pass

def emit_property(property_data):
    """Emit a single scraped property to the UI for real-time table update."""
    if HAS_SOCKET and sio.connected:
        try:
            sio.emit('property_scraped', property_data)
        except:
            pass


async def check_signals():
    """Check for pause/stop signals from the filesystem."""
    if os.path.exists(STOP_FLAG_FILE):
        return "stop"
    
    if os.path.exists(PAUSE_FLAG_FILE):
        emit_to_ui('INFO', '[STATUS] paused')
        while os.path.exists(PAUSE_FLAG_FILE):
            await asyncio.sleep(2)
            if os.path.exists(STOP_FLAG_FILE):
                return "stop"
        emit_to_ui('INFO', '[STATUS] resumed')
        return "resume"
    
    return None

def handle_blocked_profile():
    """Delete the current profile if it has been blocked/poisoned to ensure next run is fresh."""
    import shutil
    import glob
    
    emit_to_ui("WARN", "☣️  PROFILE POISONED: Purging blocked profile directory...")
    
    if os.path.exists(STEALTH_PROFILE_DIR):
        try:
            # Ensure browser is closed and remove the directory
            shutil.rmtree(STEALTH_PROFILE_DIR, ignore_errors=True)
            emit_to_ui("OK", "✨ Poisoned profile deleted. Next run will generate a fresh, clean identity.")
        except Exception as e:
            emit_to_ui("ERR", f"Failed to delete poisoned profile: {e}")
            
    # Also trigger a general cleanup of any old residual blocked folders
    try:
        base_dir = os.path.dirname(STEALTH_PROFILE_DIR)
        pattern = os.path.join(base_dir, "stealth_profile_BLOCKED_*")
        blocked_folders = glob.glob(pattern)
        for folder in blocked_folders:
            if os.path.isdir(folder):
                shutil.rmtree(folder, ignore_errors=True)
    except:
        pass

async def detect_captcha(page) -> str | None:
    """Check if page shows CAPTCHA/bot protection. Returns 'block', 'captcha' or None.
    Delegado a detect_captcha_or_block (función canónica en utils.py).
    """
    return await detect_captcha_or_block(page)

async def variable_scroll(page):
    """Perform variable scroll pattern (Extra Stealth)."""
    # Sometimes scroll up a bit first
    if random.random() < 0.3:
        try:
            await page.evaluate('window.scrollBy(0, -150)')
            await asyncio.sleep(random.uniform(0.3, 0.8))
        except: pass
    
    # Variable scroll amounts
    for step in range(SCROLL_STEPS):
        try:
            scroll_amount = random.randint(200, 500)
            await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
            
            # Use stealth pause range
            pause = random.uniform(*EXTRA_STEALTH_SCROLL_PAUSE_RANGE)
            await asyncio.sleep(pause)
            
            # Occasionally pause mid-scroll as if reading
            if random.random() < 0.2:
                pause_time = random.uniform(1.0, 3.0)
                await asyncio.sleep(pause_time)
        except: pass
    
    # Sometimes scroll back up slightly
    if random.random() < 0.2:
        try:
            await page.evaluate('window.scrollBy(0, -100)')
            await asyncio.sleep(random.uniform(0.2, 0.5))
        except: pass

def save_to_journal(filename, row):
    """Append a row to the journal file."""
    try:
        # We store: filename (for context), timestamp, and the row data
        entry = {
            "source_file": os.path.basename(filename),
            "timestamp": time.time(),
            "data": row
        }
        with open(JOURNAL_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception as e:
        print(f"Error saving to journal: {e}")

def load_journal(target_filename):
    """Load updated rows from journal for a specific file."""
    if not os.path.exists(JOURNAL_FILE):
        return []
        
    restored = []
    target_base = os.path.basename(target_filename)
    
    try:
        with open(JOURNAL_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    entry = json.loads(line)
                    if entry.get("source_file") == target_base:
                        restored.append(entry.get("data"))
                except:
                    pass
    except Exception as e:
        print(f"Error loading journal: {e}")
    return restored

def load_history():
    """Load the global enriched history cache."""
    history_path = Path(SCRAPER_DIR) / ENRICHED_HISTORY_FILE
    if not history_path.exists():
        return {}
    try:
        with open(history_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        emit_to_ui('ERR', f"Error loading history: {e}")
        return {}

def save_history(history_data):
    """Save enriched data to global history cache."""
    history_path = Path(SCRAPER_DIR) / ENRICHED_HISTORY_FILE
    try:
        # Load existing first to merge? Or assume we have the full dict in memory?
        # If multiple processes run, we should lock. For now, assume single process.
        # Ideally, we append or update. Reading full file every 50 items is fine for small/medium files.
        # But if history grows huge (100k+), this is slow. 
        # For now, let's just write what we have if we pass the full dict, 
        # OR better: read current, update, write.
        
        current_history = {}
        if history_path.exists():
            with open(history_path, 'r', encoding='utf-8') as f:
                try:
                    current_history = json.load(f)
                except: pass
        
        current_history.update(history_data)
        
        with open(history_path, 'w', encoding='utf-8') as f:
            json.dump(current_history, f, ensure_ascii=False, indent=2)
            
    except Exception as e:
        emit_to_ui('ERR', f"Error saving history: {e}")

async def save_checkpoint(excel_file, updated_rows, url_to_sheet, dfs):
    """Save the current progress to the Excel file (always as _partial)."""
    # Normalize base filename (strip any existing _updated or _updated_partial suffix)
    base = excel_file.replace('_updated_partial.xlsx', '.xlsx').replace('_updated.xlsx', '.xlsx')
    _w_sfx = f"_w{_worker_id}" if _num_workers > 1 else ""
    output_xlsx = base.replace('.xlsx', f'_updated_partial{_w_sfx}.xlsx')
    
    emit_to_ui('INFO', f'Creating checkpoint: {os.path.basename(output_xlsx)} ...')
    
    try:
        # Use Safe Merge Save
        save_merged_excel(output_xlsx, dfs, updated_rows)
        emit_to_ui('OK', 'Checkpoint saved.')
    except Exception as e:
        emit_to_ui('WARN', f"Checkpoint failed (file open?): {e}")


async def update_urls(excel_file: str, selected_sheets: list = None, resume: bool = False):
    """Update URL status from Excel file."""
    # Connect to WebSocket for real-time logging (silently)
    if HAS_SOCKET:
        try:
            # Connect to WebSocket for real-time logging
            sio.connect('http://127.0.0.1:5003', wait_timeout=5)
        except Exception:
            pass 
    
    # 1. Validate file
    if not os.path.exists(excel_file):
        emit_to_ui('ERR', f'File not found: {excel_file}')
        return

    # SOURCE NORMALIZATION: If user selected a result/partial file, switch to the "Mother" (Base) file
    # This ensures we have all original sheets and don't "finish" early if pages were missing.
    base_file = excel_file.replace('_updated_partial.xlsx', '.xlsx').replace('_updated.xlsx', '.xlsx')
    if base_file != excel_file and os.path.exists(base_file):
        emit_to_ui('INFO', f'RESULT FILE DETECTED. Switching to Base file to ensure all original sheets are preserved.')
        emit_to_ui('INFO', f'Base: {os.path.basename(base_file)}')
        excel_file = base_file
    
    emit_to_ui('INFO', f'Loading URLs from: {os.path.basename(excel_file)}')
    
    # 2. Load data
    try:
        # Load ALL sheets to preserve them during merge/save
        dfs = pd.read_excel(excel_file, sheet_name=None)
        
        # Determine which sheets we should EXTRACT URLs from
        sheets_to_process = selected_sheets if (selected_sheets and len(selected_sheets) > 0) else list(dfs.keys())
        sheet_info = f"{len(sheets_to_process)} sheet(s) selected" if selected_sheets else "all sheets"
        
        # Create URL to Sheet and URL to Original Row maps
        url_to_sheet = {}
        url_to_row = {}
        all_rows = []
        
        for sheet_name, df_sheet in dfs.items():
            # Only extract URLs from the selected sheets
            if sheet_name not in sheets_to_process:
                continue
                
            if 'URL' not in df_sheet.columns:
                continue
            
            # Convert to records
            # Force string URL just in case
            df_sheet['URL'] = df_sheet['URL'].astype(str)
            records = df_sheet.to_dict('records')
            
            for row in records:
                u = row.get('URL')
                if u and isinstance(u, str) and "http" in u:
                    url_to_sheet[u] = sheet_name
                    # Clean NaN values from row immediately for easier counting?
                    # No, keep them for now, handle in merge.
                    url_to_row[u] = row
                    all_rows.append(row)

        emit_to_ui('OK', f'Loaded {len(all_rows)} rows from Excel ({sheet_info})')

    except Exception as e:
        emit_to_ui('ERR', f'Error reading Excel: {e}')
        return
    
    if not all_rows:
        emit_to_ui('ERR', f"No URLs found in the selected sheets ({', '.join(sheets_to_process)}).")
        return
    
    # Use the keys from our map as the master list
    urls = list(url_to_row.keys())
    if _num_workers > 1:
        urls = [u for i, u in enumerate(urls) if i % _num_workers == _worker_id - 1]
        emit_to_ui('INFO', f'🔀 Worker {_worker_id}/{_num_workers}: {len(urls)} URLs asignadas')
    emit_to_ui('INFO', f'Found {len(urls)} URLs to check in selected sheets')
    
    # Check resume state
    start_index = 0
    updated_rows = []
    captchas_found = 0
    captchas_solved = 0
    
    if resume:
        restored_data = load_journal(excel_file)
        if restored_data:
            updated_rows = restored_data
            saved_count = len(updated_rows)
            # Find how many URLs we have covered
            # Note: journal saves row per URL.
            # Assuming sequential processing...
            if saved_count > 0 and saved_count < len(urls):
                start_index = saved_count
                emit_to_ui('INFO', f'Resuming from journal. Restored {saved_count} properties.')
                emit_to_ui('INFO', f'Continuing from property {start_index + 1}/{len(urls)}')
            else:
                 if saved_count >= len(urls):
                     emit_to_ui('WARN', 'Journal indicates update was already completed. Resetting start index.')
                     start_index = 0 # Can reset or exit? Reset for now.
                     updated_rows = []
        else:
            emit_to_ui('WARN', 'No interrupted session found. Starting fresh (History check will still apply).')
    
    active_count = 0
    inactive_count = 0
    error_count = 0

    # ================= RECOVERY LOOP =================
    # WebKit y Firefox no soportan proxies autenticados en Windows
    PROXY_INCOMPATIBLE_ENGINES = {"webkit", "firefox"}

    while start_index < len(urls):
        try:
            # IDENTITY ROTATION — saltar engines incompatibles con proxy autenticado
            profile_config, wait_time = rotate_identity()
            while profile_config and profile_config.get("engine") in PROXY_INCOMPATIBLE_ENGINES:
                emit_to_ui('INFO', f'Saltando perfil {profile_config["name"]} (motor {profile_config.get("engine")} incompatible con proxy autenticado en Windows)')
                mark_current_profile_blocked()
                profile_config, wait_time = rotate_identity()
            if wait_time > 0:
                emit_to_ui('WARN', f'All profiles in cooldown. Waiting {int(wait_time/60)}m...')
                await asyncio.sleep(wait_time)
                continue

            profile_dir = get_profile_dir(profile_config["index"])
            os.makedirs(profile_dir, exist_ok=True)
            
            async with async_playwright() as pw:
                emit_to_ui('INFO', f'{PROXY_LABEL} Launching persistent browser with profile: {profile_config["name"]}...')
                
                browser_args = [
                    "--start-maximized",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    "--no-first-run",
                ]
                
                exe_path = get_browser_executable_path(profile_config.get("channel"))
                
                # Randomize viewport
                viewport = random.choice(VIEWPORT_SIZES)

                # --- PRE-SCAN FOR HISTORY/EXISTING --
                # To ensure UI reflects progress immediately (e.g. "34/100" if 34 are already done),
                # we scan HEAD of the list for contiguous enriched items.
                HISTORY = load_history()
                # --- PRE-SCAN: Use History to skip already done properties ---
                scan_idx = start_index
                pre_processed_rows = []
                skipped_count = 0

                for k, url in enumerate(urls[start_index:], start_index):
                    # Check History
                    in_history = url in HISTORY
                    
                    if in_history:
                        # Log summary every 50 skips to avoid flooding
                        if skipped_count > 0 and skipped_count % 50 == 0:
                            emit_to_ui('INFO', f'Pre-scan: Saltadas {skipped_count} propiedades ya encontradas en historial...')
                            await asyncio.sleep(0.01) # Small pause to allow socket to breathe
                        
                        # Add to updated_rows logic
                        d_history = HISTORY.get(url, {})
                        orig_row = url_to_row.get(url, {})
                        final_row = orig_row.copy()
                        final_row['URL'] = url
                        final_row.update(d_history)
                        
                        updated_rows.append(final_row)
                        scan_idx += 1
                        skipped_count += 1
                    else:
                        break
                
                if skipped_count > 0:
                    emit_to_ui('OK', f'✅ Pre-scan completado: {skipped_count} propiedades recuperadas del historial del día.')
                    emit_progress(scan_idx, len(urls), "Pre-procesado...", os.path.basename(excel_file))
                    start_index = scan_idx
                # ------------------------------------
                # ------------------------------------

                # Headless Mode: Faster for URL updates if not in Stealth mode
                is_headless = not is_stealth
                # Firefox y Opera siempre headless (sin ventana visible para el usuario)
                _ch = profile_config.get("channel")
                _eng = profile_config.get("engine", "chromium")
                if _eng == "firefox" or _ch == "opera":
                    is_headless = True
                
                try:
                    # Select the correct engine launcher (chromium, firefox, or webkit)
                    engine_name = profile_config.get("engine", "chromium")
                    browser_launcher = getattr(pw, engine_name)
                    
                    launch_options = {
                        "user_data_dir": profile_dir,
                        "headless": is_headless,
                        "viewport": {"width": viewport[0], "height": viewport[1]},
                        "user_agent": random.choice(USER_AGENTS),
                        "proxy": {
                            "server": f"http://{PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}",
                            "username": f"{PROXY_CONFIG['login']}-session-{PROXY_CONFIG['sticky_session_id']}",
                            "password": PROXY_CONFIG['password'],
                        },
                    }
                    
                    # ENGINE-SPECIFIC CONFIGURATION
                    if engine_name == "chromium":
                        launch_options["args"] = browser_args
                        launch_options["ignore_default_args"] = ["--enable-automation"]
                        channel = profile_config.get("channel")
                        # Only pass channel for Playwright-recognized values (chrome, msedge)
                        # Non-standard channels (opera, brave, vivaldi) need executable_path instead
                        if channel and channel not in ("opera", "brave", "vivaldi", "iron"):
                            launch_options["channel"] = channel
                        if exe_path:
                            launch_options["executable_path"] = exe_path
                    elif engine_name == "firefox":
                        # Firefox specific
                        if exe_path:
                            launch_options["executable_path"] = exe_path
                        launch_options["firefox_user_prefs"] = {
                            "dom.webdriver.enabled": False,
                            "useAutomationExtension": False,
                        }
                    else: # webkit
                        # Webkit is very sensitive to extra args, keep it minimal
                        if exe_path:
                            launch_options["executable_path"] = exe_path

                    context = await browser_launcher.launch_persistent_context(**launch_options)
                    
                    # ADVANCED STEALTH
                    await context.add_init_script(DEEP_STEALTH_SCRIPT)
                    
                    page = context.pages[0] if context.pages else await context.new_page()
                    
                    # Close extra tabs potentially restored by portable browsers (like Opera)
                    for p in context.pages:
                        if p != page:
                            try: await p.close()
                            except: pass
                    
                    # No longer doing warmup per user request
                    # if start_index == 0 or random.random() < 0.1:
                    #     await human_warmup_routine(page, emit_to_ui)
                    

                    # --- PROCESSING LOOP ---
                    # HISTORY loaded above
                    
                    # Stealth Counters
                    session_property_count = 0
                    
                    # TUNED FOR URL UPDATES (Less conservative than main scraper)
                    effective_session_limit = 200 
                    effective_rest_range = (300, 600) # 5-10 mins
                    
                    # Frequency adjust: Every 40-70 properties instead of 10-18
                    UPDATE_COFFEE_BREAK_FREQUENCY = (40, 70)
                    UPDATE_COFFEE_BREAK_DURATION = (10, 25) # Shorter breaks (10-25s)
                    
                    next_coffee_break = random.randint(*UPDATE_COFFEE_BREAK_FREQUENCY)
                    consecutive_empty_errors = 0  
                    
                    # --- PROCESSING LOOP ---
                    for i, url in enumerate(urls[start_index:], start_index + 1):
                        current_list_idx = i - 1 
                        
                        # Handle Pause/Stop
                        sig = await check_signals()
                        if sig == "stop":
                            emit_to_ui('WARN', 'Stop signal received. Saving partial progress...')
                            # Signal browser to close if possible? 
                            # The loop will break and enter finally block
                            break

                        # Get original row data for this URL
                        orig_row_data = url_to_row.get(url, {})
                            
                        # --- DEACTIVATED SKIP: Skip properties already marked as inactive ---
                        is_active = str(orig_row_data.get('Anuncio activo', '')).strip().lower() not in ['no', 'false', '0', 'falso']
                        if not is_active:
                            emit_to_ui('INFO', f'({i}/{len(urls)}) [SKIP] Already deactivated: {url}')
                            updated_rows.append(orig_row_data)
                            start_index = i
                            continue

                        # --- ENRICHMENT SKIP: Check 'enriched' column and 'Fecha Scraping' ---
                        is_enriched_in_excel = (
                            str(orig_row_data.get('enriched', '')).upper() in ['VERDADERO', 'TRUE', 'YES', 'SI', '1'] or
                            str(orig_row_data.get('__enriched__', '')).upper() in ['VERDADERO', 'TRUE', 'YES', 'SI', '1']
                        )
                        
                        # Efficiency Skip: If already scraped today and active, no need to re-check
                        from datetime import datetime
                        today_str = datetime.now().strftime("%d/%m/%Y")
                        date_scraped = str(orig_row_data.get('Fecha Scraping', '')).strip()
                        
                        if is_enriched_in_excel or (is_active and date_scraped == today_str):
                            reason = "Enriched" if is_enriched_in_excel else "Scraped today"
                            emit_to_ui('INFO', f'({i}/{len(urls)}) [SKIP] {reason} & active: {url}')
                            # Add original row to updated_rows to preserve it
                            updated_rows.append(orig_row_data)
                            
                            start_index = i
                            continue
                            
                        # --- SMART SKIP: Check History ---
                        if url in HISTORY:
                            # Log every 20 skips during regular processing
                            if i % 20 == 0:
                                emit_to_ui('INFO', f'({i}/{len(urls)}) Skipping known properties (History)...')
                                await asyncio.sleep(0.01)

                            d_history = HISTORY.get(url, {})
                            orig_row = url_to_row.get(url, {})
                            final_row = orig_row.copy()
                            final_row['URL'] = url
                            final_row.update(d_history)
                            
                            updated_rows.append(final_row)
                            emit_progress(i, len(urls), url_to_sheet.get(url, 'Unknown'), os.path.basename(excel_file))
                            emit_property(final_row) 
                            
                            start_index = i
                            continue

                        try:
                            # Mode Selection: Default to FAST mode for speed, check flag for STEALTH
                            if is_stealth:
                                card_delay = STEALTH_CARD_DELAY_RANGE
                                post_delay = STEALTH_POST_CARD_DELAY_RANGE
                                posts_delay = STEALTH_POST_CARD_DELAY_RANGE
                            else:
                                # FAST MODE (Default for URL updates)
                                card_delay = FAST_CARD_DELAY_RANGE
                                post_delay = FAST_POST_CARD_DELAY_RANGE
                                posts_delay = FAST_POST_CARD_DELAY_RANGE
                                
                            # --- STEALTH: Coffee Break & Session Rest ---
                            if is_stealth:
                                # 1. Coffee Break
                                if session_property_count >= next_coffee_break:
                                    break_duration = random.uniform(*UPDATE_COFFEE_BREAK_DURATION)
                                    emit_to_ui('INFO', f'☕ Coffee break: Pausing for {int(break_duration)}s...')
                                    await asyncio.sleep(break_duration)
                                    next_coffee_break = session_property_count + random.randint(*UPDATE_COFFEE_BREAK_FREQUENCY)

                                # 2. Session Rest (Long Pause)
                                if session_property_count >= effective_session_limit:
                                    rest_duration = random.uniform(*effective_rest_range)
                                    rest_mins = int(rest_duration / 60)
                                    emit_to_ui('WARN', f'Session limit reached ({effective_session_limit}). Resting for {rest_mins} mins...')
                                    
                                    # Countdown log
                                    remaining = rest_duration
                                    while remaining > 0:
                                        if remaining % 60 == 0: # Log every minute
                                             emit_to_ui('INFO', f'Resting... {int(remaining/60)} mins remaining.')
                                        await asyncio.sleep(min(10, remaining))
                                        remaining -= 10
                                    
                                    session_property_count = 0 # Reset counter
                                    emit_to_ui('INFO', 'Rest complete. Resuming session.')

                            start_item_time = time.time()
                            
                            # Navigate
                            await _goto_with_retry(page, url, humanize=is_stealth)
                            
                            # Check Block immediately
                            block_status = await detect_captcha(page)
                            if block_status == "block":
                                raise BlockedException("Hard Block detected")
                                
                            # Enhanced Stealth Scroll
                            t0 = time.time()
                            if is_stealth:
                                if is_stealth: # Redundant check, but matches original logic
                                    await variable_scroll(page)
                                else:
                                    await simulate_human_interaction(page)
                            await asyncio.sleep(random.uniform(*post_delay))
                            
                            # Extract
                            # Extract
                            d = None
                            for attempt in range(3):
                                try:
                                    d = await extract_detail_fields(page, debug_items=False)
                                    if d and d.get('isBlocked'):
                                        raise BlockedException("Uso Indebido detected (via extractor)")
                                    
                                    # Check data integrity - if empty, we might be blocked or page failed to load
                                    if not d or (not d.get('Titulo') and not d.get('price')):
                                         # Check if inactive or blocked
                                         emit_to_ui('INFO', f'Empty data for {url}. Checking for block...')
                                         await asyncio.sleep(2) # Give it a moment to settle
                                         block_status = await detect_captcha(page)
                                         if block_status == "block": 
                                             raise BlockedException("Hard Block detected during extraction")
                                         if block_status == "captcha": 
                                             break # Deal with captcha later
                                         
                                         page_text = await page.evaluate("document.body.innerText")
                                         if "No encontramos lo que estás buscando" in page_text or "ya no está en nuestra base de datos" in page_text:
                                              if not d: d = {}
                                              d['Anuncio activo'] = 'No'
                                              d['Baja anuncio'] = 'desconocida'
                                              break
                                         
                                         # Look for explicit block strings in body
                                         if any(kw in page_text.lower() for kw in ["uso indebido", "bloqueado", "peticiones"]):
                                             raise BlockedException("Undetected block keywords found in page body")
                                              
                                         # If we reach here, we have no data and it's not a known "Inactive" page.
                                         # High probability of a block/ghost page.
                                         raise BlockedException("Extraction empty (Title/Price missing) — potential block detected")
                                    
                                    break
                                except BlockedException:
                                    raise 
                                except Exception as e:
                                    if "Execution context was destroyed" in str(e) and attempt < 2:
                                        await asyncio.sleep(1)
                                        continue
                                    raise e
                            
                            # Final block/captcha check
                            block_status = await detect_captcha(page)
                            if block_status == "block":
                                raise BlockedException("Hard Block detected")
                                
                            if block_status == "captcha":
                                captchas_found += 1
                                emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA detectado.')
                                emit_to_ui('INFO', 'Intentando resolver CAPTCHA automáticamente...')
                                if await solve_captcha_advanced(page):
                                     if await detect_captcha(page) is None:
                                          captchas_solved += 1
                                          emit_to_ui('OK', 'CAPTCHA resuelto automáticamente!')
                                          d = await extract_detail_fields(page, debug_items=False)
                                
                                if await detect_captcha(page) == "captcha":
                                    emit_to_ui('WARN', 'Resuelve el CAPTCHA manualmente en el navegador.')
                                    manual_deadline = asyncio.get_event_loop().time() + 90
                                    while await detect_captcha(page) == "captcha":
                                        if asyncio.get_event_loop().time() >= manual_deadline:
                                            emit_to_ui('ERR', '⏰ Timeout manual (90s). Rotando identidad...')
                                            raise BlockedException("Manual captcha timeout")
                                        play_captcha_alert()
                                        await asyncio.sleep(5)
                                    d = await extract_detail_fields(page, debug_items=False)
                            
                            # --- Data Merging & Logging ---
                            d = d or {}
                            orig_row = url_to_row.get(url, {})
                            
                            # Count pre-existing fields (non-empty)
                            pre_count = sum(1 for k, v in orig_row.items() if pd.notna(v) and str(v).strip() != "")
                            
                            # Helper for date parsing
                            def parse_relative_date(date_str):
                                if not date_str: return None
                                date_str = date_str.strip().lower()
                                today = pd.Timestamp.now()
                                if date_str == 'hoy':
                                    return today.strftime('%Y-%m-%d')
                                elif date_str == 'ayer':
                                    return (today - pd.Timedelta(days=1)).strftime('%Y-%m-%d')
                                elif date_str == 'anteayer':
                                    return (today - pd.Timedelta(days=2)).strftime('%Y-%m-%d')
                                # Check DD/MM/YYYY
                                try:
                                    # If it matches our expected regex format from extractor (DD/MM/YYYY)
                                    if "/" in date_str:
                                         return pd.to_datetime(date_str, format="%d/%m/%Y").strftime('%Y-%m-%d')
                                except:
                                    pass
                                return date_str

                            # Status check
                            is_inactive = d.get('Anuncio activo') == 'No' or d.get('Baja anuncio')
                            final_row = orig_row.copy()
                            final_row['URL'] = url
                            
                            # Normalize 'Baja anuncio' date if present
                            baja_raw = d.get('Baja anuncio')
                            baja_date = parse_relative_date(baja_raw)
                            
                            if is_inactive:
                                emit_to_ui('WARN', f'{PROXY_LABEL} ({i}/{len(urls)}) [baja] {url}')
                                inactive_count += 1
                                # Inactive: Preserve original data, update status only
                                final_row['Anuncio activo'] = 'No'
                                if baja_date:
                                    final_row['Baja anuncio'] = baja_date
                            else:
                                emit_to_ui('OK', f'{PROXY_LABEL} ({i}/{len(urls)}) [activo] {url}')
                                active_count += 1
                                
                                # Active: OVERWRITE MODE (Prioritize fresh data)
                                # Start fresh with scraped data 'd', keeping URL
                                final_row = d.copy()
                                final_row['URL'] = url
                                
                                # Force current date as the scraping date
                                final_row['Fecha Scraping'] = pd.Timestamp.now().strftime('%Y-%m-%d')
                                
                                # Backfill optional metadata ONLY if it is missing or empty in the new scrape
                                # (e.g. 'Ciudad', 'exterior' might not always be on the detail page)
                                for col in ['Ciudad', 'exterior']:
                                    new_val = final_row.get(col)
                                    if new_val is None or (isinstance(new_val, str) and not new_val.strip()):
                                        old_val = orig_row.get(col)
                                        if old_val is not None and pd.notna(old_val) and str(old_val).strip() != "":
                                            final_row[col] = old_val
                                            
                                # Also ensure we don't have a 'Baja anuncio' date if it is active
                                if 'Baja anuncio' in final_row:
                                    del final_row['Baja anuncio']
                            
                            updated_rows.append(final_row)
                            emit_progress(i, len(urls), url_to_sheet.get(url, 'Unknown'), os.path.basename(excel_file))
                            emit_property(final_row) # Real-time table update
                            
                            # Helper to clean types
                            def clean_for_json(obj):
                                if isinstance(obj, (pd.Timestamp, pd.Timedelta)):
                                    return str(obj)
                                if pd.isna(obj):
                                    return None
                                return obj

                            # Count final fields & identify new keys
                            final_field_count = sum(1 for k, v in final_row.items() if pd.notna(v) and str(v).strip() != "")
                            new_keys = []
                            for k, v in final_row.items():
                                if pd.notna(v) and str(v).strip() != "":
                                    orig_v = orig_row.get(k)
                                    if pd.isna(orig_v) or str(orig_v).strip() == "":
                                        new_keys.append(k)
                            
                            new_fields = len(new_keys)
                            
                            if not is_inactive:
                                emit_to_ui('INFO', f'Fila original: {pre_count} campos. Fila final: {final_field_count} campos ({new_fields} nuevos).')
                                
                                # Emit to UI with metadata about new fields
                                if HAS_SOCKET and sio.connected:
                                    try:
                                        payload = final_row.copy()
                                        payload['_new_fields'] = new_keys
                                        payload = {k: clean_for_json(v) for k, v in payload.items()}
                                        sio.emit('property_scraped', payload)
                                    except: pass

                            save_to_journal(excel_file, final_row)
                            
                            total_item_time = time.time() - start_item_time
                            
                            consecutive_empty_errors = 0  # Reset on success
                            session_property_count += 1 # Increment stealth counter
                            
                            # --- HISTORY UPDATE ---
                            # Clean final_row for history? Convert types?
                            
                            history_entry = {k: clean_for_json(v) for k, v in final_row.items()}
                            pending_history[url] = history_entry
                            HISTORY[url] = history_entry # Update in-memory copy for subsequent lookups if dupes exist

                            emit_progress(i, len(urls), url_to_sheet.get(url, 'Unknown'), os.path.basename(excel_file))
                            
                            start_index = i
                            
                            # --- PERIODIC SAVE (Every 50) ---
                            if i % 50 == 0:
                                await save_checkpoint(excel_file, updated_rows, url_to_sheet, dfs)
                                if pending_history:
                                    save_history(pending_history)
                                    pending_history = {} # Reset buffer
                            
                        except BlockedException:
                            # Save checkpoint before raising
                            await save_checkpoint(excel_file, updated_rows, url_to_sheet, dfs)
                            if pending_history: save_history(pending_history)
                            raise 
                        except Exception as e:
                            emit_to_ui('ERR', f'({i}/{len(urls)}) Error processing {url}: {e}')
                            error_count += 1
                            # REMOVED: start_index = i  <-- This was causing the skipping of the current URL
                            err_msg = str(e).lower()
                            
                            # Convert block-related exceptions to BlockedException
                            if any(x in err_msg for x in ["bloqueado", "uso indebido", "captcha_block", "captcha_timeout", "access denied", "block detected", "extraction empty", "captcha timeout"]):
                                emit_to_ui('ERR', f'Potential block detected via exception: {e}')
                                await save_checkpoint(excel_file, updated_rows, url_to_sheet, dfs)
                                if pending_history: save_history(pending_history)
                                raise BlockedException(f"Block detected via extraction failure: {e}")
                            
                            if any(x in err_msg for x in ["target closed", "session", "page crashed", "browser_crashed_or_closed", "failed sending data to the peer", "connection reset"]):
                                emit_to_ui('WARN', f"Browser/Channel crash detected: {e}. Restarting context...")
                                await asyncio.sleep(2)
                                break 
                            
                            # Consecutive error counter for implicit block detection
                            if "extraction empty" in err_msg or "title/price missing" in err_msg:
                                consecutive_empty_errors += 1
                                emit_to_ui('WARN', f'Consecutive empty errors: {consecutive_empty_errors}/3')
                                if consecutive_empty_errors >= 3:
                                    emit_to_ui('ERR', f'3 consecutive empty extractions — likely a hard block!')
                                    await save_checkpoint(excel_file, updated_rows, url_to_sheet, dfs)
                                    if pending_history: save_history(pending_history)
                                    raise BlockedException("Implicit block: 3 consecutive empty extractions")
                    
                    # End of loop save
                    if pending_history: save_history(pending_history)
                    
                    if start_index >= len(urls):
                        break

                finally:
                    # Close context and delete profile directory
                    try:
                        await context.close()
                    except:
                        pass
                    try:
                        import shutil
                        if os.path.exists(profile_dir):
                            shutil.rmtree(profile_dir, ignore_errors=True)
                    except Exception:
                        pass

                    if start_index >= len(urls):
                        break


        except BlockedException:
            emit_to_ui('ERR', 'HARD BLOCK DETECTED. Marking profile as blocked and rotating...')
            mark_current_profile_blocked()
            try:
                import shutil
                if os.path.exists(profile_dir):
                    shutil.rmtree(profile_dir, ignore_errors=True)
            except Exception:
                pass

            # Wait a bit before rotating
            await asyncio.sleep(10)
            continue
            


        except Exception as e:
            emit_to_ui('ERR', f"Critical Session Error: {e}")
            await asyncio.sleep(10)
            # Try to restart?
            continue

    # ================= END RECOVERY LOOP =================
    
    emit_to_ui('INFO', f'SUMMARY: {active_count} activos, {inactive_count} dados de baja, {error_count} errores')
    
    if not updated_rows:
        emit_to_ui('WARN', 'No rows updated. Exiting.')
        return
    
    # 5. Save to Excel
    # 5. Save to Excel
    # Normalize base filename
    base = excel_file.replace('_updated_partial.xlsx', '.xlsx').replace('_updated.xlsx', '.xlsx')
    
    # Check if we completed all URLs
    is_complete = (len(updated_rows) >= len(urls)) # If we have same number of updated rows as source urls
    
    if is_complete:
        output_xlsx = base.replace('.xlsx', '_updated.xlsx')
        # If promoting to full, remove partial if exists
        partial_name = base.replace('.xlsx', '_updated_partial.xlsx')
        if os.path.exists(partial_name):
            try:
                os.remove(partial_name)
                emit_to_ui('INFO', f'Promoted partial file to full: {os.path.basename(partial_name)} -> {os.path.basename(output_xlsx)}')
            except: pass
    else:
        output_xlsx = base.replace('.xlsx', '_updated_partial.xlsx')
        if os.path.exists(STOP_FLAG_FILE):
             emit_to_ui('WARN', f'Process STOPPED by user. Saving {len(updated_rows)} URLs to: {os.path.basename(output_xlsx)}')
        else:
             emit_to_ui('WARN', f'Process incomplete ({len(updated_rows)}/{len(urls)}). Saving as partial.')

    emit_to_ui('INFO', f'Saving to: {os.path.basename(output_xlsx)}')
    
    emit_to_ui('INFO', f'Saving to: {os.path.basename(output_xlsx)}')
    
    try:
        # Write to Excel with Retry logic
        while True:
            try:
                save_merged_excel(output_xlsx, dfs, updated_rows)
                break
            except PermissionError:
                 emit_to_ui('WARN', f'File is busy: {os.path.basename(output_xlsx)}. Please close it.')
                 await asyncio.sleep(10)

        emit_to_ui('OK', f'DONE: {os.path.basename(excel_file)}')
        emit_to_ui('INFO', f'Saved {len(updated_rows)} properties, merged with original data.')
        
    except Exception as e:
        emit_to_ui('ERR', f"Error saving Excel (FATAL): {e}")
            
    emit_to_ui('OK', 'URL status update complete!')
    emit_to_ui('INFO', f'📊 CAPTCHAs solved/found: {captchas_solved}/{captchas_found}')
    
    if os.path.exists(JOURNAL_FILE):
        try:
            # Check if this journal belongs to current file before deleting? 
            # save_to_journal saves full_path.
            # load_journal filtered by full_path.
            # If we delete, we might lose other file progress?
            # Ideally we filter and rewrite. But for now, just delete is simpler if we assume single user.
            os.remove(JOURNAL_FILE)
        except KeyboardInterrupt:
            emit_to_ui('INFO', "Stopped by user.")
            emit_to_ui('INFO', "[STATUS] stopped")
        except Exception as e:
            emit_to_ui('ERR', f"Critical error: {e}")
            emit_to_ui('INFO', f"[STATUS] error: {str(e)}")
        finally:
            # Final cleanup or status? 
            # If we exited normally, Main blocks handling completion or loop finish
            pass
    
    if HAS_SOCKET and sio.connected:
        sio.disconnect()


if __name__ == "__main__":
    import json
    import argparse
    import sys
    from pathlib import Path # Import Path for file operations
    parser = argparse.ArgumentParser(description="Update URLs from Excel")
    parser.add_argument('excel_file', type=str, help='Path to Excel file')
    parser.add_argument('--sheets', type=str, default='[]', help='JSON list of sheet names')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--stealth', action='store_true', help='Legacy stealth flag')
    parser.add_argument('--mode', type=str, default='fast', choices=['fast', 'stealth'], help='Scraping mode')
    
    args = parser.parse_args()
    
    sheets = None
    if args.sheets:
        try:
             sheets = json.loads(args.sheets)
        except:
             pass
             
    # Clean flags on start
    if os.path.exists(PAUSE_FLAG_FILE): os.remove(PAUSE_FLAG_FILE)
    if os.path.exists(STOP_FLAG_FILE): os.remove(STOP_FLAG_FILE)

    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

    asyncio.run(update_urls(args.excel_file, sheets, args.resume))
