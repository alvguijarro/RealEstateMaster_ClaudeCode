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

# Pause flag file
PAUSE_FLAG_FILE = "update_paused.flag"
STOP_FLAG_FILE = "update_stop.flag"
STEALTH_FLAG_FILE = "update_stealth.flag"
JOURNAL_FILE = "update_progress.jsonl"
ENRICHED_HISTORY_FILE = "enriched_history.json" # Local cache of enriched data
STEALTH_PROFILE_DIR = str(Path(__file__).parent.parent / "stealth_profile")

# Add scraper directory to path
SCRAPER_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRAPER_DIR))

# Force UTF-8 for stdout/stderr to avoid Windows charmap errors
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from idealista_scraper.scraper import _goto_with_retry
from idealista_scraper.extractors import extract_detail_fields, missing_fields
from idealista_scraper.utils import log, play_captcha_alert, simulate_human_interaction, solve_slider_captcha
from idealista_scraper.config import (
    FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE,
    STEALTH_CARD_DELAY_RANGE, STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SESSION_LIMIT, EXTRA_STEALTH_REST_DURATION_RANGE,
    EXTRA_STEALTH_COFFEE_BREAK_RANGE, EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY,
    SCROLL_STEPS, EXTRA_STEALTH_SCROLL_PAUSE_RANGE,
    USER_AGENTS
)
from playwright.async_api import async_playwright

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

def handle_blocked_profile():
    """Archive the current profile if it has been blocked/poisoned."""
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"stealth_profile_BLOCKED_{timestamp}"
    backup_path = os.path.join(os.path.dirname(STEALTH_PROFILE_DIR), backup_name)
    
    emit_to_ui("WARN", "PROFILE POISONED: Dealing with blocked profile...")
    
    if os.path.exists(STEALTH_PROFILE_DIR):
        try:
            shutil.move(STEALTH_PROFILE_DIR, backup_path)
            emit_to_ui("WARN", f"Moved poisoned profile to: {backup_name}")
            emit_to_ui("OK", "Next run will generate a fresh, clean profile.")
        except Exception as e:
            emit_to_ui("ERR", f"Failed to archive profile: {e}")

async def detect_captcha(page) -> bool:
    """Check if page shows CAPTCHA/bot protection based on page title and body."""
    try:
        title = (await page.title() or "").lower()
        is_captcha_title = any(kw in title for kw in [
            "attention", "moment", "challenge", "robot", "captcha",
            "access denied", "security", "peticiones", "verificación", "verification"
        ])
        
        if is_captcha_title:
             return True

        # Check body text for "uso indebido"
        page_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        text_lower = page_text.lower()
        if "uso indebido" in text_lower or "se ha bloqueado" in text_lower or "access denied" in text_lower:
             return True
        if "recibiendo muchas peticiones" in text_lower or "desliza hacia la derecha" in text_lower:
             return True
             
        return False
    except:
        return False

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
    output_xlsx = base.replace('.xlsx', '_updated_partial.xlsx')
    
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
            import io
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            try:
                sio.connect('http://127.0.0.1:5003', wait_timeout=5)
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
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
        dfs = pd.read_excel(excel_file, sheet_name=None)
        
        # Filter sheets if requested
        if selected_sheets and len(selected_sheets) > 0:
            dfs = {k: v for k, v in dfs.items() if k in selected_sheets}
            sheet_info = f"{len(dfs)} sheet(s): {', '.join(dfs.keys())}"
        else:
            sheet_info = "all sheets"
        
        # Create URL to Sheet and URL to Original Row maps
        url_to_sheet = {}
        url_to_row = {}
        all_rows = []
        
        for sheet_name, df_sheet in dfs.items():
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
        emit_to_ui('ERR', "No rows found in Excel.")
        return
    
    # Use the keys from our map as the master list
    urls = list(url_to_row.keys())
    emit_to_ui('INFO', f'Found {len(urls)} unique URLs to check')
    
    # Check resume state
    start_index = 0
    updated_rows = []
    
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
    while start_index < len(urls):
        try:
            async with async_playwright() as pw:
                emit_to_ui('INFO', 'Launching persistent browser...')
                
                # Use persistent context to match main scraper behavior
                os.makedirs(STEALTH_PROFILE_DIR, exist_ok=True)
                
                browser_args = [
                    "--start-maximized",
                    "--disable-dev-shm-usage",
                    "--disable-infobars",
                    "--disable-extensions",
                    "--no-first-run",
                    "--no-default-browser-check",
                    "--disable-popup-blocking",
                ]
                
                # Randomize viewport slightly to avoid identical fingerprint
                width = random.randint(1200, 1600)
                height = random.randint(800, 1000)

                # --- PRE-SCAN FOR HISTORY/EXISTING --
                # To ensure UI reflects progress immediately (e.g. "34/100" if 34 are already done),
                # we scan HEAD of the list for contiguous enriched items.
                HISTORY = load_history()
                pending_history = {}
                
                scan_idx = start_index 
                pre_processed_rows = []
                skipped_count = 0

                # Helper to check if row is "enriched enough" (basic check)
                def is_enriched(r):
                    # If it has Price and m2 and isn't empty, it's likely enriched. 
                    # Or we rely on HISTORY presence.
                    # User request explicitly mentions "scraper skips", which refers to HISTORY logic.
                    return False 

                for k, url in enumerate(urls[start_index:], start_index):
                    # Check History
                    in_history = url in HISTORY
                    
                    if in_history:
                        emit_to_ui('INFO', f'({k+1}/{len(urls)}) [SKIP] Enriched in history: {url}')
                        
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
                        # Stop at first non-enriched to maintain sequence 
                        # (or we could skip non-contiguously, but start_index implies linear start)
                        # Actually, the scraping loop iterates linearly. 
                        # If we have [Done, Done, Todo, Done], catching the first 2 is good.
                        # Catching the 4th is harder with 'start_index' logic unless we complexify.
                        # Let's stick to contiguous prefix for 'start_index' optimization.
                        break
                
                if skipped_count > 0:
                    emit_to_ui('INFO', f'Skipping {skipped_count} previously enriched properties...')
                    emit_progress(scan_idx, len(urls), "Pre-processing...", os.path.basename(excel_file))
                    start_index = scan_idx
                # ------------------------------------

                # Launch PERSISTENT context
                try:
                    context = await pw.chromium.launch_persistent_context(
                        user_data_dir=STEALTH_PROFILE_DIR,
                        headless=False,
                        args=browser_args,
                        ignore_default_args=["--enable-automation", "--no-sandbox"],
                        
                        viewport={"width": width, "height": height},
                        user_agent=random.choice(USER_AGENTS)
                    )
                    
                    # Mask webdriver
                    await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
                    
                    page = context.pages[0] if context.pages else await context.new_page()
                    
                    # Initial warm-up if needed
                    if start_index == 0:
                        try:
                            await page.goto("https://www.idealista.com", timeout=30000)
                            await asyncio.sleep(2)
                            # Accept cookies
                            await page.evaluate(r"""() => {
                                const acceptBtn = document.querySelector('#didomi-notice-agree-button, [id*="accept"], .onetrust-accept-btn');
                                if (acceptBtn) acceptBtn.click();
                            }""")
                        except:
                            pass
                    

                    # --- PROCESSING LOOP ---
                    # HISTORY loaded above
                    
                    # Stealth Counters
                    session_property_count = 0
                    next_coffee_break = random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
                    
                    # --- PROCESSING LOOP ---
                    for i, url in enumerate(urls[start_index:], start_index + 1):
                        current_list_idx = i - 1 
                        
                        # Handle Pause
                        was_paused = False
                        while os.path.exists(PAUSE_FLAG_FILE):
                             if os.path.exists(STOP_FLAG_FILE):
                                 break
                             if not was_paused:
                                 emit_to_ui('INFO', '[STATUS] paused')
                                 was_paused = True
                             await asyncio.sleep(1)
                        
                        if os.path.exists(STOP_FLAG_FILE):
                            emit_to_ui('WARN', 'Stop signal received. Saving partial progress...')
                            break 
                            
                        if was_paused:
                            emit_to_ui('INFO', '[STATUS] running')
                            
                        # --- SMART SKIP: Check History ---
                        if url in HISTORY:
                            emit_to_ui('INFO', f'({i}/{len(urls)}) [SKIP] Enriched in history: {url}')
                            # We still need to add this row to 'updated_rows' so it ends up in the output Excel!
                            # Since we don't have the full data in HISTORY (just key check?), 
                            # we must rely on what we have.
                            # WAIT: If user wants to "Skip", they assume data is present?
                            # "Enriquecedor mirará en este archivo antes de hacer el scraping para completar los campos"
                            # This implies HISTORY stores the DATA.
                            # Our save_history implementation DOES store data (we pass a dict).
                            # So we retrieve it.
                            d_history = HISTORY.get(url, {})
                            
                            # We merge history data with original row, similar to active/overwrite logic
                            # But since it's "history", we assume it's the latest good state.
                            orig_row = url_to_row.get(url, {})
                            final_row = orig_row.copy()
                            final_row['URL'] = url
                            
                            # Merge history data (it should override original raw data)
                            # But we also respect the "Overwrite" logic which might have stripped stale fields.
                            # So really, final_row IS the history data.
                            # But we need to ensure 'Ciudad', 'exterior', 'Fecha Scraping' are preserved from ORIGINAL if missing in history?
                            # No, if it's in history, it was already enriched correctly (with preservation logic applied THEN).
                            # So we just use history data.
                            # BUT, we might need to backfill 'Ciudad' if history is from a different run?
                            # Let's assume history is the master record.
                            final_row.update(d_history)
                            
                            updated_rows.append(final_row)
                            emit_progress(i, len(urls), url_to_sheet.get(url, 'Unknown'), os.path.basename(excel_file))
                            # Don't sleep if skipping
                            start_index = i
                            continue

                        try:
                            # Use Extra Stealth if flag is present (User requested "Like Scraper Tool")
                            if os.path.exists(STEALTH_FLAG_FILE):
                                card_delay = EXTRA_STEALTH_CARD_DELAY_RANGE
                                post_delay = EXTRA_STEALTH_POST_CARD_DELAY_RANGE
                            else:
                                card_delay = FAST_CARD_DELAY_RANGE
                                post_delay = FAST_POST_CARD_DELAY_RANGE
                                
                                posts_delay = FAST_POST_CARD_DELAY_RANGE
                                
                            # --- STEALTH: Coffee Break & Session Rest ---
                            if os.path.exists(STEALTH_FLAG_FILE):
                                # 1. Coffee Break
                                if session_property_count >= next_coffee_break:
                                    break_duration = random.uniform(*EXTRA_STEALTH_COFFEE_BREAK_RANGE)
                                    emit_to_ui('INFO', f'Coffee break: Pausing for {int(break_duration)}s...')
                                    await asyncio.sleep(break_duration)
                                    next_coffee_break = session_property_count + random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)

                                # 2. Session Rest (Long Pause)
                                if session_property_count >= EXTRA_STEALTH_SESSION_LIMIT:
                                    rest_duration = random.uniform(*EXTRA_STEALTH_REST_DURATION_RANGE)
                                    rest_mins = int(rest_duration / 60)
                                    emit_to_ui('WARN', f'Session limit reached ({EXTRA_STEALTH_SESSION_LIMIT}). Resting for {rest_mins} mins...')
                                    
                                    # Countdown log
                                    remaining = rest_duration
                                    while remaining > 0:
                                        if remaining % 60 == 0: # Log every minute
                                             emit_to_ui('INFO', f'Resting... {int(remaining/60)} mins remaining.')
                                        await asyncio.sleep(min(10, remaining))
                                        remaining -= 10
                                    
                                    session_property_count = 0 # Reset counter
                                    emit_to_ui('INFO', 'Rest complete. Resuming session.')

                            await asyncio.sleep(random.uniform(*card_delay))
                            
                            # Navigate
                            await _goto_with_retry(page, url)
                            
                            # Check Block immediately
                            page_text = await page.evaluate("() => document.body ? document.body.innerText.toLowerCase() : ''")
                            if "uso indebido" in page_text or "se ha bloqueado" in page_text:
                                raise BlockedException("Uso Indebido detected")
                                
                            # Enhanced Stealth Scroll
                            if os.path.exists(STEALTH_FLAG_FILE):
                                await variable_scroll(page)
                            else:
                                await simulate_human_interaction(page)
                                
                            await asyncio.sleep(random.uniform(*post_delay))
                            
                            # Extract
                            d = None
                            for attempt in range(3):
                                try:
                                    d = await extract_detail_fields(page, debug_items=False)
                                    if d and d.get('isBlocked'):
                                        raise BlockedException("Uso Indebido detected (via extractor)")
                                    
                                    # Check data integrity - if empty, we might be blocked or page failed to load
                                    if not d or (not d.get('Titulo') and not d.get('price')):
                                         # If page loaded but no title/price, it's likely a captcha we missed or a broken page
                                         # Let's check captcha one more time
                                         if await detect_captcha(page):
                                             raise BlockedException("Hidden CAPTCHA detected")
                                         else:
                                             # If really no data, maybe it's just a failure? 
                                             # But we shouldn't save it as "Active" with empty data.
                                             # If active=No (because extractor detected it, e.g. "anuncio desactivado"), that's fine.
                                             # But if simply empty, we check for "No encontramos lo que estás buscando"
                                             page_not_found_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                                             if "No encontramos lo que estás buscando" in page_not_found_text or "el anuncio ya no está en nuestra base de datos" in page_not_found_text:
                                                  # Explicitly not found = Inactive
                                                  if not d: d = {}
                                                  d['Anuncio activo'] = 'No'
                                                  d['Baja anuncio'] = 'desconocida'
                                             else:
                                                  # Raise exception to trigger retry or skip without saving bad data.
                                                  raise Exception("Extraction returned empty data (Title/Price missing)")
                                             raise Exception("Extraction returned empty data (Title/Price missing)")
                                    
                                    break
                                except BlockedException:
                                    raise 
                                except Exception as e:
                                    if "Execution context was destroyed" in str(e) and attempt < 2:
                                        await asyncio.sleep(1)
                                        continue
                                    raise e

                            # Check Block again
                            if await detect_captcha(page):
                                if "uso indebido" in (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower():
                                    raise BlockedException("Uso Indebido detected")
                                
                                emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA detectado.')
                                emit_to_ui('INFO', 'Intentando resolver CAPTCHA automáticamente...')
                                if await solve_slider_captcha(page):
                                     if not await detect_captcha(page):
                                          emit_to_ui('OK', 'CAPTCHA resuelto automáticamente!')
                                          d = await extract_detail_fields(page, debug_items=False)
                                          if d and d.get('isBlocked'):
                                              raise BlockedException("Uso Indebido detected (via extractor)")
                                
                                if await detect_captcha(page):
                                    emit_to_ui('WARN', 'Resuelve el CAPTCHA manualmente en el navegador.')
                                    while await detect_captcha(page):
                                        play_captcha_alert()
                                        await asyncio.sleep(5)
                                    d = await extract_detail_fields(page, debug_items=False)
                                    if d and d.get('isBlocked'):
                                        raise BlockedException("Uso Indebido detected (via extractor)")
                            
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
                                emit_to_ui('WARN', f'({i}/{len(urls)}) [baja] {url}')
                                inactive_count += 1
                                # Inactive: Preserve original data, update status only
                                final_row['Anuncio activo'] = 'No'
                                if baja_date:
                                    final_row['Baja anuncio'] = baja_date
                            else:
                                emit_to_ui('OK', f'({i}/{len(urls)}) [activo] {url}')
                                active_count += 1
                                
                                # Active: OVERWRITE MODE
                                # Create new row from fresh data 'd', keeping only URL
                                # User Request: Preserve 'Ciudad', 'exterior', 'Fecha Scraping'
                                preserved_cols = ['Ciudad', 'exterior', 'Fecha Scraping']
                                
                                final_row = d.copy() # Start fresh with scraped data
                                final_row['URL'] = url
                                
                                for col in preserved_cols:
                                    val = orig_row.get(col)
                                    if val is not None and pd.notna(val) and str(val).strip() != "":
                                         final_row[col] = val
                                         
                                # Also ensure we don't have a 'Baja anuncio' date if it is active
                                if 'Baja anuncio' in final_row:
                                    del final_row['Baja anuncio']
                            
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
                            updated_rows.append(final_row)
                            
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
                            start_index = i 
                            if "Target closed" in str(e) or "session" in str(e).lower():
                                await asyncio.sleep(2)
                                break 
                    
                    # End of loop save
                    if pending_history: save_history(pending_history)
                    
                    if start_index >= len(urls):
                        break

                finally:
                    # Close context
                    try:
                        await context.close()
                    except:
                        pass 
                            
                    if start_index >= len(urls):
                        break


        except BlockedException:
            emit_to_ui('ERR', 'HARD STOP: Scraper blocked. Entering recovery mode...')
            handle_blocked_profile() # Backup bad profile
            
            # Explicitly nuke the directory to force fresh browser identity
            import shutil
            if os.path.exists(STEALTH_PROFILE_DIR):
                try:
                    shutil.rmtree(STEALTH_PROFILE_DIR)
                    emit_to_ui('INFO', 'Identity wiped. Next run will use a fresh fingerprint.')
                except:
                    pass
            
            # 15 Minute Wait Loop (Recursive Strategy)
            wait_time = 900 # 15 minutes
            emit_to_ui('WARN', f'RECOVERY: Pausing for {wait_time/60:.0f} minutes...')
            emit_to_ui('WARN', 'Process will automatically resume after cooldown.')
            
            await asyncio.sleep(wait_time)
            
            emit_to_ui('INFO', 'Cooldown complete. Resuming session...')
            continue # Retry the exact same URL that failed (start_index wasn't incremented)
            


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
    
    if os.path.exists(JOURNAL_FILE):
        try:
            # Check if this journal belongs to current file before deleting? 
            # save_to_journal saves full_path.
            # load_journal filtered by full_path.
            # If we delete, we might lose other file progress?
            # Ideally we filter and rewrite. But for now, just delete is simpler if we assume single user.
            os.remove(JOURNAL_FILE)
        except:
            pass
    
    if HAS_SOCKET and sio.connected:
        sio.disconnect()


def main():
    import json
    parser = argparse.ArgumentParser(description='Update URL status from Excel file')
    parser.add_argument('excel_file', help='Path to Excel file to update')
    parser.add_argument('--sheets', default='[]', help='JSON array of sheet names to process')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    args = parser.parse_args()
    
    try:
        selected_sheets = json.loads(args.sheets)
        if not isinstance(selected_sheets, list):
            selected_sheets = []
    except:
        selected_sheets = []
    
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    asyncio.run(update_urls(args.excel_file, selected_sheets, args.resume))


if __name__ == "__main__":
    main()
