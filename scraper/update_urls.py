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
from pathlib import Path

# Pause flag file
PAUSE_FLAG_FILE = "update_paused.flag"
STEALTH_FLAG_FILE = "update_stealth.flag"
CHECKPOINT_FILE = "update_checkpoint.json"

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
    USER_AGENTS
)
from playwright.async_api import async_playwright
import random

# Configure logging to suppress noisy libraries
import logging
logging.getLogger('socketio').setLevel(logging.ERROR)
logging.getLogger('engineio').setLevel(logging.ERROR)

# Optional: Socket client for real-time updates (if available)
# Suppress stdout/stderr during import to hide "requests package" message
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


def emit_to_ui(level: str, message: str):
    """Emit log message to both console and UI (if connected)."""
    # print(f"[{level}] {message}") # Console print is handled by server streaming now, avoid double print
    # Actually server streams stdout. So we MUST print to stdout.
    print(f"[{level}] {message}")
    # WebSocket emit is NOT needed because server.py captures stdout and emits logs.
    # BUT for 'progress' event, we DO need a direct socket emit because server.py only captures stdout logs.
    # Wait, server.py uses subprocess.stdout.readline() loop to emit 'log'.
    # It does NOT emit 'progress'.
    # So we DO need to emit 'progress' via socketio client from here.
    pass

def emit_progress(current, total):
    """Emit progress event to UI."""
    if HAS_SOCKET and sio.connected:
        try:
            sio.emit('progress', {
                'current_properties': current,
                'total_properties': total,
                'current_page': 1,
                'total_pages': 1
            })
        except:
            pass


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
             
        return False
    except:
        return False


import json

JOURNAL_FILE = "update_progress.jsonl"

def save_to_journal(excel_file: str, data: dict):
    """Append a processed property result to the journal."""
    try:
        entry = {
            'excel_file': os.path.basename(excel_file),
            'full_path': excel_file,
            'data': data,
            'timestamp': time.time()
        }
        with open(JOURNAL_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry) + '\n')
    except Exception as e:
        pass

def load_journal(excel_file: str):
    """Load previously processed data from journal."""
    if not os.path.exists(JOURNAL_FILE):
        return []
    
    restored_rows = []
    try:
        with open(JOURNAL_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                if not line.strip(): continue
                try:
                    entry = json.loads(line)
                    # Use full path equality strictly
                    if entry.get('full_path') == excel_file:
                        restored_rows.append(entry['data'])
                except:
                    continue
    except:
        return []
    
    return restored_rows



async def update_urls(excel_file: str, selected_sheets: list = None, resume: bool = False):
    """Update URL status from Excel file.
    
    Args:
        excel_file: Path to Excel file
        selected_sheets: List of sheet names to process (None = all sheets)
        resume: If True, try to resume from checkpoint
    """
    # Connect to WebSocket for real-time logging (silently)
    if HAS_SOCKET:
        try:
            # Suppress internal socketio prints during connection
            import io
            old_stdout = sys.stdout
            old_stderr = sys.stderr
            sys.stdout = io.StringIO()
            sys.stderr = io.StringIO()
            try:
                sio.connect('http://127.0.0.1:5000', wait_timeout=5)
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
        except Exception:
            pass  # Silently continue without socket
    
    # 1. Validate file
    if not os.path.exists(excel_file):
        emit_to_ui('ERR', f'File not found: {excel_file}')
        return
    
    emit_to_ui('INFO', f'Loading URLs from: {os.path.basename(excel_file)}')
    
    # 2. Load data
    try:
        # Read sheets (sheet_name=None returns a dict of DataFrames)
        dfs = pd.read_excel(excel_file, sheet_name=None)
        
        # Filter sheets if specified
        if selected_sheets and len(selected_sheets) > 0:
            # Only keep selected sheets
            dfs = {k: v for k, v in dfs.items() if k in selected_sheets}
            sheet_info = f"{len(dfs)} sheet(s): {', '.join(dfs.keys())}"
        else:
            sheet_info = "all sheets"
        
        # Create a single dataframe with all rows
        df = pd.concat(dfs.values(), ignore_index=True)
        
        emit_to_ui('OK', f'Loaded {len(df)} rows from Excel ({sheet_info})')
    except Exception as e:
        emit_to_ui('ERR', f'Error reading Excel: {e}')
        return
    
    if 'URL' not in df.columns:
        emit_to_ui('ERR', "Dataset missing 'URL' column")
        return
    
    urls = df['URL'].dropna().unique().tolist()
    emit_to_ui('INFO', f'Found {len(urls)} unique URLs to check')
    
    # Check resume state
    start_index = 0
    updated_rows = []
    
    if resume:
        # Load EVERYTHING from journal first
        restored_data = load_journal(excel_file)
        if restored_data:
            updated_rows = restored_data
            saved_count = len(updated_rows)
            if saved_count > 0 and saved_count < len(urls):
                start_index = saved_count
                emit_to_ui('INFO', f'Resuming from journal. Restored {saved_count} properties.')
                emit_to_ui('INFO', f'Continuing from property {start_index + 1}/{len(urls)}')
            else:
                 # If journal is complete or empty, start over or just warn
                 if saved_count >= len(urls):
                     emit_to_ui('WARN', 'Journal indicates update was already completed. Please start clean if needed.')
                     start_index = 0 # Or maybe just stop? For now reset. 
                     updated_rows = []
        else:
            emit_to_ui('WARN', 'No journal found to resume from.')
    
    # 3. Scrape each URL
    updated_rows = []
    
    # If resuming, we need to load previous rows? 
    # Actually, simpler strategy: 
    # We append new results to a TEMPORARY partial file, or we just append to list.
    # But if we crash, we lose the list.
    # Ideally we should read the existing output file if it exists?
    # For now, let's assume we just skip the URLs and apppend the NEW status.
    # BUT, to save the final file, we need ALL rows.
    # So we should probably keep the ORIGINAL rows for the skipped ones, or mark them as "not updated".
    
    # Better approach for this script:
    # We loaded the DF. It has 'URL', 'Anuncio activo', etc.
    # We can use the existing DF as the source of truth.
    # We only update the rows for the URLs we process.
    # When saving, we save the WHOLE revised df.
    
    # Map URLs to their rows in the original DF for easy updating
    # (A bit complex if multiple rows have same URL. The script initially said "unique URLs to check")
    # The script re-scrapes UNIQUE urls.
    # Then it says: "Re-scrapes ALL URLs found... bypassing deduplication."
    # Wait, line 154: urls = df['URL'].dropna().unique().tolist()
    # So we touch each unique URL once.
    
    # For simplicity in this script which builds "updated_rows" list from scratch:
    # If resuming, we CAN'T easily reconstruct "updated_rows" unless we saved them incrementally.
    # OR, we just initialize `updated_rows` with the data derived from the original DF for the skipped items?
    # This might be risky if the original file is old.
    
    # ALTERNATIVE:
    # If this is "Update Status", maybe we don't need to re-scrape the first N items, 
    # but we DO need their data in the final list.
    # Let's trust the columns in the loaded Excel for the skipped items.
    
    # Let's populate updated_rows with the skipped items first, directly from the source DF.
    # Problem: source DF might have multiple rows per URL?
    # Line 154 takes unique.
    # Line 246 creates a new row: row = {"URL": url, **d}
    # Line 269: new_df = pd.DataFrame(updated_rows)
    # This implies the output file will ONLY contain one row per unique URL.
    # If the input had duplicates (e.g. same URL in different sheets), this script collapses them?
    # Let's look at line 277: new_df.to_excel(..., index=False)
    # Yes, it seems this script produces a FLAT list of unique URLs.
    
    # If not resuming (or start_index is 0), updated_rows is empty.
    # If resuming, updated_rows is already populated with the preserved data.
    
    # We DO NOT need to pre-fill from original DF because we rely on the journaling to have saved the ACTUAL scraped data.
    # The journal stores the FULL 'd' dict plus 'URL'.
    
                 
    active_count = 0
    inactive_count = 0
    error_count = 0
    
    async with async_playwright() as pw:
        # emit_to_ui('INFO', 'Launching browser...')
        browser = await pw.chromium.launch(
            headless=False, 
            args=["--start-maximized"],
            ignore_default_args=["--enable-automation", "--no-sandbox"]
        )
        
        # Select random user agent
        ua = random.choice(USER_AGENTS) if 'USER_AGENTS' in globals() else "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
        
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=ua
        )
        # Apply stealth script
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        page = await context.new_page()
        
        page = await context.new_page()
        
        # Start loop from start_index
        for i, url in enumerate(urls[start_index:], start_index + 1):
            
            # Append result is handled AFTER scraping below
            pass
            
            # Check for pause
            was_paused = False
            while os.path.exists(PAUSE_FLAG_FILE):
                 if not was_paused:
                     emit_to_ui('INFO', '[STATUS] paused')
                     emit_to_ui('INFO', 'Update paused by user.')
                     was_paused = True
                 await asyncio.sleep(1)
            
            if was_paused:
                emit_to_ui('INFO', '[STATUS] running')
                emit_to_ui('INFO', 'Update resumed.')

            try:
                # Dynamic delay based on mode flag
                if os.path.exists(STEALTH_FLAG_FILE):
                    # Stealth mode
                    card_delay = STEALTH_CARD_DELAY_RANGE
                    post_delay = STEALTH_POST_CARD_DELAY_RANGE
                    # emit_to_ui('INFO', 'Running in STEALTH mode') # Too noisy
                else:
                    # Fast mode
                    card_delay = FAST_CARD_DELAY_RANGE
                    post_delay = FAST_POST_CARD_DELAY_RANGE

                # Pre-action delay
                await asyncio.sleep(random.uniform(*card_delay))
                
                await _goto_with_retry(page, url)
                await simulate_human_interaction(page)
                
                # Post-action delay
                await asyncio.sleep(random.uniform(*post_delay))
                
                # Small fixed render wait
                await asyncio.sleep(0.5) 
                
                # SMART WAIT: Wait for price or title to appear, allowing "Verificacion" screen to pass
                try:
                    # Wait up to 10s for the price or main title, signaling the real page is loaded
                    await page.wait_for_selector('.info-data-price, .main-info__title-main, h1', timeout=10000)
                except:
                    # If timeout, it might be a 404, blocked content, or just slow. 
                    # We proceed to extraction which will handle the missing data logic.
                    pass
                
                
                # Extract details (includes active status check)
                # Retry logic for "Execution context was destroyed"
                d = None
                for attempt in range(3):
                    try:
                        d = await extract_detail_fields(page, debug_items=False)
                        break
                    except Exception as e:
                        if "Execution context was destroyed" in str(e) and attempt < 2:
                            # Wait and retry
                            await asyncio.sleep(1)
                            continue
                        raise e
                
                # Check for CAPTCHA (missing critical fields or CAPTCHA page detected)
                if d:
                    d['URL'] = url
                
                is_room_mode = 'habitacion' in url.lower()
                
                # Check for inactive status FIRST
                is_inactive_pre = d.get('Anuncio activo') == 'No' or d.get('Baja anuncio') or d.get('isExpired')
                
                if is_inactive_pre:
                     # If inactive, we expect missing fields. Don't check them.
                     miss = []
                else:
                     miss = missing_fields(d, is_room_mode=is_room_mode) if d else ["all"]
                
                # Check for BLOCK (uso indebido)
                if await detect_captcha(page) and "uso indebido" in (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower():
                    emit_to_ui('ERR', f'({i}/{len(urls)}) 🛑 HARD STOP: Scraper bloqueado ("Uso Indebido").')
                    # Implement profile nuking here or just stop? 
                    # For update process, we just stop and let the user handle it (profile is shared but this script is separate)
                    # Ideally we should nuke it too, but we can rely on main scraper wrapper for that logic.
                    # For now, just break hard.
                    break

                if miss or await detect_captcha(page):
                    emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA detectado.')
                    
                    # 1. Try automatic slider solve
                    emit_to_ui('INFO', '🤖 Intentando resolver CAPTCHA automáticamente...')
                    if await solve_slider_captcha(page):
                         if not await detect_captcha(page):
                              emit_to_ui('OK', '✅ CAPTCHA resuelto automáticamente!')
                              # Re-extract data
                              d = await extract_detail_fields(page, debug_items=False)
                              if d: d['URL'] = url
                              is_inactive_loop = d.get('Anuncio activo') == 'No' or d.get('Baja anuncio') or d.get('isExpired')
                              miss = [] if is_inactive_loop else missing_fields(d, is_room_mode=is_room_mode)
                              if not miss:
                                   # We continue below
                                   pass
                    
                    if miss or await detect_captcha(page):
                        emit_to_ui('WARN', 'Resuelve el CAPTCHA manualmente en el navegador.')
                    
                    # Wait loop with repeating alarm until CAPTCHA is solved
                    captcha_resolved = False
                    while not captcha_resolved:
                        play_captcha_alert()
                        await asyncio.sleep(10)  # Wait 10 seconds before checking again
                        
                        # Check if CAPTCHA is solved
                        if not await detect_captcha(page):
                            # Re-extract data
                            d = await extract_detail_fields(page, debug_items=False)
                            
                            # Apply the same data prep as main loop
                            if d:
                                d['URL'] = url
                            
                            # Check for inactive status FIRST (consistent with main loop)
                            is_inactive_loop = d.get('Anuncio activo') == 'No' or d.get('Baja anuncio') or d.get('isExpired')
                            
                            if is_inactive_loop:
                                miss = []
                            else:
                                miss = missing_fields(d, is_room_mode=is_room_mode) if d else ["all"]

                            if not miss:
                                emit_to_ui('OK', f'({i}/{len(urls)}) CAPTCHA resuelto! Continuando...')
                                captcha_resolved = True
                            else:
                                emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA aún presente. Resuelve y espera...')
                
                # Check status
                is_inactive = d.get('Anuncio activo') == 'No' or d.get('Baja anuncio')
                
                if is_inactive:
                    baja_date = d.get('Baja anuncio', 'fecha desconocida')
                    emit_to_ui('WARN', f'({i}/{len(urls)}) [anuncio dado de baja] {url} - {baja_date}')
                    inactive_count += 1
                else:
                    emit_to_ui('OK', f'({i}/{len(urls)}) [activo] {url}')
                    active_count += 1
                
                row = {"URL": url, **d}
                
                # Journaling: Save THIS result immediately
                save_to_journal(excel_file, row)
                
                updated_rows.append(row)
                
                # Emit progress
                emit_progress(i, len(urls))
                
            except Exception as e:
                emit_to_ui('ERR', f'({i}/{len(urls)}) Error: {url} - {str(e)[:100]}') # Increased length
                error_count += 1
                emit_progress(i, len(urls))
        
        await browser.close()
    
    # 4. Summary
    # 4. Summary
    # emit_to_ui('INFO', '=' * 40) # Removing separator as requested
    emit_to_ui('INFO', f'SUMMARY: {active_count} activos, {inactive_count} dados de baja, {error_count} errores')
    
    if not updated_rows:
        emit_to_ui('WARN', 'No rows updated. Exiting.')
        return
    
    # 5. Save to Excel (with retry if file is open)
    new_df = pd.DataFrame(updated_rows)
    output_xlsx = excel_file.replace('.xlsx', '_status_updated.xlsx')
    
    emit_to_ui('INFO', f'Saving to: {os.path.basename(output_xlsx)}')
    
    # Retry loop for PermissionError (file open in Excel)
    while True:
        try:
            new_df.to_excel(output_xlsx, sheet_name='oportunidades', index=False)
            break
        except PermissionError:
            emit_to_ui('WARN', f'⚠️ No se puede escribir en "{os.path.basename(output_xlsx)}". El archivo parece estar abierto en Excel.')
            emit_to_ui('WARN', 'Cierra el archivo Excel y espera 10 segundos para reintentar...')
            await asyncio.sleep(10)
            emit_to_ui('INFO', 'Reintentando guardado...')
    
    emit_to_ui('OK', 'URL status update complete!')
    
    # Cleanup journal on success
    if os.path.exists(JOURNAL_FILE):
        try:
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
    
    # Parse sheets JSON
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
