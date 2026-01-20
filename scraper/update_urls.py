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

# Add scraper directory to path
SCRAPER_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRAPER_DIR))

# Force UTF-8 for stdout/stderr to avoid Windows charmap errors
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from idealista_scraper.scraper import _goto_with_retry
from idealista_scraper.extractors import extract_detail_fields, missing_fields
from idealista_scraper.utils import log, play_captcha_alert
from idealista_scraper.config import FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE
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
    """Check if page shows CAPTCHA/bot protection based on page title."""
    try:
        title = (await page.title() or "").lower()
        is_captcha = any(kw in title for kw in [
            "attention", "moment", "challenge", "robot", "captcha",
            "access denied", "security", "peticiones", "verificación", "verification"
        ])
        return is_captcha
    except:
        return False


async def update_urls(excel_file: str, selected_sheets: list = None):
    """Update URL status from Excel file.
    
    Args:
        excel_file: Path to Excel file
        selected_sheets: List of sheet names to process (None = all sheets)
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
    
    # 3. Scrape each URL
    updated_rows = []
    active_count = 0
    inactive_count = 0
    error_count = 0
    
    async with async_playwright() as pw:
        # emit_to_ui('INFO', 'Launching browser...')
        browser = await pw.chromium.launch(
            headless=False, 
            args=["--start-maximized", "--disable-blink-features=AutomationControlled"]
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )
        page = await context.new_page()
        
        page = await context.new_page()
        
        for i, url in enumerate(urls, 1):
            # Check for pause
            while os.path.exists(PAUSE_FLAG_FILE):
                 # emit_to_ui('INFO', 'Paused...') # Too noisy if repeated
                 await asyncio.sleep(1)

            try:
                # Fast mode pre-delay
                await asyncio.sleep(random.uniform(*FAST_CARD_DELAY_RANGE))
                
                await _goto_with_retry(page, url)
                
                # Fast mode post-delay (instead of fixed 2s)
                await asyncio.sleep(random.uniform(*FAST_POST_CARD_DELAY_RANGE))
                # Wait a bit more for render if needed, but fast mode relies on post_delay
                # The original fast mode used 2.0s wait in scraper.py? 
                # Scraper logic was: await page.wait_for_timeout(PAGE_WAIT_MS) -> 250ms
                # But scraper_wrapper used: await asyncio.sleep(random.uniform(*post_card_delay))
                # Let's match scraper_wrapper logic + small render wait
                await asyncio.sleep(0.5) 
                
                
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
                miss = missing_fields(d) if d else ["all"]
                if miss or await detect_captcha(page):
                    emit_to_ui('WARN', f'({i}/{len(urls)}) CAPTCHA detectado. Resuelve el CAPTCHA manualmente en el navegador.')
                    
                    # Wait loop with repeating alarm until CAPTCHA is solved
                    captcha_resolved = False
                    while not captcha_resolved:
                        play_captcha_alert()
                        await asyncio.sleep(10)  # Wait 10 seconds before checking again
                        
                        # Check if CAPTCHA is solved
                        if not await detect_captcha(page):
                            # Re-extract data
                            d = await extract_detail_fields(page, debug_items=False)
                            miss = missing_fields(d) if d else ["all"]
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
    
    if HAS_SOCKET and sio.connected:
        sio.disconnect()


def main():
    import json
    parser = argparse.ArgumentParser(description='Update URL status from Excel file')
    parser.add_argument('excel_file', help='Path to Excel file to update')
    parser.add_argument('--sheets', default='[]', help='JSON array of sheet names to process')
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
    
    asyncio.run(update_urls(args.excel_file, selected_sheets))


if __name__ == "__main__":
    main()
