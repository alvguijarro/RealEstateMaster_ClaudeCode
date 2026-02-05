"""Flask-SocketIO web server for the Idealista scraper.

Provides REST API endpoints and WebSocket communication for real-time updates.
"""
from __future__ import annotations

import os
import sys
import asyncio
import threading
import json
import subprocess
import time
from datetime import datetime
from pathlib import Path

from flask import Flask, render_template, jsonify, request, send_file
from flask_socketio import SocketIO

# Add parent directory to path for idealista_scraper imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.scraper_wrapper import ScraperController, DEFAULT_OUTPUT_DIR

app = Flask(__name__, static_folder='static', template_folder='static')
app.config['SECRET_KEY'] = 'idealista-scraper-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Allow embedding in iframes and add CORS headers for polling
@app.after_request
def after_request(response):
    response.headers.pop('X-Frame-Options', None)
    # Add CORS headers for cross-origin polling from dashboard
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Methods'] = 'GET, POST, OPTIONS'
    response.headers['Access-Control-Allow-Headers'] = 'Content-Type'
    return response

@app.route('/health', methods=['GET', 'OPTIONS'])
def health_check():
    """Simple health check endpoint for service readiness polling."""
    return jsonify({'status': 'ok'})

# Global scraper controller instance
scraper_controller: ScraperController | None = None
update_process = None

# Scrape history storage
HISTORY_FILE = Path(__file__).parent.parent / "scrape_history.json"
scrape_history: list = []


def load_history():
    """Load scrape history from file."""
    global scrape_history
    if HISTORY_FILE.exists():
        try:
            with open(HISTORY_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Ensure data is a list, not a dict or other type
                if isinstance(data, list):
                    scrape_history = data
                else:
                    scrape_history = []
        except Exception:
            scrape_history = []
    return scrape_history


def save_history():
    """Save scrape history to file."""
    try:
        with open(HISTORY_FILE, 'w', encoding='utf-8') as f:
            json.dump(scrape_history, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"Error saving history: {e}")


def add_history_entry(seed_url: str, properties_count: int, category: str, output_file: str):
    """Add a new entry to scrape history."""
    entry = {
        'timestamp': datetime.now().isoformat(),
        'seed_url': seed_url,
        'properties_count': properties_count,
        'category': category,
        'output_file': output_file,
        'filename': os.path.basename(output_file) if output_file else None
    }
    scrape_history.insert(0, entry)  # Add to beginning (newest first)
    save_history()
    # Emit to connected clients
    socketio.emit('history_update', entry)
    return entry


# Load history on startup
load_history()


def emit_log(level: str, message: str):
    """Send log message to all connected clients."""
    socketio.emit('log', {'level': level, 'message': message})


def emit_property(data: dict):
    """Send scraped property data to all connected clients."""
    socketio.emit('property_scraped', data)


def emit_progress(data: dict):
    """Send progress update (pages/properties) to all connected clients."""
    socketio.emit('progress_update', data)


def emit_browser_closed():
    """Notify clients that browser was closed by user. Scraper is paused awaiting decision."""
    socketio.emit('browser_closed', {'message': 'Browser was closed. Resume or stop?'})


def emit_status(status: str, **kwargs):
    """Send status update to all connected clients."""
    socketio.emit('status_change', {'status': status, **kwargs})
    
    # When completed OR stopped, add to history
    if status in ('completed', 'stopped') and scraper_controller:
        # Only add to history if there are scraped properties
        if scraper_controller.scraped_properties:
            category = scraper_controller._detected_sheet or 'unknown'
            add_history_entry(
                seed_url=scraper_controller.seed_url,
                properties_count=len(scraper_controller.scraped_properties),
                category=category,
                output_file=scraper_controller.output_file or ''
            )


@app.route('/')
def index():
    """Serve the main HTML interface with cache busting."""
    import time
    return render_template('index.html', cache_bust=int(time.time()))


@app.route('/api/config', methods=['GET'])
def get_config():
    """Get default configuration values."""
    return jsonify({
        'default_output_dir': DEFAULT_OUTPUT_DIR
    })


@app.route('/api/start', methods=['POST'])
def start_scraping():
    """Start a new scraping session."""
    global scraper_controller
    
    data = request.get_json()
    seed_url = data.get('seed_url', '').strip()
    mode = data.get('mode', 'stealth')  # 'stealth' or 'fast'
    dual_mode = data.get('dual_mode', False)
    output_dir = data.get('output_dir', '').strip() or DEFAULT_OUTPUT_DIR
    
    if not seed_url:
        return jsonify({'error': 'Seed URL is required'}), 400
    
    if not seed_url.startswith('http'):
        seed_url = 'https://' + seed_url
    
    if 'idealista.com' not in seed_url:
        return jsonify({'error': 'URL must be from idealista.com'}), 400
    
    # Stop any existing scraper
    if scraper_controller and scraper_controller.is_running:
        scraper_controller.stop()
    
    # For DUAL MODE: Calculate the second URL now
    dual_mode_url = None
    if dual_mode:
        if '/alquiler-viviendas/' in seed_url:
            dual_mode_url = seed_url.replace('/alquiler-viviendas/', '/venta-viviendas/')
        elif '/venta-viviendas/' in seed_url:
            dual_mode_url = seed_url.replace('/venta-viviendas/', '/alquiler-viviendas/')
    
    # Create controller with dual_mode_url if applicable
    scraper_controller = ScraperController(
        seed_url=seed_url,
        output_dir=output_dir,
        mode=mode,
        dual_mode_url=dual_mode_url,  # Pass second URL for same-browser execution
        on_log=emit_log,
        on_property=emit_property,
        on_status=emit_status,
        on_progress=emit_progress,
        on_browser_closed=emit_browser_closed,
    )
    
    # Start scraping in background thread
    def run_scraper():
        global scraper_controller
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            # Run scraper (handles both phases internally if dual_mode_url is set)
            loop.run_until_complete(scraper_controller.run())
        finally:
            loop.close()
    
    thread = threading.Thread(target=run_scraper, daemon=True)
    thread.start()
    
    return jsonify({'status': 'started', 'mode': mode, 'dual_mode': dual_mode})


@app.route('/api/set_mode', methods=['POST'])
def set_mode():
    """Update scraping mode dynamically."""
    data = request.get_json()
    mode = data.get('mode')
    
    if mode not in ['fast', 'stealth']:
        return jsonify({'error': 'Invalid mode'}), 400
    
    if scraper_controller:
        scraper_controller.set_mode(mode)
    
    # Also toggle flag for the standalone update_urls.py script
    try:
        update_script_dir = Path(__file__).parent.parent
        flag_file = update_script_dir / "update_stealth.flag"
        
        if mode == 'stealth':
            flag_file.touch()
        elif mode == 'fast':
            if flag_file.exists():
                flag_file.unlink()
    except Exception as e:
        print(f"Error toggling update mode flag: {e}")

    return jsonify({'status': 'mode_updated', 'mode': mode})





@app.route('/api/server/stop', methods=['POST'])
def stop_server():
    """Execute STOP_SILENT.bat to stop all services (no browser)."""
    try:
        base_dir = Path(__file__).parent.parent.parent
        bat_file = base_dir / 'scripts' / 'STOP_SILENT.bat'
        if bat_file.exists():
            subprocess.Popen(['cmd', '/c', str(bat_file)], cwd=str(base_dir),
                           creationflags=subprocess.CREATE_NO_WINDOW)
            return jsonify({'status': 'stopped', 'message': 'Server stopping...'})
        return jsonify({'error': 'STOP_SILENT.bat not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/server/restart', methods=['POST'])
def restart_server():
    """Execute RESTART_SILENT.bat to restart all services (no browser)."""
    try:
        base_dir = Path(__file__).parent.parent.parent
        bat_file = base_dir / 'scripts' / 'RESTART_SILENT.bat'
        if bat_file.exists():
            subprocess.Popen(['cmd', '/c', str(bat_file)], cwd=str(base_dir),
                           creationflags=subprocess.CREATE_NO_WINDOW)
            return jsonify({'status': 'restarting', 'message': 'Server restarting...'})
        return jsonify({'error': 'RESTART_SILENT.bat not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pause', methods=['POST'])
def pause_scraping():
    """Pause the current scraping session."""
    global scraper_controller
    
    if not scraper_controller or not scraper_controller.is_running:
        return jsonify({'error': 'No active scraping session'}), 400
    
    scraper_controller.pause()
    return jsonify({'status': 'paused'})


@app.route('/api/resume', methods=['POST'])
def resume_scraping():
    """Resume a paused scraping session."""
    global scraper_controller
    
    if not scraper_controller:
        return jsonify({'error': 'No active scraping session'}), 400
    
    scraper_controller.resume()
    return jsonify({'status': 'running'})


@app.route('/api/resume-state', methods=['GET'])
def get_resume_state():
    """Get saved resume state if available."""
    from app.scraper_wrapper import ScraperController
    state = ScraperController.load_state()
    if state:
        return jsonify({
            'has_state': True,
            'state': state
        })
    return jsonify({'has_state': False})


@app.route('/api/clear-state', methods=['POST'])
def clear_resume_state():
    """Clear saved resume state."""
    from app.scraper_wrapper import ScraperController
    ScraperController.clear_state()
    return jsonify({'status': 'cleared'})


@app.route('/api/stop', methods=['POST'])
def stop_scraping():
    """Stop the current scraping session and export data."""
    global scraper_controller
    
    if not scraper_controller:
        return jsonify({'error': 'No active scraping session'}), 400
    
    scraper_controller.stop()
    return jsonify({'status': 'stopped'})


@app.route('/api/status', methods=['GET'])
def get_status():
    """Get current scraper status."""
    global scraper_controller
    
    if not scraper_controller:
        return jsonify({
            'status': 'idle',
            'properties_count': 0,
            'output_file': None
        })
    
    return jsonify({
        'status': scraper_controller.status,
        'properties_count': len(scraper_controller.scraped_properties),
        'output_file': scraper_controller.output_file
    })


@app.route('/api/download', methods=['GET'])
def download_file():
    """Download the generated Excel file."""
    global scraper_controller
    
    if not scraper_controller or not scraper_controller.output_file:
        return jsonify({'error': 'No file available for download'}), 404
    
    file_path = scraper_controller.output_file
    if not os.path.exists(file_path):
        return jsonify({'error': 'File not found'}), 404
    
    return send_file(
        file_path,
        as_attachment=True,
        download_name=os.path.basename(file_path)
    )


@app.route('/api/properties', methods=['GET'])
def get_properties():
    """Get all scraped properties."""
    global scraper_controller
    
    if not scraper_controller:
        return jsonify({'properties': []})
    
    return jsonify({'properties': scraper_controller.scraped_properties})


@app.route('/api/history', methods=['GET'])
def get_history():
    """Get scrape history."""
    return jsonify({'history': scrape_history})


@app.route('/api/history/clear', methods=['POST'])
def clear_history():
    """Clear scrape history."""
    global scrape_history
    scrape_history = []
    save_history()
    return jsonify({'status': 'cleared'})


@app.route('/api/excel-files', methods=['GET'])
def get_excel_files():
    """Get list of Excel files in the output directory and project directories."""
    import glob
    import pandas as pd
    
    files = []
    
    # Search in multiple directories
    search_dirs = [
        DEFAULT_OUTPUT_DIR,  # Primary output directory
        Path(__file__).parent.parent,  # Scraper root directory
        Path(DEFAULT_OUTPUT_DIR).parent if DEFAULT_OUTPUT_DIR else None,  # Parent of output
    ]
    
    for search_dir in search_dirs:
        if not search_dir:
            continue
        search_path = Path(search_dir)
        if search_path.exists():
            for f in search_path.glob('*.xlsx'):
                if f.is_file():
                    # count rows
                    count = 0
                    try:
                        # Read all sheets (sheet_name=None returns a dict of DataFrames)
                        # We only need one column to count, but reading all sheets can be heavy if big.
                        # Still, it's the only way to get total count.
                        dfs = pd.read_excel(f, sheet_name=None, usecols=[0])
                        count = sum(len(df) for df in dfs.values())
                    except Exception as e:
                        print(f"Error counting rows in {f.name}: {e}")
                        pass
                        
                    files.append({
                        'name': f.name,
                        'path': str(f.resolve()),
                        'count': count,
                        'mtime': f.stat().st_mtime
                    })
    
    # Deduplicate by path
    seen_paths = set()
    unique_files = []
    for f in files:
        if f['path'] not in seen_paths:
            seen_paths.add(f['path'])
            unique_files.append(f)
    
    return jsonify({'files': unique_files})


@app.route('/api/excel-worksheets', methods=['GET'])
def get_excel_worksheets():
    """Get list of worksheet names for a given Excel file."""
    import pandas as pd
    
    file_path = request.args.get('file', '').strip()
    
    if not file_path:
        return jsonify({'error': 'File path is required'}), 400
    
    if not os.path.exists(file_path):
        return jsonify({'error': f'File not found: {file_path}'}), 404
    
    try:
        # Read just sheet names without loading data
        xl = pd.ExcelFile(file_path)
        sheets = xl.sheet_names
        return jsonify({'sheets': sheets})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/update-urls', methods=['POST'])
def update_urls():
    """Start URL update process for an existing Excel file."""
    import subprocess
    import json as json_module
    
    data = request.get_json()
    excel_file = data.get('excel_file', '').strip()
    sheets = data.get('sheets', [])  # List of sheet names to process
    resume = data.get('resume', False)
    
    if not excel_file:
        return jsonify({'error': 'Excel file path is required'}), 400
    
    if not os.path.exists(excel_file):
        return jsonify({'error': f'File not found: {excel_file}'}), 404
    
    # Path to the update_urls.py script (local copy in scraper workspace)
    update_script = Path(__file__).parent.parent / 'update_urls.py'
    
    if not update_script.exists():
        return jsonify({'error': 'Update script not found'}), 500
    
    emit_log('INFO', f'Starting URL update for: {os.path.basename(excel_file)}')
    
    # Run the update script in background with file path argument
    def run_update():
        global update_process
        import subprocess
        try:
            # Clean flag before start
            flag_file = update_script.parent / "update_paused.flag"
            if flag_file.exists():
                flag_file.unlink()

            # Run script with the Excel file path and sheets as arguments
            sheets_json = json_module.dumps(sheets) if sheets else '[]'
            
            cmd = [sys.executable, '-u', str(update_script), excel_file, '--sheets', sheets_json]
            if resume:
                cmd.append('--resume')

            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True, # text=True is important for readline
                bufsize=1, # Line buffered
                cwd=str(update_script.parent)
            )
            update_process = process

            
            # Stream output line by line to WebSocket
            for line in iter(process.stdout.readline, ''):
                line = line.strip()
                if not line:
                    continue
                # Parse log level from line
                # Parse log level from line
                if '[STATUS]' in line:
                    status = line.split('[STATUS]')[1].strip().lower() # e.g. "paused"
                    emit_status(status)
                elif '[ERR]' in line or 'ERROR' in line:
                    emit_log('ERR', line.replace('[ERR]', '').replace('ERROR:', '').strip())
                elif '[WARN]' in line:
                    emit_log('WARN', line.replace('[WARN]', '').strip())
                elif '[OK]' in line or 'SUCCESS' in line:
                    emit_log('OK', line.replace('[OK]', '').replace('SUCCESS:', '').strip())
                elif '[INFO]' in line:
                    emit_log('INFO', line.replace('[INFO]', '').strip())
                else:
                    emit_log('INFO', line)
            
            process.wait()
            
            # Check if stopped manually (global var cleared)
            was_stopped = (update_process is None)
            update_process = None
            
            # Clean up flag
            if flag_file.exists():
                flag_file.unlink()
            
            if was_stopped:
                 emit_log('INFO', 'Update stopped by user.')
                 emit_status('stopped', message='Update stopped')
            elif process.returncode == 0:
                emit_log('OK', 'URL update completed successfully!')
                emit_status('completed', message='URL update finished')
            else:
                emit_log('ERR', f'Update failed with code {process.returncode}')
                emit_status('error', message='URL update failed')
                
        except Exception as e:
            emit_log('ERR', f'Error running update: {str(e)}')
            emit_status('error', message=str(e))
    
    thread = threading.Thread(target=run_update, daemon=True)
    thread.start()
    
    return jsonify({'status': 'started', 'file': excel_file})


@app.route('/api/update/check-state', methods=['POST'])
def check_update_state():
    """Check if there is a resumable state for the given file."""
    import json as json_module
    
    data = request.get_json()
    excel_file = data.get('excel_file', '').strip()
    
    if not excel_file:
        return jsonify({'error': 'Excel file path is required'}), 400
        
    update_script = Path(__file__).parent.parent / "update_urls.py"
    journal_file = update_script.parent / "update_progress.jsonl"
    
    if not journal_file.exists():
        return jsonify({'can_resume': False})
        
    try:
        # Check if journal matches this file by reading the first line
        with open(journal_file, 'r', encoding='utf-8') as f:
            first_line = f.readline()
            if not first_line:
                return jsonify({'can_resume': False})
                
            entry = json_module.loads(first_line)
            if entry.get('full_path') == excel_file:
                # Count lines to determine progress
                # Reset file pointer to count all
                f.seek(0)
                count = sum(1 for _ in f)
                
                # We don't know total here easily unless we open Excel, 
                # but we can return the count of processed items.
                # Ideally we should cache the total?
                # For now let's just return the count and client can disable "Resume" if data is weird.
                return jsonify({
                    'can_resume': True,
                    'current_index': count,
                    'total': '?' # Client will show "Reanudar (X finished)"
                })
    except:
        pass
        
    return jsonify({'can_resume': False})



@app.route('/api/update/pause', methods=['POST'])
def pause_update():
    """Pause the update process."""
    update_script = Path(__file__).parent.parent / "update_urls.py"
    flag_file = update_script.parent / "update_paused.flag"
    flag_file.touch()
    return jsonify({'status': 'paused'})

@app.route('/api/update/resume', methods=['POST'])
def resume_update():
    """Resume the update process."""
    update_script = Path(__file__).parent.parent / "update_urls.py"
    flag_file = update_script.parent / "update_paused.flag"
    if flag_file.exists():
        flag_file.unlink()
    return jsonify({'status': 'resumed'})

@app.route('/api/update/stop', methods=['POST'])
def stop_update_process():
    """Stop the update process gracefully to allow saving."""
    global update_process
    update_script = Path(__file__).parent.parent / "update_urls.py"
    
    try:
        if update_process:
            # 1. Create Stop Flag for graceful shutdown
            stop_flag = update_script.parent / "update_stop.flag"
            stop_flag.touch()
            
            # 2. Wait for process to exit (it should see flag and break loop)
            # Give it 5-8 seconds to save Excel
            try:
                update_process.wait(timeout=8)
            except subprocess.TimeoutExpired:
                # 3. Force kill if it's stuck
                try:
                    update_process.terminate()
                    print("Force terminated update process.")
                except: pass
            
            update_process = None
            
        # Clean flags
        flag_file = update_script.parent / "update_paused.flag"
        if flag_file.exists(): flag_file.unlink()
        
        stop_flag = update_script.parent / "update_stop.flag"
        if stop_flag.exists(): stop_flag.unlink()
            
        return jsonify({'status': 'stopped'})
    except Exception as e:
        print(f"Error stopping update: {e}")
        return jsonify({'error': str(e)}), 500



@app.route('/api/import-api', methods=['POST'])
def start_api_import():
    """Start API Import process."""
    from scraper.idealista_scraper.api_client import fetch_data_generator
    from scraper.idealista_scraper.excel_writer import export_split_by_distrito
    import pandas as pd
    
    data = request.get_json()
    location_id = data.get('location_id', '').strip()
    operation = data.get('operation', 'rent')
    max_pages = int(data.get('max_pages', 50))
    location_name = data.get('location_name', location_id) # Allow passing name if manual
    
    if not location_id:
        return jsonify({'error': 'Location ID is required'}), 400
        
    emit_log('INFO', f'Starting API Import for ID: {location_id} ({operation})')
    emit_status('running', mode='api_import')
    
    def run_import():
        try:
            generator = fetch_data_generator(
                location_id=location_id,
                operation=operation,
                max_pages=max_pages,
                on_log=emit_log,
                location_name=location_name
            )
            
            all_rows = []
            
            for event in generator:
                if event['type'] == 'progress':
                    emit_progress({
                        'current_page': event['page'],
                        'total_pages': max_pages,
                        'current_properties': event['total'],
                        'total_properties': 0 
                    })
                elif event['type'] == 'batch':
                    new_rows = event['rows']
                    all_rows.extend(new_rows)
                    if new_rows:
                        emit_property(new_rows[-1])

            if not all_rows:
                emit_log('WARN', 'API Import finished but no properties found.')
                emit_status('completed', message='No data found')
                return

            timestamp = datetime.now().strftime("%Y%m%d_%H%M")
            from scraper.idealista_scraper.utils import sanitize_filename_part
            loc_clean = sanitize_filename_part(location_name)
            
            filename = f"API_IMPORT_{loc_clean}_{operation}_{timestamp}.xlsx"
            out_path = os.path.join(DEFAULT_OUTPUT_DIR, filename)
            
            emit_log('INFO', f"Exporting {len(all_rows)} properties to {filename}...")
            
            export_split_by_distrito(
                existing_df=pd.DataFrame(),
                additions=all_rows,
                out_path=out_path,
                carry_cols=set()
            )
            
            # Global controller output_file might be needed for download button?
            # We don't have a scraper_controller instance for API mode...
            # But the UI checks /api/status.
            # We can mock a controller state or just set the global output file?
            # Or just rely on history entry.
            # But "Download" button uses `scraper_controller.output_file`.
            # I can create a dummy object?
            
            class DummyController:
                def __init__(self, f, p):
                    self.output_file = f
                    self.scraped_properties = p
                    self.status = 'completed'
                    self.current_page = 0
                    self.is_running = False
            
            global scraper_controller
            scraper_controller = DummyController(out_path, all_rows)
            
            add_history_entry(
                seed_url=f"API:{location_id}",
                properties_count=len(all_rows),
                category=f"{loc_clean}_{operation}",
                output_file=out_path
            )
            
            emit_log('OK', f"API Import Successful! Saved: {filename}")
            emit_status('completed', message='Import successful', output_file=out_path)
            
        except Exception as e:
            emit_log('ERR', f"API Import failed: {e}")
            emit_status('error', message=str(e))

    thread = threading.Thread(target=run_import, daemon=True)
    thread.start()

    return jsonify({'status': 'started'})


@socketio.on('progress')
def handle_progress(data):
    """Forward progress events from update_urls.py to UI."""
    socketio.emit('progress_update', data)

@socketio.on('property_scraped')
def handle_property(data):
    """Forward property events from update_urls.py to UI."""
    socketio.emit('property_scraped', data)


def run_server(host='127.0.0.1', port=5003):
    """Run the Flask-SocketIO server."""
    print(f"Starting server at http://{host}:{port}")
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)



# =============================================================================
# API & DATABASE DASHBOARD ENDPOINTS
# =============================================================================

# =============================================================================
# BATCH ENRICHMENT MANAGER
# =============================================================================

class BatchManager:
    def __init__(self):
        self.queue = []      # List of file paths to process
        self.completed = []  # List of successfully processed files
        self.failed = []     # List of failed files
        self.current_idx = -1
        self.is_running = False
        self.thread = None
        self.stop_requested = False
        
    def start_batch(self, files):
        """Start a new batch process with the given files."""
        if self.is_running:
            return False, "Batch already running"
            
        self.queue = files
        self.completed = []
        self.failed = []
        self.current_idx = 0
        self.stop_requested = False
        self.is_running = True
        
        # Start worker thread
        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()
        
        return True, "Batch started"
        
    def stop_batch(self):
        """Stop the current batch."""
        self.stop_requested = True
        self.is_running = False
        # Also kill the current subprocess if exists
        stop_update_process() 
        emit_status('batch_stopped')
        
    def _worker_loop(self):
        """Main loop processing files one by one."""
        # emit_status, emit_log are in global scope
        
        total = len(self.queue)
        emit_log('INFO', f'STARTING BATCH ENRICHMENT: {total} files')
        
        while self.current_idx < total and not self.stop_requested:
            current_file = self.queue[self.current_idx]
            file_name = os.path.basename(current_file)
            
            emit_log('INFO', f'[{self.current_idx + 1}/{total}] Processing: {file_name}')
            emit_status('batch_progress', current=self.current_idx + 1, total=total, file=file_name)
            
            # --- EXECUTE update_urls.py ---
            # We reuse the logic from update_urls endpoint but synchronous here
            success = self._run_update_sync(current_file)
            
            if self.stop_requested: 
                break
                
            if success:
                self.completed.append(current_file)
                emit_log('OK', f'Finished: {file_name}')
            else:
                self.failed.append(current_file)
                emit_log('ERR', f'Failed: {file_name}')
                
            self.current_idx += 1
            
            # Small delay between files
            if self.current_idx < total:
                emit_log('INFO', 'Cooling down 5s before next file...')
                time.sleep(5)
                
        self.is_running = False
        
        if self.stop_requested:
            emit_log('WARN', 'Batch Stopped by User.')
        else:
            emit_log('OK', f'BATCH COMPLETED! {len(self.completed)}/{total} files processed.')
            emit_status('batch_completed', completed=len(self.completed), total=total)

    def _run_update_sync(self, excel_file):
        """Run update_urls.py specifically for this file and wait for it."""
        global update_process
        import subprocess
        import json as json_module
        
        update_script = Path(__file__).parent.parent / 'update_urls.py'
        
        # Ensure we always try to RESUME in batch mode to match user expectation (resume from left off)
        # But for new files it just starts from 0.
        cmd = [sys.executable, '-u', str(update_script), excel_file, '--resume']
        
        # NOTE: We force logic to create STEALTH flag if not present?
        # User requested "Stealth" mode.
        flag_file = update_script.parent / "update_stealth.flag"
        if not flag_file.exists():
            flag_file.touch()
            
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=str(update_script.parent)
            )
            update_process = process # Set global so pause/stop endpoints works
            
            # Stream output
            for line in iter(process.stdout.readline, ''):
                if self.stop_requested:
                    process.terminate()
                    return False
                    
                line = line.strip()
                if not line: continue
                
                # Forward specific logs
                if '[STATUS]' in line:
                    status = line.split('[STATUS]')[1].strip().lower()
                    emit_status(status) # Forward paused/resumed status
                elif '[ERR]' in line or 'ERROR' in line:
                    emit_log('ERR', line.replace('[ERR]', '').strip())
                elif '[WARN]' in line:
                    emit_log('WARN', line.replace('[WARN]', '').strip())
                elif '[OK]' in line or 'SUCCESS' in line:
                    emit_log('OK', line.replace('[OK]', '').strip())
                elif '[INFO]' in line:
                    emit_log('INFO', line.replace('[INFO]', '').strip())
                else:
                    # Capture untagged output (like tracebacks)
                    emit_log('INFO', line)
                
            process.wait()
            update_process = None
            return process.returncode == 0
            
        except Exception as e:
            emit_log('ERR', f"Subprocess exception: {e}")
            return False

# Initialize Global Manager
batch_manager = BatchManager()

@app.route('/api/batch/start', methods=['POST'])
def batch_start():
    data = request.json
    files = data.get('files', []) # List of full paths
    
    if not files:
        return jsonify({'error': 'No files provided'}), 400
        
    success, msg = batch_manager.start_batch(files)
    if success:
        return jsonify({'status': 'started', 'message': msg})
    else:
        return jsonify({'error': msg}), 400

@app.route('/api/batch/stop', methods=['POST'])
def batch_stop():
    batch_manager.stop_batch()
    return jsonify({'status': 'stopped'})

@app.route('/api/batch/status', methods=['GET'])
def batch_status():
    return jsonify({
        'is_running': batch_manager.is_running,
        'current_idx': batch_manager.current_idx,
        'total': len(batch_manager.queue),
        'current_file': batch_manager.queue[batch_manager.current_idx] if batch_manager.is_running and 0 <= batch_manager.current_idx < len(batch_manager.queue) else None
    })


# List of supported provinces (INE Codes)
PROVINCES_LIST = [
    {"id": "0-EU-ES-01", "name": "Alava"}, {"id": "0-EU-ES-02", "name": "Albacete"}, {"id": "0-EU-ES-03", "name": "Alicante"}, 
    {"id": "0-EU-ES-04", "name": "Almeria"}, {"id": "0-EU-ES-05", "name": "Avila"}, {"id": "0-EU-ES-06", "name": "Badajoz"},
    {"id": "0-EU-ES-07", "name": "Baleares"}, {"id": "0-EU-ES-08", "name": "Barcelona"}, {"id": "0-EU-ES-09", "name": "Burgos"},
    {"id": "0-EU-ES-10", "name": "Caceres"}, {"id": "0-EU-ES-11", "name": "Cadiz"}, {"id": "0-EU-ES-12", "name": "Castellon"},
    {"id": "0-EU-ES-13", "name": "Ciudad Real"}, {"id": "0-EU-ES-14", "name": "Cordoba"}, {"id": "0-EU-ES-15", "name": "A Coruna"},
    {"id": "0-EU-ES-16", "name": "Cuenca"}, {"id": "0-EU-ES-17", "name": "Girona"}, {"id": "0-EU-ES-18", "name": "Granada"},
    {"id": "0-EU-ES-19", "name": "Guadalajara"}, {"id": "0-EU-ES-20", "name": "Guipuzcoa"}, {"id": "0-EU-ES-21", "name": "Huelva"},
    {"id": "0-EU-ES-22", "name": "Huesca"}, {"id": "0-EU-ES-23", "name": "Jaen"}, {"id": "0-EU-ES-24", "name": "Leon"},
    {"id": "0-EU-ES-25", "name": "Lleida"}, {"id": "0-EU-ES-26", "name": "La Rioja"}, {"id": "0-EU-ES-27", "name": "Lugo"},
    {"id": "0-EU-ES-28", "name": "Madrid"}, {"id": "0-EU-ES-29", "name": "Malaga"}, {"id": "0-EU-ES-30", "name": "Murcia"},
    {"id": "0-EU-ES-31", "name": "Navarra"}, {"id": "0-EU-ES-32", "name": "Ourense"}, {"id": "0-EU-ES-33", "name": "Asturias"},
    {"id": "0-EU-ES-34", "name": "Palencia"}, {"id": "0-EU-ES-35", "name": "Las Palmas"}, {"id": "0-EU-ES-36", "name": "Pontevedra"},
    {"id": "0-EU-ES-37", "name": "Salamanca"}, {"id": "0-EU-ES-38", "name": "Santa Cruz de Tenerife"},
    {"id": "0-EU-ES-39", "name": "Cantabria"}, {"id": "0-EU-ES-40", "name": "Segovia"}, {"id": "0-EU-ES-41", "name": "Sevilla"},
    {"id": "0-EU-ES-42", "name": "Soria"}, {"id": "0-EU-ES-43", "name": "Tarragona"}, {"id": "0-EU-ES-44", "name": "Teruel"},
    {"id": "0-EU-ES-45", "name": "Toledo"}, {"id": "0-EU-ES-46", "name": "Valencia"}, {"id": "0-EU-ES-47", "name": "Valladolid"},
    {"id": "0-EU-ES-48", "name": "Vizcaya"}, {"id": "0-EU-ES-49", "name": "Zamora"}, {"id": "0-EU-ES-50", "name": "Zaragoza"},
    {"id": "0-EU-ES-51", "name": "Ceuta"}, {"id": "0-EU-ES-52", "name": "Melilla"}
]

@app.route('/api/provinces', methods=['GET'])
def get_provinces():
    return jsonify({'provinces': PROVINCES_LIST})

@app.route('/api/salidas-files', methods=['GET'])
def get_salidas_files():
    """Optimized file listing for scraper/salidas using scandir for maximum performance."""
    try:
        limit = request.args.get('limit', default=200, type=int)
        
        # Resolve scraper/salidas relative to this server file
        current_dir = Path(__file__).parent.parent
        salidas_dir = (current_dir / "salidas").resolve()
        
        if not salidas_dir.exists():
            return jsonify({'files': []})
        
        files = []
        # scandir is significantly faster for directories with many files
        with os.scandir(salidas_dir) as it:
            for entry in it:
                if entry.is_file() and entry.name.endswith('.xlsx') and not entry.name.startswith('~$') and not entry.name.startswith('.'):
                    files.append({
                        'name': entry.name,
                        'path': entry.path,
                        'mtime': entry.stat().st_mtime
                    })
        
        # Sort by modification time (newest first)
        files.sort(key=lambda x: x['mtime'], reverse=True)
        
        # Apply limit
        files = files[:limit]
        
        return jsonify({'files': files})
    except Exception as e:
        print(f"Error in get_salidas_files: {e}")
        return jsonify({'error': str(e), 'files': []}), 500

@app.route('/api/batch-scan', methods=['POST'])
def run_batch_scan():
    """Run batch API scan script."""
    global update_process
    if update_process and update_process.poll() is None:
        return jsonify({'status': 'error', 'message': 'A task is already running. Please wait.'}), 409
        
    data = request.json or {}
    operation = data.get('operation', 'rent') # rent or sale
    provinces = data.get('provinces', []) # List of strings
    
    script_path = (Path(__file__).parent.parent.parent / "scripts" / "batch_api_scan.py").resolve()
    
    cmd = [sys.executable, str(script_path), "--operation", operation, "--resume"]
    
    if provinces:
        # Pass comma-separated list
        # Ensure we don't have empty strings
        clean_provs = [p.strip() for p in provinces if p.strip()]
        if clean_provs:
            cmd.extend(["--provinces", ",".join(clean_provs)])

    return start_background_task(cmd, f"Batch Scan ({operation.upper()})")

@app.route('/api/enrich', methods=['POST'])
def run_enrichment():
    """Run enrichment worker script."""
    global update_process
    if update_process and update_process.poll() is None:
        return jsonify({'status': 'error', 'message': 'A task is already running. Please wait.'}), 409
    
    data = request.json or {}
    operation = data.get('operation', 'rent')
    file_path = data.get('file_path')
    
    script_path = (Path(__file__).parent.parent.parent / "scripts" / "enrich_worker.py").resolve()
    
    if file_path:
        # Use specific file (from picker)
        input_pattern = file_path
    else:
        # Fallback to operation pattern
        input_pattern = f"scraper/salidas/*_{operation}_*.xlsx"
    
    cmd = [sys.executable, str(script_path), "--input", input_pattern, "--max-price", "300000"]
    return start_background_task(cmd, f"Enrichment ({operation.upper()})")

@app.route('/api/db/upload', methods=['POST'])
def run_db_upload():
    """Run upload to Supabase script."""
    global update_process
    if update_process and update_process.poll() is None:
        return jsonify({'status': 'error', 'message': 'A task is already running. Please wait.'}), 409
        
    script_path = (Path(__file__).parent.parent / "import_historical_data.py").resolve()
    
    cmd = [sys.executable, str(script_path)]
    return start_background_task(cmd, "Supabase Upload")

@app.route('/api/db/sync-bq', methods=['POST'])
def run_bq_sync():
    """Run BigQuery sync script."""
    global update_process
    if update_process and update_process.poll() is None:
        return jsonify({'status': 'error', 'message': 'A task is already running. Please wait.'}), 409
        
    script_path = (Path(__file__).parent.parent / "migrate_to_gbq.py").resolve()
    
    cmd = [sys.executable, str(script_path)]
    return start_background_task(cmd, "BigQuery Sync")

@app.route('/api/db/delete', methods=['POST'])
def run_db_delete():
    """Delete all data from Supabase."""
    # This is quick enough to run synchronously in the request, or we can background it.
    # Let's background it to keep UI responsive and consistent logging.
    
    # We'll run a small inline script or just call the function if we can import it.
    # To keep logging consistent with other tasks, let's run a one-liner script.
    
    global update_process
    if update_process and update_process.poll() is None:
        return jsonify({'status': 'error', 'message': 'A task is already running. Please wait.'}), 409

    # Python one-liner to call delete
    cmd = [
        sys.executable, "-c", 
        "import sys; sys.path.insert(0, 'scraper'); from database_manager import DatabaseManager; db=DatabaseManager(); db.delete_all_listings()"
    ]
    # We need to run this from project root so 'scraper' import works if sys.path isn't set right by default
    cwd = str(Path(__file__).parent.parent.parent)
    
    return start_background_task(cmd, "Supabase Delete", cwd=cwd)


def start_background_task(cmd, task_name, cwd=None):
    """Helper to start a background process and stream output to frontend."""
    global update_process
    
    def run_and_stream():
        global update_process
        emit_log("INFO", f"Starting task: {task_name}")
        emit_log("INFO", f"Command: {' '.join(cmd)}")
        
        try:
            # Use project root as default CWD if not specified
            working_dir = cwd if cwd else str(Path(__file__).parent.parent.parent)
            
            update_process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                cwd=working_dir,
                env={**os.environ, "PYTHONUNBUFFERED": "1"} # Force unbuffered output
            )
            
            for line in iter(update_process.stdout.readline, ''):
                if line:
                    emit_log("INFO", line.strip())
            
            update_process.wait()
            rc = update_process.returncode
            
            if rc == 0:
                emit_log("OK", f"Task '{task_name}' completed successfully.")
            else:
                emit_log("ERR", f"Task '{task_name}' failed with exit code {rc}")
                
        except Exception as e:
            emit_log("ERR", f"Failed to start task: {e}")
        finally:
            update_process = None

    thread = threading.Thread(target=run_and_stream)
    thread.daemon = True
    thread.start()
    
    return jsonify({'status': 'started', 'task': task_name})

if __name__ == '__main__':

    run_server()
