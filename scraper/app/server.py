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
                        'count': count
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
            
            cmd = ['python', '-u', str(update_script), excel_file, '--sheets', sheets_json]
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
    """Stop the update process."""
    global update_process
    update_script = Path(__file__).parent.parent / "update_urls.py"
    
    try:
        if update_process:
            try:
                update_process.terminate()
            except:
                pass
            update_process = None
            
        # Clean flag
        flag_file = update_script.parent / "update_paused.flag"
        if flag_file.exists():
            flag_file.unlink()
            
        return jsonify({'status': 'stopped'})
    except Exception as e:
        print(f"Error stopping update: {e}")
        return jsonify({'error': str(e)}), 500



@socketio.on('progress')
def handle_progress(data):
    """Forward progress events from update_urls.py to UI."""
    socketio.emit('progress_update', data)


def run_server(host='127.0.0.1', port=5003):
    """Run the Flask-SocketIO server."""
    print(f"Starting server at http://{host}:{port}")
    socketio.run(app, host=host, port=port, debug=False, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    run_server()
