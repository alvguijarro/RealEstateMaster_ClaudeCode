import os
import sys
import sqlite3
import threading
import subprocess
import datetime
from flask import Flask, render_template, jsonify, request, send_file
from flask_socketio import SocketIO
import csv
import io
import re
import json
from pathlib import Path

# Setup paths
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "market_trends.db"
CHECKPOINT_FILE = DATA_DIR / "checkpoint.json"
STOP_FLAG_FILE = DATA_DIR / "TRACKER_STOP.flag"

# Ensure data directory exists
os.makedirs(DATA_DIR, exist_ok=True)

# Add project root to sys.path
PROJECT_ROOT = BASE_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    from shared.config import TRENDS_PORT
except ImportError:
    TRENDS_PORT = 5005

app = Flask(__name__)
app.config['SECRET_KEY'] = 'trends-scraper-secret'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Global state for tracker
TRACKER_PROCESS = None

def process_log_monitor(process, socketio, event_name='log_update'):
    """Reads stdout from a process and emits lines via SocketIO."""
    try:
        for line in iter(process.stdout.readline, b''):
            decoded = line.decode('utf-8', errors='replace').strip()
            if decoded:
                socketio.emit(event_name, {'message': decoded})
        process.stdout.close()
    except Exception as e:
        print(f"Monitor error: {e}")

def init_db():
    """Initialize the SQLite database for Market Trends."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_record TEXT NOT NULL,
            iso_year INTEGER NOT NULL,
            iso_week INTEGER NOT NULL,
            province TEXT NOT NULL,
            zone TEXT NOT NULL,
            operation TEXT NOT NULL,
            total_properties INTEGER NOT NULL,
            UNIQUE(date_record, province, zone, operation)
        )
    ''')
    conn.commit()
    conn.close()

# Initialize DB on startup
init_db()

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

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/trends', methods=['GET'])
def get_trends():
    """Retrieve historical trend data. Optionally filter by province, zone, etc."""
    province = request.args.get('province')
    zones = request.args.getlist('zone')
    operation = request.args.get('operation')
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    query = "SELECT * FROM inventory_trends"
    params = []
    conditions = []
    
    if province:
        conditions.append("province = ?")
        params.append(province)
    
    if zones:
        # Create placeholders for 'IN' clause
        placeholders = ','.join(['?'] * len(zones))
        conditions.append(f"zone IN ({placeholders})")
        params.extend(zones)
        
    if operation:
        conditions.append("operation = ?")
        params.append(operation)
        
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
        
    query += " ORDER BY iso_year ASC, iso_week ASC, date_record ASC"
    
    cursor.execute(query, params)
    rows = cursor.fetchall()
    
    data = [dict(row) for row in rows]
    conn.close()
    
    return jsonify(data)

@app.route('/api/provinces', methods=['GET'])
def get_provinces():
    """Get unique provinces and zones from the mapping file."""
    mapping_file = PROJECT_ROOT / "scraper" / "documentation" / "province_urls_mapping.md"
    
    provinces_dict = {}
    current_province = None
    
    if mapping_file.exists():
        with open(mapping_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line.startswith("|") and not line.startswith("| :---"):
                    parts = [p.strip() for p in line.split("|")]
                    if len(parts) >= 4:
                        prov = parts[1].replace("**", "").strip()
                        zone = parts[2].strip()
                        
                        if prov and prov.lower() != "provincia":
                            current_province = prov
                            if current_province not in provinces_dict:
                                provinces_dict[current_province] = set()
                            if zone:
                                provinces_dict[current_province].add(zone)
                                
    # Convert sets to lists
    result = {k: sorted(list(v)) for k, v in provinces_dict.items()}
    return jsonify({"provinces": result})

@app.route('/api/status', methods=['GET'])
def get_status():
    """Return status of the tracker process."""
    global TRACKER_PROCESS
    
    is_running = False
    if TRACKER_PROCESS is not None:
        if TRACKER_PROCESS.poll() is None:
            is_running = True
        else:
            TRACKER_PROCESS = None
            
    return jsonify({
        "status": "running" if is_running else "idle"
    })

@app.route('/api/checkpoint', methods=['GET'])
def get_checkpoint():
    """Return current checkpoint status."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return jsonify({"available": True, "data": data})
        except Exception as e:
            return jsonify({"available": False, "error": str(e)})
    return jsonify({"available": False})

@app.route('/api/start_tracker', methods=['POST'])
def start_tracker():
    """Launch the background scraper to update trends."""
    global TRACKER_PROCESS
    
    if TRACKER_PROCESS is not None and TRACKER_PROCESS.poll() is None:
        return jsonify({"error": "Tracker is already running"}), 400
        
    # Clear stop flag
    if STOP_FLAG_FILE.exists():
        try: STOP_FLAG_FILE.unlink()
        except: pass
        
    script_path = BASE_DIR / "trends_tracker.py"
    # Added -u flag for unbuffered output to stream logs in real-time
    cmd = [sys.executable, "-u", str(script_path)]
    
    try:
        TRACKER_PROCESS = subprocess.Popen(
            cmd, 
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False, # Keep as bytes to properly handle decode issues
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        # Start monitor thread
        monitor_thread = threading.Thread(
            target=process_log_monitor, 
            args=(TRACKER_PROCESS, socketio), 
            daemon=True
        )
        monitor_thread.start()
        
        return jsonify({"status": "started", "message": "Tracker background process launched."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/resume_tracker', methods=['POST'])
def resume_tracker():
    """Launch the background scraper to update trends, resuming from checkpoint."""
    global TRACKER_PROCESS
    
    if TRACKER_PROCESS is not None and TRACKER_PROCESS.poll() is None:
        return jsonify({"error": "Tracker is already running"}), 400
        
    # Clear stop flag
    if STOP_FLAG_FILE.exists():
        try: STOP_FLAG_FILE.unlink()
        except: pass
        
    script_path = BASE_DIR / "trends_tracker.py"
    cmd = [sys.executable, "-u", str(script_path), "--resume"]
    
    try:
        TRACKER_PROCESS = subprocess.Popen(
            cmd, 
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            creationflags=subprocess.CREATE_NO_WINDOW
        )
        
        # Start monitor thread
        monitor_thread = threading.Thread(
            target=process_log_monitor, 
            args=(TRACKER_PROCESS, socketio), 
            daemon=True
        )
        monitor_thread.start()
        
        return jsonify({"status": "started", "message": "Tracker resumed from checkpoint."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/stop_tracker', methods=['POST'])
def stop_tracker():
    """Stop the background scraper."""
    global TRACKER_PROCESS
    
    # Touch stop flag for graceful termination
    with open(STOP_FLAG_FILE, 'w', encoding='utf-8') as f:
        f.write("STOP")
    
    if TRACKER_PROCESS is None or TRACKER_PROCESS.poll() is not None:
        return jsonify({"error": "Tracker is not running"}), 400
        
    try:
        # Give it up to 15 seconds to gracefully shut down the browser 
        # and save the checkpoint.
        try:
            TRACKER_PROCESS.wait(timeout=15)
        except subprocess.TimeoutExpired:
            print("Force killing tracker process due to timeout.")
            TRACKER_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {TRACKER_PROCESS.pid}")
            
        TRACKER_PROCESS = None
        return jsonify({"status": "stopped", "message": "Tracker background process stopped."})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/export_csv')
def export_csv():
    """Export the inventory trends database to a CSV file."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, operation, total_properties FROM inventory_trends ORDER BY id DESC")
        rows = cursor.fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Fecha', 'Año ISO', 'Semana ISO', 'Provincia', 'Zona', 'Operación', 'Total Propiedades'])
        writer.writerows(rows)
        
        output.seek(0)
        return send_file(
            io.BytesIO(output.getvalue().encode('utf-8')),
            mimetype='text/csv',
            as_attachment=True,
            download_name=f'market_trends_{datetime.datetime.now().strftime("%Y%m%d")}.csv'
        )
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    init_db()
    print(f"Starting Trends Service on port {TRENDS_PORT}...")
    socketio.run(app, debug=False, host='127.0.0.1', port=TRENDS_PORT, allow_unsafe_werkzeug=True)
