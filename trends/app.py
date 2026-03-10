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
    """Initialize/migrate the SQLite database for Market Trends."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Crear tabla si no existe (instalaciones nuevas)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_record TEXT NOT NULL,
            iso_year INTEGER NOT NULL,
            iso_week INTEGER NOT NULL,
            province TEXT NOT NULL,
            zone TEXT NOT NULL,
            subzone TEXT NOT NULL DEFAULT '',
            operation TEXT NOT NULL,
            total_properties INTEGER NOT NULL,
            UNIQUE(date_record, province, zone, subzone, operation)
        )
    ''')

    # Migración: añadir columna subzone si falta (instalaciones existentes)
    cursor.execute("PRAGMA table_info(inventory_trends)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'subzone' not in cols:
        cursor.execute("ALTER TABLE inventory_trends RENAME TO inventory_trends_old")
        cursor.execute('''
            CREATE TABLE inventory_trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_record TEXT NOT NULL,
                iso_year INTEGER NOT NULL,
                iso_week INTEGER NOT NULL,
                province TEXT NOT NULL,
                zone TEXT NOT NULL,
                subzone TEXT NOT NULL DEFAULT '',
                operation TEXT NOT NULL,
                total_properties INTEGER NOT NULL,
                UNIQUE(date_record, province, zone, subzone, operation)
            )
        ''')
        cursor.execute('''
            INSERT INTO inventory_trends
                SELECT id, date_record, iso_year, iso_week, province, zone, '', operation, total_properties
                FROM inventory_trends_old
        ''')
        cursor.execute("DROP TABLE inventory_trends_old")

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
    """Retrieve historical trend data. Optionally filter by province, zone, subzone, operation."""
    province = request.args.get('province')
    zones = request.args.getlist('zone')
    subzones = request.args.getlist('subzone')
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
        placeholders = ','.join(['?'] * len(zones))
        conditions.append(f"zone IN ({placeholders})")
        params.extend(zones)

    if subzones:
        placeholders = ','.join(['?'] * len(subzones))
        conditions.append(f"subzone IN ({placeholders})")
        params.extend(subzones)

    if operation:
        conditions.append("operation = ?")
        params.append(operation)

    if conditions:
        query += " WHERE " + " AND ".join(conditions)

    query += " ORDER BY id ASC"

    cursor.execute(query, params)
    rows = cursor.fetchall()

    data = [dict(row) for row in rows]
    conn.close()

    return jsonify(data)

@app.route('/api/provinces', methods=['GET'])
def get_provinces():
    """Get provinces/zones/subzones from mapping file + subzones_complete.json.
    Returns a 3-level dict: { province: { zone: [subzone, ...] } }
    where subzone list is [] for leaf zones and ["sz1","sz2",...] for zones with subzones.
    """
    mapping_file = PROJECT_ROOT / "scraper" / "documentation" / "province_urls_mapping.md"
    subzones_file = PROJECT_ROOT / "scraper" / "documentation" / "subzones_complete.json"

    # Load subzones data
    subzones_data = {}
    if subzones_file.exists():
        try:
            with open(subzones_file, 'r', encoding='utf-8') as f:
                subzones_data = json.load(f)
        except Exception:
            pass

    # Parse provinces and zones from mapping file
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
                                provinces_dict[current_province] = {}
                            if zone and zone not in provinces_dict[current_province]:
                                provinces_dict[current_province][zone] = []

    # Enrich with subzones from subzones_complete.json
    for province, zones in provinces_dict.items():
        prov_subzones = subzones_data.get(province, {})
        for zone in zones:
            zone_data = prov_subzones.get(zone, {})
            subzone_list = zone_data.get('subzones', [])
            if subzone_list:
                provinces_dict[province][zone] = [sz['name'] for sz in subzone_list]

    # Sort zones alphabetically within each province
    result = {
        prov: dict(sorted(zones.items()))
        for prov, zones in sorted(provinces_dict.items())
    }
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
        popen_kwargs = dict(cwd=str(BASE_DIR), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False)
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        TRACKER_PROCESS = subprocess.Popen(cmd, **popen_kwargs)

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
        popen_kwargs = dict(cwd=str(BASE_DIR), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=False)
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
        TRACKER_PROCESS = subprocess.Popen(cmd, **popen_kwargs)

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
            if sys.platform == "win32":
                os.system(f"taskkill /F /T /PID {TRACKER_PROCESS.pid}")
            else:
                import signal
                os.kill(TRACKER_PROCESS.pid, signal.SIGKILL)
            
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
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, subzone, operation, total_properties FROM inventory_trends ORDER BY id DESC")
        rows = cursor.fetchall()
        conn.close()

        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(['Fecha', 'Año ISO', 'Semana ISO', 'Provincia', 'Zona', 'Subzona', 'Operación', 'Total Propiedades'])
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
    host = '0.0.0.0' if sys.platform != 'win32' else '127.0.0.1'
    socketio.run(app, debug=False, host=host, port=TRENDS_PORT, allow_unsafe_werkzeug=True)
