import os
import sys
import subprocess
import webbrowser
import threading
import time
import requests
from flask import Flask, render_template, jsonify, redirect

app = Flask(__name__)

# Configuration
SCRAPER_PORT = 5003
ANALYZER_PORT = 5001
METRICS_PORT = 5004
DASHBOARD_PORT = 5000

# Process handles
SCRAPER_PROCESS = None
ANALYZER_PROCESS = None
METRICS_PROCESS = None

def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def start_service(service_name):
    global SCRAPER_PROCESS, ANALYZER_PROCESS, METRICS_PROCESS
    
    # Get absolute path to RealEstateMaster directory
    base_dir = os.path.dirname(os.path.abspath(__file__))
    
    if service_name == 'scraper':
        if is_port_in_use(SCRAPER_PORT):
            return True # Already running
        
        # Run app.server module directly (not start.py which spawns more subprocesses)
        scraper_dir = os.path.join(base_dir, 'scraper')
        cmd = [sys.executable, '-m', 'app.server']
        env = os.environ.copy()
        env['PYTHONPATH'] = scraper_dir
        env['NO_BROWSER_OPEN'] = '1'  # Don't open browser from subprocess
        SCRAPER_PROCESS = subprocess.Popen(cmd, cwd=scraper_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
        return True
        
    elif service_name == 'analyzer':
        if is_port_in_use(ANALYZER_PORT):
            return True
        
        # Run app.py from analyzer directory
        analyzer_dir = os.path.join(base_dir, 'analyzer')
        script = os.path.join(analyzer_dir, 'app.py')
        cmd = [sys.executable, script]
        env = os.environ.copy()
        env['NO_BROWSER_OPEN'] = '1'
        ANALYZER_PROCESS = subprocess.Popen(cmd, cwd=analyzer_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
        return True
    
    elif service_name == 'metrics':
        if is_port_in_use(METRICS_PORT):
            return True
        
        # Run app.py from dashboard directory
        metrics_dir = os.path.join(base_dir, 'dashboard')
        script = os.path.join(metrics_dir, 'app.py')
        cmd = [sys.executable, script]
        env = os.environ.copy()
        env['NO_BROWSER_OPEN'] = '1'
        METRICS_PROCESS = subprocess.Popen(cmd, cwd=metrics_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
        return True
        
    return False

@app.after_request
def after_request(response):
    """Add anti-caching headers and prevent this dashboard from being embedded in iframes."""
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    # CRITICAL: Prevent this page from being embedded in iframes to stop recursive nesting
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-Service-Identity'] = 'Real-Estate-Master-Main-5000'
    return response

@app.route('/')
def index():
    return render_template('design_sidebar.html', scraper_port=SCRAPER_PORT, analyzer_port=ANALYZER_PORT, metrics_port=METRICS_PORT)

@app.route('/api/start/<service>', methods=['POST'])
@app.route('/api/start/<service>', methods=['POST'])
def api_start_service(service):
    if service not in ['scraper', 'analyzer', 'metrics', 'merger']:
        return jsonify({'error': 'Invalid service'}), 400
        
    try:
        # 'merger' is hosted in the analyzer service
        target_service = 'analyzer' if service == 'merger' else service
        
        success = start_service(target_service)
        if success:
            # Wait a bit for startup
            time.sleep(2)
            return jsonify({'status': 'started'})
        else:
            return jsonify({'error': 'Failed to start'}), 500
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stop-all', methods=['POST'])
def stop_all():
    global SCRAPER_PROCESS, ANALYZER_PROCESS, METRICS_PROCESS
    
    count = 0
    if SCRAPER_PROCESS:
        try:
            SCRAPER_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {SCRAPER_PROCESS.pid}")
            count += 1
        except: pass
        SCRAPER_PROCESS = None
        
    if ANALYZER_PROCESS:
        try:
            ANALYZER_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {ANALYZER_PROCESS.pid}")
            count += 1
        except: pass
        ANALYZER_PROCESS = None
    
    if METRICS_PROCESS:
        try:
            METRICS_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {METRICS_PROCESS.pid}")
            count += 1
        except: pass
        METRICS_PROCESS = None
    
    return jsonify({'status': 'stopped', 'count': count})

if __name__ == '__main__':
    # Auto-open dashboard
    url = f"http://127.0.0.1:{DASHBOARD_PORT}"
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    print(f"Starting Unified Dashboard at {url}")
    app.run(port=DASHBOARD_PORT, debug=False, use_reloader=False)
