import os
import sys
import subprocess
import webbrowser
import threading
import time
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)

# Configuration
SCRAPER_PORT = 5003
ANALYZER_PORT = 5001
DASHBOARD_PORT = 5000

# Heartbeat state
LAST_HEARTBEAT_TIME = time.time() + 15

def kill_by_port(port):
    try:
        cmd = f'powershell -Command "Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | ForEach-Object {{ Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }}"'
        subprocess.run(cmd, shell=True, capture_output=True)
    except:
        pass

def kill_processes():
    kill_by_port(SCRAPER_PORT)
    kill_by_port(ANALYZER_PORT)

def is_port_in_use(port):
    import socket
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('127.0.0.1', port)) == 0

def start_service(service_name):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    env = os.environ.copy()
    env['NO_BROWSER_OPEN'] = '1'
    env['FLASK_USE_RELOADER'] = 'False'  # Prevent double processes (parents not killed by STOP_ALL)
    
    # Minimize child windows
    startupinfo = subprocess.STARTUPINFO()
    startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    startupinfo.wShowWindow = 6 # SW_MINIMIZE
    
    if service_name == 'scraper':
        if is_port_in_use(SCRAPER_PORT):
            return True
        # Run app.server module directly (not start.py which spawns more subprocesses)
        scraper_dir = os.path.join(base_dir, 'scraper')
        cmd = [sys.executable, '-m', 'app.server']
        env['PYTHONPATH'] = scraper_dir
        subprocess.Popen(cmd, cwd=scraper_dir, env=env, 
                        creationflags=subprocess.CREATE_NO_WINDOW)
        return True
        
    elif service_name == 'analyzer':
        if is_port_in_use(ANALYZER_PORT):
            return True
        analyzer_dir = os.path.join(base_dir, 'analyzer')
        script = os.path.join(analyzer_dir, 'app.py')
        subprocess.Popen([sys.executable, script], cwd=analyzer_dir, env=env,
                        creationflags=subprocess.CREATE_NO_WINDOW)
        return True
    
    elif service_name == 'metrics':
        # Check port 5004 specifically for metrics
        if is_port_in_use(5004): 
            return True

        metrics_dir = os.path.join(base_dir, 'dashboard')
        script = os.path.join(metrics_dir, 'app.py')
        
        # Log output to file for debugging
        with open(os.path.join(base_dir, 'metrics_debug.log'), 'w') as log_file:
            subprocess.Popen([sys.executable, script], cwd=metrics_dir, env=env,
                            creationflags=subprocess.CREATE_NO_WINDOW,
                            stdout=log_file, stderr=log_file)
        return True
    
    return False

@app.route('/')
def index():
    return render_template('design_sidebar.html', scraper_port=SCRAPER_PORT, analyzer_port=ANALYZER_PORT)

@app.route('/api/start/<service>', methods=['POST'])
def api_start_service(service):
    if service not in ['scraper', 'analyzer', 'metrics']:
        return jsonify({'error': 'Invalid service'}), 400
    try:
        start_service(service)
        time.sleep(3)
        return jsonify({'status': 'started'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/stop-all', methods=['POST'])
def stop_all():
    kill_processes()
    return jsonify({'status': 'stopped'})

@app.route('/api/heartbeat', methods=['POST'])
def heartbeat():
    global LAST_HEARTBEAT_TIME
    LAST_HEARTBEAT_TIME = time.time()
    return jsonify({'status': 'ok'})

def monitor_activity():
    global LAST_HEARTBEAT_TIME
    print("Heartbeat monitor started...")
    while True:
        if time.time() - LAST_HEARTBEAT_TIME > 70:
            print("Dashboard closed. Shutting down services...")
            kill_processes()
            os._exit(0)
        time.sleep(1)

if __name__ == '__main__':
    url = f"http://127.0.0.1:{DASHBOARD_PORT}"
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    threading.Thread(target=monitor_activity, daemon=True).start()
    
    print(f"Starting Sidebar View Dashboard at {url}")
    app.run(port=DASHBOARD_PORT, debug=False, use_reloader=False)
