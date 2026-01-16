import os
import sys
import subprocess
import webbrowser
import threading
import time
import platform
import requests
from flask import Flask, render_template, jsonify, request, Response, stream_with_context
from flask_basicauth import BasicAuth

app = Flask(__name__)

# Configuration
IS_WINDOWS = sys.platform == 'win32'
SCRAPER_PORT = 5003
ANALYZER_PORT = 5001
# On cloud, we must bind to the port provided by the environment variable
DASHBOARD_PORT = int(os.environ.get('PORT', 5004))

# Basic Auth Configuration
app.config['BASIC_AUTH_USERNAME'] = os.environ.get('BASIC_AUTH_USERNAME', 'admin')
app.config['BASIC_AUTH_PASSWORD'] = os.environ.get('BASIC_AUTH_PASSWORD', 'admin')
app.config['BASIC_AUTH_FORCE'] = True  # Protect entire app

basic_auth = BasicAuth(app)

# Heartbeat state
LAST_HEARTBEAT_TIME = time.time() + 15

def kill_by_port(port):
    if IS_WINDOWS:
        try:
            cmd = f'powershell -Command "Get-NetTCPConnection -LocalPort {port} -ErrorAction SilentlyContinue | ForEach-Object {{ Stop-Process -Id $_.OwningProcess -Force -ErrorAction SilentlyContinue }}"'
            subprocess.run(cmd, shell=True, capture_output=True)
        except:
            pass
    else:
        # Linux/Unix way to kill process on port
        try:
            cmd = f"lsof -ti:{port} | xargs kill -9"
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
    env['FLASK_USE_RELOADER'] = 'False'  # Prevent double processes
    
    # Platform-specific startup info
    startupinfo = None
    creationflags = 0
    
    if IS_WINDOWS:
        # Minimize child windows on Windows
        startupinfo = subprocess.STARTUPINFO()
        startupinfo.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        startupinfo.wShowWindow = 6 # SW_MINIMIZE
        creationflags = subprocess.CREATE_NEW_CONSOLE
    
    if service_name == 'scraper':
        if is_port_in_use(SCRAPER_PORT):
            return True
        scraper_dir = os.path.join(base_dir, 'scraper')
        script = os.path.join(scraper_dir, 'start.py')
        subprocess.Popen([sys.executable, script], cwd=scraper_dir, env=env, 
                        creationflags=creationflags,
                        startupinfo=startupinfo)
        return True
        
    elif service_name == 'analyzer':
        if is_port_in_use(ANALYZER_PORT):
            return True
        analyzer_dir = os.path.join(base_dir, 'analyzer')
        script = os.path.join(analyzer_dir, 'app.py')
        subprocess.Popen([sys.executable, script], cwd=analyzer_dir, env=env,
                        creationflags=creationflags,
                        startupinfo=startupinfo)
        return True
    
    return False

# =============================================================================
# REVERSE PROXY LOGIC
# =============================================================================
@app.route('/proxy/<service_name>/', defaults={'path': ''}, methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
@app.route('/proxy/<service_name>/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE', 'PATCH'])
def proxy(service_name, path):
    """
    Forward requests to internal services (scraper/analyzer).
    This allows the frontend to access them via the main exposed port.
    """
    if service_name == 'scraper':
        target_port = SCRAPER_PORT
    elif service_name == 'analyzer':
        target_port = ANALYZER_PORT
    else:
        return jsonify({'error': 'Unknown service'}), 404

    target_url = f'http://127.0.0.1:{target_port}/{path}'
    
    # Forward query parameters
    params = request.args.copy()
    # Forward content
    data = request.get_data()
    # Forward headers (excluding Host to avoid confusion)
    headers = {key: value for (key, value) in request.headers if key != 'Host'}

    try:
        resp = requests.request(
            method=request.method,
            url=target_url,
            headers=headers,
            data=data,
            params=params,
            cookies=request.cookies,
            allow_redirects=False,
            stream=True  # Important for streaming logs
        )

        # Exclude hop-by-hop headers
        excluded_headers = ['content-encoding', 'content-length', 'transfer-encoding', 'connection']
        headers = [(name, value) for (name, value) in resp.raw.headers.items()
                   if name.lower() not in excluded_headers]

        return Response(stream_with_context(resp.iter_content(chunk_size=1024)),
                        status=resp.status_code,
                        headers=headers,
                        content_type=resp.headers['content-type'])
    except Exception as e:
        return jsonify({'error': f'Proxy error: {str(e)}'}), 502


@app.route('/')
def index():
    # Pass proxy base URLs instead of ports
    return render_template('design_sidebar.html', 
                          scraper_proxy='/proxy/scraper', 
                          analyzer_proxy='/proxy/analyzer')

@app.route('/api/start/<service>', methods=['POST'])
def api_start_service(service):
    if service not in ['scraper', 'analyzer']:
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
        # Disable auto-shutdown on cloud/headless environments
        if not IS_WINDOWS or os.environ.get('HEADLESS'):
            time.sleep(60)
            continue
            
        if time.time() - LAST_HEARTBEAT_TIME > 70:
            print("Dashboard closed. Shutting down services...")
            kill_processes()
            os._exit(0)
        time.sleep(1)

if __name__ == '__main__':
    # Only open browser on Windows and if not in headless mode
    if IS_WINDOWS and not os.environ.get('HEADLESS'):
        url = f"http://127.0.0.1:{DASHBOARD_PORT}"
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    print(f"Starting Sidebar View Dashboard on port {DASHBOARD_PORT}")
    
    # On Cloud (Linux), bind to 0.0.0.0 to be accessible externally
    host = '127.0.0.1' if IS_WINDOWS else '0.0.0.0'
    
    threading.Thread(target=monitor_activity, daemon=True).start()
    app.run(host=host, port=DASHBOARD_PORT, debug=False, use_reloader=False)
