import os
import sys
import subprocess
import webbrowser
import threading
import time
import requests
from flask import Flask, render_template, jsonify, redirect

# Add project root to path for shared imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import from shared config
try:
    from shared.config import SCRAPER_PORT, ANALYZER_PORT, METRICS_PORT, DASHBOARD_PORT, MERGER_PORT, TRENDS_PORT
except ImportError:
    # Fallback for standalone execution
    SCRAPER_PORT = 5003
    ANALYZER_PORT = 5001
    METRICS_PORT = 5004
    DASHBOARD_PORT = 5000
    MERGER_PORT = 5002
    TRENDS_PORT = 5005

app = Flask(__name__)

# Process handles
SCRAPER_PROCESS = None
ANALYZER_PROCESS = None
METRICS_PROCESS = None
MERGER_PROCESS = None
TRENDS_PROCESS = None

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
            print(f"   [OK] Scraper already running on port {SCRAPER_PORT}")
            return True
        
        scraper_dir = os.path.join(base_dir, 'scraper')
        
        # Determine the Python interpreter: favor embedded python in scraper/python/
        embedded_python = os.path.join(scraper_dir, 'python', 'python.exe')
        python_exe = embedded_python if os.path.exists(embedded_python) else sys.executable
        
        print(f"   [START] Launching Scraper using: {python_exe}")
        
        cmd = [python_exe, '-m', 'app.server']
        env = os.environ.copy()
        
        # Propagate Playwright Browsers Path for Portable version
        if 'PLAYWRIGHT_BROWSERS_PATH' not in env:
            # Check if we are in portable mode
            portable_browsers = os.path.join(base_dir, 'python_portable', 'browsers')
            if os.path.exists(portable_browsers):
                env['PLAYWRIGHT_BROWSERS_PATH'] = portable_browsers
                print(f"   [INFO] Setting PLAYWRIGHT_BROWSERS_PATH to: {portable_browsers}")
        
        env['PYTHONPATH'] = scraper_dir
        env['NO_BROWSER_OPEN'] = '1'
        
        try:
            SCRAPER_PROCESS = subprocess.Popen(cmd, cwd=scraper_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
            print(f"   [OK] Scraper process started (PID: {SCRAPER_PROCESS.pid})")
            return True
        except Exception as e:
            print(f"   [ERR] Failed to start scraper: {e}")
            return False
        
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
    
    elif service_name == 'merger':
        if is_port_in_use(MERGER_PORT):
            return True
        
        # Run app.py from merger directory
        merger_dir = os.path.join(base_dir, 'merger')
        script = os.path.join(merger_dir, 'app.py')
        cmd = [sys.executable, script]
        env = os.environ.copy()
        env['NO_BROWSER_OPEN'] = '1'
        global MERGER_PROCESS
        MERGER_PROCESS = subprocess.Popen(cmd, cwd=merger_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
        return True
        
    elif service_name == 'trends':
        if is_port_in_use(TRENDS_PORT):
            return True
        
        # Run app.py from trends directory
        trends_dir = os.path.join(base_dir, 'trends')
        script = os.path.join(trends_dir, 'app.py')
        cmd = [sys.executable, script]
        env = os.environ.copy()
        env['NO_BROWSER_OPEN'] = '1'
        global TRENDS_PROCESS
        TRENDS_PROCESS = subprocess.Popen(cmd, cwd=trends_dir, env=env, creationflags=subprocess.CREATE_NO_WINDOW)
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
    return render_template('design_sidebar.html', scraper_port=SCRAPER_PORT, analyzer_port=ANALYZER_PORT, metrics_port=METRICS_PORT, merger_port=MERGER_PORT, trends_port=TRENDS_PORT)

@app.route('/api/start/<service>', methods=['POST'])
def api_start_service(service):
    if service not in ['scraper', 'analyzer', 'metrics', 'merger', 'calculator', 'trends']:
        return jsonify({'error': 'Invalid service'}), 400
        
    try:
        # 'calculator' is hosted in the analyzer service, 'merger' is now its own service
        if service == 'calculator':
            target_service = 'analyzer'
        else:
            target_service = service
        
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
    
    if MERGER_PROCESS:
        try:
            MERGER_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {MERGER_PROCESS.pid}")
            count += 1
        except: pass
        MERGER_PROCESS = None
        
    if TRENDS_PROCESS:
        try:
            TRENDS_PROCESS.terminate()
            os.system(f"taskkill /F /T /PID {TRENDS_PROCESS.pid}")
            count += 1
        except: pass
        TRENDS_PROCESS = None
    
    return jsonify({'status': 'stopped', 'count': count})

if __name__ == '__main__':
    # Auto-open dashboard
    url = f"http://127.0.0.1:{DASHBOARD_PORT}"
    threading.Timer(1.5, lambda: webbrowser.open(url)).start()
    
    print(f"Starting Unified Dashboard at {url}")
    app.run(port=DASHBOARD_PORT, debug=False, use_reloader=False)
