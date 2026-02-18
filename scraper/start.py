#!/usr/bin/env python3
"""
Idealista Scraper - Standalone Launcher

This script bootstraps the application:
1. Downloads embedded Python if not present
2. Installs pip and dependencies on first run
3. Launches the web server
4. Opens the browser automatically

Run with: python start.py
Or double-click on Windows.
"""

import os
import sys
import subprocess
import zipfile
import urllib.request
import webbrowser
import time
import socket
from pathlib import Path

# Configuration
PYTHON_VERSION = "3.12.0"
PYTHON_URL = f"https://www.python.org/ftp/python/{PYTHON_VERSION}/python-{PYTHON_VERSION}-embed-amd64.zip"
APP_DIR = Path(__file__).parent.absolute()
PYTHON_DIR = APP_DIR / "python"
PYTHON_EXE = PYTHON_DIR / "python.exe"
PIP_URL = "https://bootstrap.pypa.io/get-pip.py"
SERVER_HOST = "127.0.0.1"
SERVER_PORT = 5003

# Dependencies
DEPENDENCIES = [
    "flask>=3.0.0",
    "flask-socketio>=5.3.0",
    "playwright>=1.40.0",
    "playwright-stealth>=1.0.0",  # Anti-detection for Stealth mode
    "pandas>=2.0.0",
    "openpyxl>=3.1.0",
    "python-engineio>=4.8.0",
    "python-socketio>=5.10.0",
    "simple-websocket>=1.0.0",
    "2captcha-python>=2.0.0",
]


def print_banner():
    """Print startup banner."""
    print("\n" + "=" * 60)
    print("   [home] Idealista Scraper - Standalone Application")
    print("=" * 60 + "\n")


def download_file(url: str, dest: Path, desc: str = ""):
    """Download a file with progress indicator."""
    print(f"[DOWNLOAD] Downloading {desc or url}...")
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"[OK] Downloaded to {dest}")
        return True
    except Exception as e:
        print(f"[ERROR] Download failed: {e}")
        return False


def setup_embedded_python():
    """Download and setup embedded Python."""
    if PYTHON_EXE.exists():
        print(f"[OK] Embedded Python found at {PYTHON_DIR}")
        return True
    
    print("[SETUP] Setting up embedded Python (first run only)...")
    
    # Create python directory
    PYTHON_DIR.mkdir(parents=True, exist_ok=True)
    
    # Download Python
    zip_path = PYTHON_DIR / "python.zip"
    if not download_file(PYTHON_URL, zip_path, f"Python {PYTHON_VERSION}"):
        return False
    
    # Extract
    print("[EXTRACT] Extracting Python...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(PYTHON_DIR)
        zip_path.unlink()
        print("[OK] Python extracted")
    except Exception as e:
        print(f"[ERROR] Extraction failed: {e}")
        return False
    
    # Enable site-packages by modifying python312._pth
    pth_file = PYTHON_DIR / "python312._pth"
    if pth_file.exists():
        print("[CONFIG] Configuring Python paths...")
        with open(pth_file, 'w') as f:
            f.write("python312.zip\n")
            f.write(".\n")
            f.write("..\n")  # Parent directory for app module
            f.write("Lib\\site-packages\n")
            f.write("import site\n")  # Enable site-packages
    
    # Create Lib/site-packages directory
    site_packages = PYTHON_DIR / "Lib" / "site-packages"
    site_packages.mkdir(parents=True, exist_ok=True)
    
    return True


def setup_pip():
    """Install pip in embedded Python."""
    pip_exe = PYTHON_DIR / "Scripts" / "pip.exe"
    if pip_exe.exists():
        print("[OK] pip already installed")
        return True
    
    print("[SETUP] Installing pip...")
    
    # Download get-pip.py
    get_pip = PYTHON_DIR / "get-pip.py"
    if not download_file(PIP_URL, get_pip, "pip installer"):
        return False
    
    # Run get-pip.py
    try:
        result = subprocess.run(
            [str(PYTHON_EXE), str(get_pip)],
            cwd=str(APP_DIR),
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"[ERROR] pip installation failed: {result.stderr}")
            return False
        print("[OK] pip installed")
        get_pip.unlink()
        return True
    except Exception as e:
        print(f"[ERROR] pip installation failed: {e}")
        return False


def install_dependencies():
    """Install Python dependencies."""
    pip_exe = PYTHON_DIR / "Scripts" / "pip.exe"
    
    # Check if dependencies are already installed
    marker_file = PYTHON_DIR / ".deps_installed"
    if marker_file.exists():
        print("[OK] Dependencies already installed")
        return True
    
    print("[INSTALL] Installing dependencies (this may take a few minutes)...")
    
    try:
        for dep in DEPENDENCIES:
            print(f"   Installing {dep.split('>=')[0]}...")
            result = subprocess.run(
                [str(pip_exe), "install", dep, "--quiet"],
                cwd=str(APP_DIR),
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                print(f"[ERROR] Failed to install {dep}: {result.stderr}")
                return False
        
        # Install Playwright browsers
        print("   Installing Playwright browsers...")
        result = subprocess.run(
            [str(PYTHON_EXE), "-m", "playwright", "install", "chromium"],
            cwd=str(APP_DIR),
            capture_output=True,
            text=True
        )
        if result.returncode != 0:
            print(f"[WARN] Playwright browser install warning: {result.stderr}")
        
        # Create marker file
        marker_file.touch()
        print("[OK] All dependencies installed")
        return True
        
    except Exception as e:
        print(f"[ERROR] Dependency installation failed: {e}")
        return False


def is_port_in_use(port: int) -> bool:
    """Check if a port is already in use."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex((SERVER_HOST, port)) == 0


def run_server():
    """Run the Flask server."""
    print(f"\n[START] Starting server at http://{SERVER_HOST}:{SERVER_PORT}")
    print("   Press Ctrl+C to stop\n")
    
    # Set environment variables
    env = os.environ.copy()
    env["PYTHONPATH"] = str(APP_DIR)
    
    # Open browser after a short delay
    def open_browser():
        time.sleep(2)
        url = f"http://{SERVER_HOST}:{SERVER_PORT}"
        print(f"[BROWSER] Opening browser: {url}")
        webbrowser.open(url)
    
    import threading
    if not os.environ.get('NO_BROWSER_OPEN'):
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()
    
    # Run server
    # Run server in background (invisible)
    try:
        # DETACHED_PROCESS = 0x00000008
        # CREATE_NO_WINDOW = 0x08000000
        creationflags = 0x08000000 if sys.platform == 'win32' else 0
        
        subprocess.Popen(
            [str(PYTHON_EXE), "-m", "app.server"],
            cwd=str(APP_DIR),
            env=env,
            creationflags=creationflags
        )
        print("[OK] Server started in background.")
        # Keep window open briefly to show success
        time.sleep(2)
    except Exception as e:
        print(f"[ERROR] Failed to start server: {e}")
        time.sleep(5)


def run_with_system_python():
    """Run with system Python if embedded setup fails."""
    print("\n[WARN] Embedded Python setup failed. Trying system Python...")
    
    # Check if dependencies are installed
    try:
        import flask
        import flask_socketio
        import playwright
        import pandas
        print("[OK] Dependencies found in system Python")
    except ImportError as e:
        print(f"[ERROR] Missing dependency: {e}")
        print("\nPlease install dependencies manually:")
        print("   pip install flask flask-socketio playwright pandas openpyxl")
        print("   python -m playwright install chromium")
        sys.exit(1)
    
    # Run server directly
    print(f"\n[START] Starting server at http://{SERVER_HOST}:{SERVER_PORT}")
    print("   Press Ctrl+C to stop\n")
    
    # Open browser
    def open_browser():
        time.sleep(2)
        url = f"http://{SERVER_HOST}:{SERVER_PORT}"
        print(f"[BROWSER] Opening browser: {url}")
        webbrowser.open(url)
    
    import threading
    if not os.environ.get('NO_BROWSER_OPEN'):
        browser_thread = threading.Thread(target=open_browser, daemon=True)
        browser_thread.start()
    
    # Import and run server
    from app.server import run_server as flask_run
    flask_run(host=SERVER_HOST, port=SERVER_PORT)


def main():
    """Main entry point."""
    print_banner()
    
    # Check if port is already in use
    if is_port_in_use(SERVER_PORT):
        print(f"[WARN] Port {SERVER_PORT} is already in use!")
        print(f"   Open http://{SERVER_HOST}:{SERVER_PORT} in your browser")
        print("   Or close the existing server and try again.")
        sys.exit(1)
    
    # Try embedded Python first
    try:
        if setup_embedded_python() and setup_pip() and install_dependencies():
            run_server()
        else:
            run_with_system_python()
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        print("\nTrying fallback with system Python...")
        run_with_system_python()


if __name__ == "__main__":
    main()
