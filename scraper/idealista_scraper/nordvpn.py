import subprocess
import time
import os
import random

# Robust import for log function
try:
    from .utils import log
except ImportError:
    try:
        from idealista_scraper.utils import log
    except ImportError:
        # Fallback: define a simple log function
        def log(level, msg):
            print(f"[{level}] {msg}")

NORDVPN_PATH = r"C:\Program Files\NordVPN\nordvpn.exe"

def run_nordvpn(args):
    """Run NordVPN CLI command and return output."""
    if not os.path.exists(NORDVPN_PATH):
        log("ERR", f"NordVPN not found at {NORDVPN_PATH}")
        return None
    
    cmd = [NORDVPN_PATH] + args
    try:
        # Use shell=True for Windows might be needed if it's a batch wrapper, 
        # but nordvpn.exe is a binary.
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)
        return result.stdout
    except Exception as e:
        log("ERR", f"Error running NordVPN command: {e}")
        return None

def disconnect():
    """Disconnect from NordVPN."""
    log("INFO", "NordVPN: Disconnecting...")
    return run_nordvpn(["-d"])

def connect(group=None):
    """Connect to NordVPN."""
    args = ["-c"]
    if group:
        args += ["-g", group]
    
    log("INFO", f"NordVPN: Connecting{' to ' + group if group else ''}...")
    return run_nordvpn(args)

def rotate_ip(group=None):
    """Disconnect and reconnect to change IP."""
    disconnect()
    time.sleep(2)
    result = connect(group)
    # Wait a bit for the connection to stabilize
    time.sleep(5)
    return result

def get_status():
    """Check if NordVPN is connected (heuristic based on output)."""
    # Note: 'nordvpn' command with no args or --status usually gives status
    # The search results didn't explicitly say how to get status, 
    # but usually it's `nordvpn` or `nordvpn status`.
    output = run_nordvpn([]) # Try empty or status
    if not output:
        return "Unknown"
    
    if "Connected" in output:
        return "Connected"
    elif "Disconnected" in output:
        return "Disconnected"
    return "Unknown"

if __name__ == "__main__":
    # Test
    print("NordVPN status:", get_status())
    # rotate_ip()
