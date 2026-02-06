import subprocess
import time
import os
from scraper.idealista_scraper.utils import log

NORDVPN_PATH = r"C:\Program Files\NordVPN\NordVPN.exe"

def run_nordvpn(args):
    """Run NordVPN CLI command and return output."""
    if not os.path.exists(NORDVPN_PATH):
        # Fallback to search in PATH if not in default location
        try:
            result = subprocess.run(["where", "nordvpn"], capture_output=True, text=True, check=False)
            if result.returncode == 0:
                path = result.stdout.strip().split("\n")[0]
            else:
                log("ERR", f"NordVPN not found at {NORDVPN_PATH} and not in PATH")
                return None
        except:
            log("ERR", f"NordVPN not found at {NORDVPN_PATH}")
            return None
    else:
        path = NORDVPN_PATH
    
    cmd = [path] + args
    try:
        # We use a shortcut to avoid hanging if the app opens a GUI or something
        # But usually CLI commands return quickly.
        result = subprocess.run(cmd, capture_output=True, text=True, check=False, timeout=30)
        return result.stdout
    except subprocess.TimeoutExpired:
        log("WARN", f"NordVPN command timed out: {' '.join(args)}")
        return None
    except Exception as e:
        log("ERR", f"Error running NordVPN command: {e}")
        return None

def disconnect():
    """Disconnect from NordVPN."""
    log("INFO", "🌐 NordVPN: Disconnecting...")
    return run_nordvpn(["-d"])

def connect(group=None):
    """Connect to NordVPN."""
    args = ["-c"]
    if group:
        args += ["-g", group]
    
    log("INFO", f"🌐 NordVPN: Connecting{' to ' + group if group else ''}...")
    return run_nordvpn(args)

def rotate_ip(group="Spain"):
    """Disconnect and reconnect to change IP."""
    log("INFO", "🔄 Rotating IP via NordVPN...")
    disconnect()
    time.sleep(3)
    result = connect(group)
    # Wait a bit for the connection to stabilize and network to resume
    log("INFO", "⌛ Waiting 10s for connection to stabilize...")
    time.sleep(10)
    return result

def get_status():
    """Check NordVPN status."""
    # Running without args usually shows status on Windows
    output = run_nordvpn([]) 
    if not output:
        return "Unknown"
    
    if "connected" in output.lower():
        return "Connected"
    elif "disconnected" in output.lower():
        return "Disconnected"
    return output.strip()

if __name__ == "__main__":
    print(f"Status: {get_status()}")
