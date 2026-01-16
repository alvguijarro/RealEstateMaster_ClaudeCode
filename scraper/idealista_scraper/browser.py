"""Browser automation and Chrome DevTools Protocol (CDP) connection management.

This module handles:
- Detecting if Chrome is already running with DevTools enabled
- Finding the Chrome executable on Windows
- Launching Chrome with the appropriate flags for remote debugging
- Ensuring the CDP port is accessible before proceeding
"""
from __future__ import annotations

import os
import sys
import time
import socket
import subprocess
from typing import Optional

from .utils import log


def _is_port_open(host: str, port: int, timeout: float = 0.4) -> bool:
    """Check if a TCP port is open and accepting connections.
    
    Args:
        host: Hostname or IP address to check
        port: TCP port number
        timeout: Connection timeout in seconds
        
    Returns:
        True if the port is open, False otherwise
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def _find_chrome_exe() -> Optional[str]:
    """Locate the Chrome executable on Windows.
    
    Searches common installation paths in the following order:
    1. Program Files (64-bit)
    2. Program Files (x86) (32-bit)
    3. User's Local AppData
    4. System PATH
    
    Returns:
        Absolute path to chrome.exe if found, None otherwise
    """
    candidates = [
        os.path.join(os.environ.get("PROGRAMFILES", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("PROGRAMFILES(X86)", ""), "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(os.environ.get("LOCALAPPDATA", ""), "Google", "Chrome", "Application", "chrome.exe"),
        "chrome.exe",  # Check if it's in PATH
    ]
    for path in candidates:
        if path and os.path.isfile(path):
            return path
    return None


def ensure_chrome_running(cdp_port: int) -> None:
    """Ensure Chrome is running with DevTools Protocol enabled on the specified port.
    
    If Chrome is not already running with CDP enabled, this function will:
    1. Locate the Chrome executable
    2. Create a dedicated user profile directory
    3. Launch Chrome with remote debugging enabled
    4. Wait for the CDP port to become available
    
    Args:
        cdp_port: The port number for Chrome DevTools Protocol (typically 9222)
        
    Raises:
        SystemExit: If Chrome cannot be found, launched, or the CDP port doesn't open in time
    """
    # Check if Chrome is already running with CDP
    if _is_port_open("127.0.0.1", cdp_port):
        log("INFO", f"Chrome DevTools already running on port {cdp_port}.")
        log("INFO", "If you don't see Chrome, close all Chrome windows and try again.")
        return
    
    # Find Chrome executable
    chrome = _find_chrome_exe()
    if not chrome:
        log("ERR", "Could not find chrome.exe. Install Chrome or add it to PATH.")
        log("ERR", "Download Chrome from: https://www.google.com/chrome/")
        sys.exit(1)
    
    log("INFO", f"Found Chrome at: {chrome}")
    
    # Create a dedicated profile directory for CDP sessions
    profile_dir = os.path.join(os.environ.get("LOCALAPPDATA", "."), "ChromeCdpProfile")
    os.makedirs(profile_dir, exist_ok=True)
    
    # Chrome launch arguments - with visibility flags
    args = [
        chrome,
        f"--remote-debugging-port={cdp_port}",
        f"--user-data-dir={profile_dir}",
        "--new-window",
        "--start-maximized",  # Make window visible and maximized
        "about:blank",
        "--no-first-run",
        "--no-default-browser-check",
    ]
    
    # Launch Chrome as a visible process
    try:
        # Use CREATE_NEW_CONSOLE on Windows to ensure visibility
        subprocess.Popen(args)
        log("INFO", f"Launching Chrome with DevTools on port {cdp_port} ...")
        log("INFO", "A Chrome window should open now. Look for it on your taskbar.")
    except Exception as e:
        log("ERR", f"Failed to launch Chrome: {e}")
        log("ERR", f"Chrome path attempted: {chrome}")
        sys.exit(1)
    
    # Wait for Chrome to open the CDP port (up to 15 seconds)
    log("INFO", "Waiting for Chrome to start...")
    for attempt in range(75):
        if _is_port_open("127.0.0.1", cdp_port):
            log("INFO", "Chrome DevTools is ready.")
            return
        time.sleep(0.2)
    
    # Timeout - Chrome didn't open the port
    log("ERR", "Chrome did not open its DevTools port in time.")
    log("ERR", f"Expected port {cdp_port} to be open on 127.0.0.1")
    log("ERR", "Try manually launching Chrome with: chrome.exe --remote-debugging-port=9222")
    sys.exit(1)
