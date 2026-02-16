import os
import sys
from pathlib import Path

def check_browser(name, possible_paths):
    found = False
    for p in possible_paths:
        if os.path.exists(p):
            print(f"✅ FOUND {name}: {p}")
            found = True
            break
    if not found:
        print(f"[MISSING] {name}")
    return found

def main():
    project_root = Path(__file__).parent
    
    # Check multiple locations
    # Priority 1: python_portable/browsers
    # Priority 2: root/browsers 
    possible_browsers_dirs = [
        project_root / "python_portable" / "browsers",
        project_root / "browsers"
    ]
    
    browsers_dir = None
    for d in possible_browsers_dirs:
        # Pick the FIRST one that exists AND has files/folders inside
        if d.exists() and any(d.iterdir()):
            browsers_dir = d
            break

    if not browsers_dir:
        # If neither has content, show error
        print(f"[ERROR] No populated 'browsers' directory found.")
        print(f"Checked locations:")
        for d in possible_browsers_dirs: print(f"  - {d}")
        return
    
    print(f"[CHECKING] Verifying Portable Browser Setup in: {browsers_dir}")

    # Check Playwright Browsers (folders usually start with chromium-, firefox-, webkit-)
    print("\n--- Playwright Managed Browsers ---")
    pw_browsers = ["chromium", "firefox", "webkit"]
    for b in pw_browsers:
        # Playwright creates folders like 'chromium-1124', so we look for partial matches
        found = False
        try:
            for entry in browsers_dir.iterdir():
                if entry.is_dir() and entry.name.startswith(b + "-"):
                    print(f"[OK] FOUND {b}: {entry.name}")
                    found = True
                    break
        except FileNotFoundError:
            pass # catch if dir deleted mid-run
            
        if not found:
            print(f"[MISSING] {b} (Run 'playwright install' inside scraper/python)")

    # Check Custom Portable Browsers
    print("\n--- Custom Portable Browsers ---")
    
    # Google Chrome Portable
    check_browser("Google Chrome Portable", [
        browsers_dir / "GoogleChromePortable" / "GoogleChromePortable.exe",
        browsers_dir / "GoogleChromePortable" / "App" / "Chrome-bin" / "chrome.exe"
    ])

    # Opera
    check_browser("Opera Portable", [
        browsers_dir / "OperaPortable" / "OperaPortable.exe",
        browsers_dir / "OperaPortable" / "App" / "Opera" / "opera.exe",
        browsers_dir / "Opera" / "opera.exe"
    ])
    
    # LibreWolf Portable
    check_browser("LibreWolf Portable", [
        browsers_dir / "LibreWolfPortable" / "LibreWolfPortable.exe",
        browsers_dir / "LibreWolfPortable" / "App" / "LibreWolf" / "librewolf.exe"
    ])

    # Falkon Portable
    check_browser("Falkon Portable", [
        browsers_dir / "FalkonPortable" / "FalkonPortable.exe",
        browsers_dir / "FalkonPortable" / "App" / "Falkon" / "falkon.exe"
    ])

    # SRWare Iron
    check_browser("Iron Portable", [
        browsers_dir / "IronPortable" / "IronPortable.exe",
        browsers_dir / "IronPortable" / "App" / "Iron" / "iron.exe"
    ])

    print("-" * 50)
    print("If you see [MISSING] for Playwright browsers, run:")
    print(f"   cd {project_root}")
    print(f"   set PLAYWRIGHT_BROWSERS_PATH=.\\browsers")
    print(f"   .\\scraper\\python\\python.exe -m playwright install chromium firefox webkit")

if __name__ == "__main__":
    main()
