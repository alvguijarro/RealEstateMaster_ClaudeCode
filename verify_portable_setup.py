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
    browsers_dir = project_root / "browsers"
    
    print(f"[CHECKING] Verifying Portable Browser Setup in: {browsers_dir}")
    print("-" * 50)

    if not browsers_dir.exists():
        print(f"[ERROR] 'browsers' directory does not exist at {browsers_dir}")
        print("   Did you run 'mkdir browsers'?")
        return

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
    
    # Brave
    check_browser("Brave", [
        browsers_dir / "Brave" / "brave.exe",
        browsers_dir / "BravePortable" / "App" / "Brave-64" / "brave.exe"
    ])
    
    # Opera
    check_browser("Opera", [
        browsers_dir / "Opera" / "opera.exe",
        browsers_dir / "OperaPortable" / "App" / "Opera" / "opera.exe"
    ])
    
    # Vivaldi
    check_browser("Vivaldi", [
        browsers_dir / "Vivaldi" / "Application" / "vivaldi.exe",
        browsers_dir / "VivaldiPortable" / "App" / "Vivaldi" / "vivaldi.exe"
    ])

    print("-" * 50)
    print("If you see [MISSING] for Playwright browsers, run:")
    print(f"   cd {project_root}")
    print(f"   set PLAYWRIGHT_BROWSERS_PATH=.\\browsers")
    print(f"   .\\scraper\\python\\python.exe -m playwright install chromium firefox webkit")

if __name__ == "__main__":
    main()
