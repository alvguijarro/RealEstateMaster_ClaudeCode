
import asyncio
import os
import sys
from playwright.async_api import async_playwright

# Mock get_browser_executable_path for testing
def get_vivaldi_path():
    local_app_data = os.environ.get("LOCALAPPDATA", "")
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")
    
    paths = [
        os.path.join(local_app_data, "Vivaldi", "Application", "vivaldi.exe"),
        os.path.join(program_files, "Vivaldi", "Application", "vivaldi.exe"),
        os.path.join(program_files_x86, "Vivaldi", "Application", "vivaldi.exe"),
    ]
    for p in paths:
        if os.path.exists(p): return p
    return None

async def verify_fix():
    print("Verifying Vivaldi Launch Fix...")
    
    vivaldi_path = get_vivaldi_path()
    if not vivaldi_path:
        print("Vivaldi not found on this system. Cannot verify actual launch.")
        print("However, the logic fix (handling missing executable) should be verified by scraper_wrapper.")
        return

    print(f"Found Vivaldi at: {vivaldi_path}")
    
    async with async_playwright() as pw:
        # TEST 1: Reproduce the bug (launch with channel="vivaldi")
        print("\nTEST 1: Attempting launch with channel='vivaldi' (Expect FAIL)...")
        try:
            browser = await pw.chromium.launch_persistent_context(
                user_data_dir="test_profile_vivaldi_bug",
                channel="vivaldi",
                headless=True
            )
            print("TEST 1 FAILED: Vivaldi launched with channel='vivaldi'? This should have failed!")
            await browser.close()
        except Exception as e:
            if "Unsupported chromium channel" in str(e):
                print(f"TEST 1 PASSED: Reproduced expected error: {e}")
            else:
                print(f"TEST 1: Unexpected error: {e}")

        # TEST 2: Verify the FIX (launch with executable_path and channel=None)
        print("\nTEST 2: Attempting launch with executable_path (Expect SUCCESS)...")
        try:
            browser = await pw.chromium.launch_persistent_context(
                user_data_dir="test_profile_vivaldi_fix",
                executable_path=vivaldi_path,
                channel=None, # CRITICAL: This MUST be None
                headless=True,
                args=["--no-sandbox"] 
            )
            print("TEST 2 PASSED: Vivaldi launched using executable_path!")
            page = browser.pages[0]
            ua = await page.evaluate("navigator.userAgent")
            print(f"   User Agent: {ua}")
            await browser.close()
        except Exception as e:
            print(f"TEST 2 FAILED: Could not launch with executable_path: {e}")

if __name__ == "__main__":
    asyncio.run(verify_fix())
