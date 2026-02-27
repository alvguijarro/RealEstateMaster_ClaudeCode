import asyncio
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from shared.config import TWOCAPTCHA_API_KEY
except ImportError:
    TWOCAPTCHA_API_KEY = None

from twocaptcha import TwoCaptcha
from twocaptcha.async_solver import AsyncTwoCaptcha

# Import real proxy config
try:
    from shared.proxy_config import get_2captcha_proxy_params, get_2captcha_proxy_dict
except ImportError:
    get_2captcha_proxy_params = lambda: {}
    get_2captcha_proxy_dict = lambda: {}

async def diagnose():
    print(f"API Key present: {bool(TWOCAPTCHA_API_KEY)}")
    if not TWOCAPTCHA_API_KEY:
        print("ERROR: No API Key found.")
        return

    solver = TwoCaptcha(TWOCAPTCHA_API_KEY)
    async_solver = AsyncTwoCaptcha(TWOCAPTCHA_API_KEY)

    print("\n--- Testing Balance ---")
    try:
        current_balance = solver.balance()
        print(f"Balance: {current_balance}")
    except Exception as e:
        print(f"Balance check failed: {e}")

    print("\n--- Testing DataDome Solver Params ---")
    proxy_params = get_2captcha_proxy_params()
    proxy_dict = get_2captcha_proxy_dict()
    print(f"Proxy config present: {bool(proxy_params)}")
    
    try:
        print("Calling ASYNC_SOLVER.datadome with dict proxy...")
        
        async def run_datadome():
            return await async_solver.datadome(
                captcha_url="https://geo.captcha-delivery.com/captcha/?initialCid=test",
                pageurl="https://www.idealista.com",
                userAgent="Mozilla/5.0",
                proxy=proxy_dict
            )
        
        try:
            res = await asyncio.wait_for(run_datadome(), timeout=5)
            print(f"DataDome result: {res}")
        except asyncio.TimeoutError:
            print("DataDome task timed out as expected (but no TypeError raised yet).")
        except TypeError as te:
            print(f"!!! CAUGHT TYPEERROR: {te}")
            import traceback
            traceback.print_exc()
        except Exception as e:
            print(f"Caught other exception: {type(e).__name__}: {e}")
    except Exception as e:
        print(f"Outer exception: {e}")

    print("\n--- Testing Coordinates Solver (Dry Run) ---")
    try:
        with open("test_captcha.png", "wb") as f:
            f.write(b"dummy")
        
        try:
            res = solver.coordinates(file="test_captcha.png", textinstructions="test")
            print(f"Coordinates result: {res}")
        except Exception as e:
            print(f"Coordinates failed as expected: {type(e).__name__}: {e}")
        finally:
            if os.path.exists("test_captcha.png"): os.remove("test_captcha.png")
    except Exception as e:
        print(f"Setup failed: {e}")

if __name__ == "__main__":
    asyncio.run(diagnose())
