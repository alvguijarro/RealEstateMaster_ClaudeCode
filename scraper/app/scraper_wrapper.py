"""Wrapper around the Idealista scraper with pause/stop and event callbacks.

This module adapts the existing v6 scraper to support:
- Pause/Resume functionality
- Stop with data export
- Real-time event callbacks for UI updates
- Mode switching (Stealth/Fast)
"""
from __future__ import annotations

import asyncio
import json
import os
import random
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

from playwright.async_api import async_playwright

# Optional playwright-stealth for enhanced anti-detection (Stealth mode only)
try:
    from playwright_stealth import stealth_async
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    stealth_async = None

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from idealista_scraper.config import (
    HARVEST_DEBOUNCE_SECONDS, PAGE_WAIT_MS, RETRY_MAX_ATTEMPTS, RETRY_BASE_DELAY,
    GOTO_WAIT_UNTIL, SCROLL_STEPS, LISTING_LINKS_PER_PAGE_MAX,
    EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    FAST_SCROLL_PAUSE_RANGE, FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SESSION_LIMIT, EXTRA_STEALTH_REST_DURATION_RANGE,
    EXTRA_STEALTH_COFFEE_BREAK_RANGE, EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY,
    EXTRA_STEALTH_READING_TIME_PER_100_CHARS, USER_AGENTS, VIEWPORT_SIZES
)
from idealista_scraper.utils import same_domain, canonical_listing_url, is_listing_url, sanitize_filename_part, play_captcha_alert, play_blocked_alert, simulate_human_interaction, solve_slider_captcha
from idealista_scraper.extractors import extract_detail_fields, missing_fields
from idealista_scraper.excel_writer import (
    load_existing_single_sheet, load_existing_specific_sheet, export_single_sheet,
    load_urls_with_dates, export_split_by_distrito
)
try:
    from app.province_mapping import (
        get_output_file_for_url, load_enriched_urls, load_all_urls_from_excel,
        mark_as_enriched, detect_province_and_operation, DEFAULT_OUTPUT_DIR as PROVINCE_OUTPUT_DIR
    )
except ImportError:
    # Fallback for when running directly from app directory or different context
    try:
        from province_mapping import (
            get_output_file_for_url, load_enriched_urls, load_all_urls_from_excel,
            mark_as_enriched, detect_province_and_operation, DEFAULT_OUTPUT_DIR as PROVINCE_OUTPUT_DIR
        )
    except ImportError:
        print("WARNING: Could not import province_mapping module. Smart enrichment will be disabled.")
        # Define dummy functions/constants to prevent crash
        PROVINCE_OUTPUT_DIR = "salidas"
        def get_output_file_for_url(*args): return None, None, None
        def load_enriched_urls(*args): return set()
        def load_all_urls_from_excel(*args): return {}
        def mark_as_enriched(row): return row
        def detect_province_and_operation(*args): return None, None


def build_paginated_url(seed_url: str, page_number: int) -> str:
    """Build paginated URL from seed URL."""
    parts = urlsplit(seed_url)
    if page_number <= 1:
        return seed_url
    path = parts.path
    is_areas = "/areas/" in path
    if is_areas:
        base_path = re.sub(r"/pagina-\d+/?$", "", path)
        if not base_path.endswith("/"):
            base_path += "/"
        new_path = f"{base_path}pagina-{page_number}"
        return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))
    else:
        base_path = re.sub(r"/pagina-\d+\.htm$", "", path)
        if not base_path.endswith("/"):
            base_path += "/"
        new_path = f"{base_path}pagina-{page_number}.htm"
        return urlunsplit((parts.scheme, parts.netloc, new_path, "", parts.fragment))


def extract_page_from_url(url: str) -> int:
    """Extract page number from URL like /pagina-4 or /pagina-16.
    
    Returns 1 if no page number found in URL.
    """
    match = re.search(r'/pagina-(\d+)', url)
    return int(match.group(1)) if match else 1


# Default output directory - now uses 'salidas' subfolder
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent / "salidas")

# Resume state file path
RESUME_STATE_FILE = str(Path(__file__).parent / "resume_state.json")

# Scrape history registry file path
SCRAPE_HISTORY_FILE = str(Path(DEFAULT_OUTPUT_DIR) / "scrape_history.json")

# =============================================================================
# MULTI-BROWSER SUPPORT WITH PROFILE COOLDOWN
# =============================================================================

# Browser engine options
BROWSER_ENGINES = ["chromium", "firefox"]

# Profile directories per engine
PROFILE_DIRS = {
    "chromium": str(Path(__file__).parent.parent / "stealth_profile_chromium"),
    "firefox": str(Path(__file__).parent.parent / "stealth_profile_firefox"),
}

# Legacy alias for backward compatibility
STEALTH_PROFILE_DIR = PROFILE_DIRS["chromium"]

# Cooldown tracking file
PROFILE_COOLDOWN_FILE = str(Path(__file__).parent / "profile_cooldowns.json")

# Cooldown duration in minutes
PROFILE_COOLDOWN_MINUTES = 15


def load_profile_cooldowns() -> dict:
    """Load profile cooldown timestamps from file."""
    if not os.path.exists(PROFILE_COOLDOWN_FILE):
        return {}
    try:
        with open(PROFILE_COOLDOWN_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_profile_cooldowns(cooldowns: dict) -> None:
    """Save profile cooldown timestamps to file."""
    try:
        with open(PROFILE_COOLDOWN_FILE, "w", encoding="utf-8") as f:
            json.dump(cooldowns, f, indent=2)
    except IOError:
        pass


def mark_profile_blocked(engine: str) -> None:
    """Mark a browser profile as blocked, starting its cooldown timer."""
    import time
    cooldowns = load_profile_cooldowns()
    cooldowns[engine] = time.time()
    save_profile_cooldowns(cooldowns)


def is_profile_available(engine: str) -> bool:
    """Check if a browser profile is available (not in cooldown)."""
    import time
    cooldowns = load_profile_cooldowns()
    
    if engine not in cooldowns:
        return True
    
    blocked_at = cooldowns[engine]
    elapsed_minutes = (time.time() - blocked_at) / 60
    
    if elapsed_minutes >= PROFILE_COOLDOWN_MINUTES:
        # Cooldown expired, clear it
        del cooldowns[engine]
        save_profile_cooldowns(cooldowns)
        return True
    
    return False


def get_cooldown_remaining(engine: str) -> int:
    """Get remaining cooldown time in minutes for a profile."""
    import time
    cooldowns = load_profile_cooldowns()
    
    if engine not in cooldowns:
        return 0
    
    blocked_at = cooldowns[engine]
    elapsed_minutes = (time.time() - blocked_at) / 60
    remaining = PROFILE_COOLDOWN_MINUTES - elapsed_minutes
    
    return max(0, int(remaining))


def get_available_engines() -> List[str]:
    """Get list of browser engines not currently in cooldown."""
    return [engine for engine in BROWSER_ENGINES if is_profile_available(engine)]


def select_next_engine(last_engine: Optional[str] = None) -> Optional[str]:
    """
    Select the next available browser engine using sequential rotation.
    
    If last_engine is provided, tries to pick a different one.
    Returns None if all engines are in cooldown.
    """
    available = get_available_engines()
    
    if not available:
        return None
    
    if last_engine is None or last_engine not in BROWSER_ENGINES:
        # First run: pick first available
        return available[0]
    
    # Try to pick a different engine
    current_idx = BROWSER_ENGINES.index(last_engine)
    for i in range(1, len(BROWSER_ENGINES) + 1):
        next_idx = (current_idx + i) % len(BROWSER_ENGINES)
        candidate = BROWSER_ENGINES[next_idx]
        if candidate in available:
            return candidate
    
    # All in cooldown
    return None


def clear_all_cooldowns() -> None:
    """Clear all profile cooldowns (for manual reset)."""
    save_profile_cooldowns({})


# Engine tracking file (to know which engine was used last)
LAST_ENGINE_FILE = str(Path(__file__).parent / "last_engine.txt")


def get_last_engine() -> Optional[str]:
    """Get the last used browser engine."""
    try:
        if os.path.exists(LAST_ENGINE_FILE):
            with open(LAST_ENGINE_FILE, "r") as f:
                engine = f.read().strip()
                return engine if engine in BROWSER_ENGINES else None
    except IOError:
        pass
    return None


def set_last_engine(engine: str) -> None:
    """Record the last used browser engine."""
    try:
        with open(LAST_ENGINE_FILE, "w") as f:
            f.write(engine)
    except IOError:
        pass


# =============================================================================
# ADVANCED ANTI-BOT EVASION (Phase 1 & 2)
# =============================================================================

# GPU fingerprints pool for randomization (common real GPUs)
GPU_FINGERPRINTS = [
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 3060/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce GTX 1660 Ti/PCIe/SSE2"),
    ("NVIDIA Corporation", "NVIDIA GeForce RTX 2070 SUPER/PCIe/SSE2"),
    ("AMD", "AMD Radeon RX 6700 XT"),
    ("AMD", "AMD Radeon RX 580 Series"),
    ("Intel", "Intel(R) UHD Graphics 630"),
    ("Intel", "Intel(R) Iris(R) Xe Graphics"),
    ("NVIDIA Corporation", "NVIDIA GeForce GTX 1080 Ti/PCIe/SSE2"),
    ("AMD", "AMD Radeon RX 5700 XT"),
]

def get_random_gpu():
    """Select a random GPU fingerprint for this session."""
    import random
    return random.choice(GPU_FINGERPRINTS)

# Generate GPU values at module load (per session)
_GPU_VENDOR, _GPU_RENDERER = get_random_gpu()

# Deep fingerprint spoofing script - injected before any page load
# Uses f-string to inject randomized GPU values
def generate_stealth_script():
    """Generate stealth script with randomized GPU fingerprint."""
    return f'''
// ==================== PHASE 1: DEEP FINGERPRINT SPOOFING ====================

// 1. Remove Chrome DevTools Protocol (CDP) signatures
try {{
    // Delete chrome.runtime which is a CDP indicator
    if (window.chrome && window.chrome.runtime) {{
        delete window.chrome.runtime;
    }}
    
    // Hide cdc_ variables (ChromeDriver signature)
    const originalCall = Function.prototype.call;
    Function.prototype.call = function(...args) {{
        if (args[0] && typeof args[0] === 'object') {{
            const str = String(args[0]);
            if (str.includes('cdc_') || str.includes('$cdc_')) {{
                return undefined;
            }}
        }}
        return originalCall.apply(this, args);
    }};
}} catch (e) {{}}

// 2. Spoof WebGL to match a real GPU (randomized per session)
try {{
    const getParameterProto = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        // UNMASKED_VENDOR_WEBGL
        if (param === 37445) return '{_GPU_VENDOR}';
        // UNMASKED_RENDERER_WEBGL  
        if (param === 37446) return '{_GPU_RENDERER}';
        return getParameterProto.call(this, param);
    }};
    
    // Also patch WebGL2
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParameter2Proto = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return '{_GPU_VENDOR}';
            if (param === 37446) return '{_GPU_RENDERER}';
            return getParameter2Proto.call(this, param);
        }};
    }}
}} catch (e) {{}}

// 3. Add realistic navigator.plugins (automated browsers often have empty plugins)
try {{
    Object.defineProperty(navigator, 'plugins', {{
        get: () => {{
            const plugins = {{
                0: {{type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format', name: 'Chrome PDF Plugin'}},
                1: {{type: 'application/pdf', suffixes: 'pdf', description: '', name: 'Chrome PDF Viewer'}},
                2: {{type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable', name: 'Native Client'}},
                length: 3,
                item: (i) => plugins[i],
                namedItem: (name) => Object.values(plugins).find(p => p.name === name),
                refresh: () => {{}}
            }};
            return plugins;
        }}
    }});
}} catch (e) {{}}

// 4. Fix navigator.languages (should be array, not frozen)
try {{
    Object.defineProperty(navigator, 'languages', {{
        get: () => ['es-ES', 'es', 'en-US', 'en']
    }});
}} catch (e) {{}}

// 5. Patch Permissions API (automation often lacks notifications permission)
try {{
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (params) => {{
        if (params.name === 'notifications') {{
            return Promise.resolve({{state: 'denied', onchange: null}});
        }}
        return originalQuery.call(window.navigator.permissions, params);
    }};
}} catch (e) {{}}

// 6. Add slight randomization to timing functions (defeats timing analysis)
try {{
    const originalNow = Date.now;
    const randomOffset = Math.floor(Math.random() * 50);
    Date.now = function() {{
        return originalNow() + randomOffset;
    }};
    
    const originalPerfNow = performance.now;
    performance.now = function() {{
        return originalPerfNow.call(performance) + (Math.random() * 0.1);
    }};
}} catch (e) {{}}

// 7. Override connection info (automation often has different values)
try {{
    Object.defineProperty(navigator, 'connection', {{
        get: () => ({{
            effectiveType: '4g',
            rtt: 50,
            downlink: 10,
            saveData: false
        }})
    }});
}} catch (e) {{}}

// 8. Hide automation indicators in window object
try {{
    // Remove common automation flags
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    
    // Ensure navigator.webdriver is undefined
    Object.defineProperty(navigator, 'webdriver', {{
        get: () => undefined
    }});
}} catch (e) {{}}

// 9. Realistic screen properties 
try {{
    Object.defineProperty(screen, 'availWidth', {{ get: () => window.innerWidth }});
    Object.defineProperty(screen, 'availHeight', {{ get: () => window.innerHeight + 40 }});
}} catch (e) {{}}

// 10. Override deviceMemory (headless often has unusual values)
try {{
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => 8 }});
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => 8 }});
}} catch (e) {{}}

console.log('[STEALTH] Deep fingerprint spoofing active - GPU: {_GPU_RENDERER}');
'''

# For backward compatibility, generate the script at module load
DEEP_STEALTH_SCRIPT = generate_stealth_script()


async def human_warmup_routine(page, log_func=None):
    """
    Simulate human-like browser warm-up before scraping.
    
    This routine:
    1. Starts from Google (natural referrer)
    2. Performs random mouse movements
    3. Navigates to Idealista via search-like behavior
    4. Scrolls and interacts with homepage
    """
    import random
    import asyncio
    
    def log(level, msg):
        if log_func:
            log_func(level, msg)
    
    log("STEALTH", "Starting human warm-up routine...")
    
    try:
        # Step 1: Visit Google first (establishes natural referrer chain)
        log("STEALTH", "Visiting search engine...")
        await page.goto('https://www.google.es', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(2, 4))
        
        # Step 2: Random mouse movements (humans always move mouse)
        log("STEALTH", "Simulating natural mouse movement...")
        for _ in range(random.randint(4, 8)):
            x = random.randint(100, 1000)
            y = random.randint(100, 600)
            await page.mouse.move(x, y, steps=random.randint(10, 25))
            await asyncio.sleep(random.uniform(0.1, 0.4))
        
        # Step 3: Click on search box and type (simulates real user)
        try:
            search_box = page.locator('textarea[name="q"], input[name="q"]').first
            await search_box.click()
            await asyncio.sleep(random.uniform(0.3, 0.8))
            
            # Type with human-like delays
            search_query = "idealista pisos"
            for char in search_query:
                await page.keyboard.type(char, delay=random.randint(50, 150))
                if random.random() < 0.1:  # 10% chance of small pause
                    await asyncio.sleep(random.uniform(0.1, 0.3))
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
            
            # Press Enter or click search (don't actually search, just simulate typing)
            await page.keyboard.press('Escape')  # Cancel search, we'll go direct
        except Exception:
            pass  # Search interaction is optional
        
        # Step 4: Navigate to Idealista (with Google as referrer)
        log("STEALTH", "Navigating to Idealista with trusted referrer...")
        await asyncio.sleep(random.uniform(1, 2))
        await page.goto('https://www.idealista.com', wait_until='domcontentloaded', timeout=30000)
        await asyncio.sleep(random.uniform(3, 5))
        
        # Step 5: Accept cookies if present
        try:
            await page.evaluate("""() => {
                const btn = document.querySelector('#didomi-notice-agree-button, [id*="accept"], .onetrust-accept-btn');
                if (btn && btn.offsetParent !== null) btn.click();
            }""")
            await asyncio.sleep(random.uniform(0.5, 1))
        except Exception:
            pass
        
        # Step 6: Scroll homepage naturally
        log("STEALTH", "Browsing homepage naturally...")
        for _ in range(random.randint(2, 4)):
            scroll_amount = random.randint(150, 400)
            await page.mouse.wheel(0, scroll_amount)
            await asyncio.sleep(random.uniform(0.8, 2.0))
            
            # Random mouse movement while "reading"
            x = random.randint(200, 900)
            y = random.randint(200, 500)
            await page.mouse.move(x, y, steps=random.randint(5, 15))
        
        # Step 7: Random hover on elements (simulates interest)
        try:
            links = await page.locator('a[href*="/venta-viviendas/"], a[href*="/alquiler-viviendas/"]').all()
            if links:
                random_link = random.choice(links[:min(5, len(links))])
                await random_link.hover()
                await asyncio.sleep(random.uniform(0.5, 1.5))
        except Exception:
            pass
        
        log("OK", "Human warm-up complete - session established")
        return True
        
    except Exception as e:
        log("WARN", f"Warm-up partial: {e}")
        return False


async def continuous_mouse_jitter(page, stop_event):
    """
    Background task that subtly moves the mouse at random intervals.
    Helps maintain "human presence" during page loads.
    """
    import random
    import asyncio
    
    while not stop_event.is_set():
        try:
            await asyncio.sleep(random.uniform(3, 8))
            
            if stop_event.is_set():
                break
                
            # Small random movement
            x_offset = random.randint(-30, 30)
            y_offset = random.randint(-30, 30)
            
            # Get current position and add offset (stay within bounds)
            try:
                await page.mouse.move(
                    random.randint(300, 900),
                    random.randint(200, 600),
                    steps=random.randint(3, 8)
                )
            except Exception:
                pass  # Page might be navigating
                
        except asyncio.CancelledError:
            break
        except Exception:
            pass


def normalize_seed_url(url: str) -> str:
    """Normalize a seed URL for consistent registry lookup.
    
    Removes trailing slashes, lowercases the domain, sorts query parameters,
    and removes page numbers to get a canonical form.
    """
    from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
    
    parts = urlsplit(url.strip())
    
    # Lowercase the netloc (domain)
    netloc = parts.netloc.lower()
    
    # Remove trailing slash and page numbers from path
    path = parts.path.rstrip("/")
    path = re.sub(r"/pagina-\d+$", "", path)
    path = re.sub(r"/pagina-\d+\.htm$", "", path)
    
    # Sort query parameters for consistent comparison
    query_params = parse_qsl(parts.query)
    sorted_query = urlencode(sorted(query_params))
    
    return urlunsplit((parts.scheme, netloc, path, sorted_query, ""))


def load_scrape_history() -> dict:
    """Load the scrape history registry from JSON file.
    
    Handles both old array format and new dictionary format.
    Migrates old format to new format automatically.
    """
    if not os.path.exists(SCRAPE_HISTORY_FILE):
        return {}
    
    try:
        with open(SCRAPE_HISTORY_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}
    
    # Check if it's the old array format
    if isinstance(data, list):
        # Migrate old array format to new dictionary format
        # Old format: [{"seed_url": "...", "output_file": "...", "filename": "...", ...}, ...]
        # New format: {"normalized_url": {"output_file": "filename.xlsx", ...}, ...}
        migrated = {}
        for entry in data:
            seed_url = entry.get("seed_url", "")
            if seed_url:
                normalized = normalize_seed_url(seed_url)
                # Use filename (just the basename) instead of full path
                filename = entry.get("filename") or os.path.basename(entry.get("output_file", ""))
                migrated[normalized] = {
                    "output_file": filename,
                    "last_scraped": entry.get("timestamp", ""),
                    "properties_count": entry.get("properties_count", 0),
                    "pages_scraped": 0,  # Not available in old format
                    "original_url": seed_url
                }
        
        # Save migrated format
        if migrated:
            save_scrape_history(migrated)
        
        return migrated
    
    # Already in new dictionary format
    return data if isinstance(data, dict) else {}


def save_scrape_history(history: dict) -> None:
    """Save the scrape history registry to JSON file (atomic write)."""
    # Ensure output directory exists
    os.makedirs(os.path.dirname(SCRAPE_HISTORY_FILE), exist_ok=True)
    
    # Write to temp file first, then rename (atomic)
    temp_file = SCRAPE_HISTORY_FILE + ".tmp"
    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        if os.path.exists(SCRAPE_HISTORY_FILE):
            os.remove(SCRAPE_HISTORY_FILE)
        os.rename(temp_file, SCRAPE_HISTORY_FILE)
    except Exception as e:
        if os.path.exists(temp_file):
            os.remove(temp_file)
        raise e


def lookup_seed_url(seed_url: str) -> Optional[dict]:
    """Look up a seed URL in the registry and return its metadata if found."""
    history = load_scrape_history()
    normalized = normalize_seed_url(seed_url)
    return history.get(normalized)


def register_scrape(seed_url: str, output_file: str, properties_count: int, pages_scraped: int) -> None:
    """Register a completed scrape in the history registry."""
    from datetime import datetime
    
    history = load_scrape_history()
    normalized = normalize_seed_url(seed_url)
    
    history[normalized] = {
        "output_file": output_file,
        "last_scraped": datetime.now().isoformat(),
        "properties_count": properties_count,
        "pages_scraped": pages_scraped,
        "original_url": seed_url  # Keep original for reference
    }
    
    save_scrape_history(history)


class BrowserClosedException(Exception):
    """Raised when the browser is closed by the user during scraping."""
    pass


class BlockedException(Exception):
    """Raised when Idealista blocks access due to 'uso indebido'."""
    pass


@dataclass
class ScraperController:
    """Controller for the Idealista scraper with pause/stop and callbacks."""
    
    seed_url: str
    mode: str = "stealth"  # "stealth" or "fast"
    out_xlsx: str = "idealista.xlsx"
    sheet_name: str = "idealista"
    output_dir: str = DEFAULT_OUTPUT_DIR  # Configurable output directory
    dual_mode_url: Optional[str] = None  # Second URL for DUAL MODE (same browser session)
    use_vpn: bool = False
    browser_engine: str = "chromium"  # "chromium" or "firefox" - for multi-browser rotation
    rotate_every: int = 5  # Rotate every N properties or pages? User said provinces, but here we only have one URL.
    # For standard scraper, maybe rotate every N pages.
    
    # Smart Enrichment Mode
    smart_enrichment: bool = False  # If True, use province-file mapping and skip already enriched URLs
    province_name: Optional[str] = None  # Province name for file lookup (e.g., "Toledo")
    operation_type: Optional[str] = None  # "venta" or "alquiler"
    forced_target_file: Optional[str] = None  # Manually selected target file to override auto-detection
    
    # Callbacks
    on_log: Optional[Callable[[str, str], None]] = None
    on_property: Optional[Callable[[dict], None]] = None
    on_status: Optional[Callable[[str], None]] = None
    on_progress: Optional[Callable[[dict], None]] = None  # For page/property progress
    on_browser_closed: Optional[Callable[[], None]] = None  # Called when browser is closed by user
    
    # State
    status: str = "idle"
    is_running: bool = False
    scraped_properties: List[dict] = field(default_factory=list)
    output_file: Optional[str] = None
    
    # Progress tracking
    total_properties_expected: int = 0
    total_pages_expected: int = 0
    current_page: int = 0
    current_property_count: int = 0
    
    # Internal state
    _stop_evt: Optional[asyncio.Event] = None
    _pause_evt: Optional[asyncio.Event] = None
    _processed: Set[str] = field(default_factory=set)
    _inflight: Set[str] = field(default_factory=set)
    _recent: Dict[str, float] = field(default_factory=dict)
    _index_map: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    _detected_sheet: Optional[str] = None
    _detected_city: Optional[str] = None  # City extracted from listing h1 header
    _is_room_mode: bool = False  # True if scraping habitaciones (room rentals)
    _browser_closed: bool = False
    _pages_scraped: int = 0
    _browser: Optional[Any] = None  # Reference to browser for force close
    _context: Optional[Any] = None  # Reference to context for force close
    _last_log_time: float = 0
    _loop: Optional[asyncio.AbstractEventLoop] = None
    
    # Extra Stealth state
    _session_property_count: int = 0  # Properties scraped this session (for rest breaks)
    _next_coffee_break: int = 0  # Property count for next coffee break
    _total_session_count: int = 0  # Total across rest breaks
    
    # Checkpoint saving state
    _last_checkpoint_idx: int = 0  # Index of last saved property
    _checkpoint_interval: int = 50  # Save every N properties
    _target_file: Optional[str] = None  # Cached target filename for checkpoints
    
    # Smart Enrichment state
    _enriched_urls: Set[str] = field(default_factory=set)  # URLs already enriched (skip completely)
    _all_existing_urls: Dict[str, dict] = field(default_factory=dict)  # All URLs in file with metadata
    _province_target_file: Optional[str] = None  # Province-based target file
    
    def __post_init__(self):
        self._stop_evt = asyncio.Event()
        self._pause_evt = asyncio.Event()
        self._pause_evt.set()  # Not paused initially
        self.scraped_properties = []
        self._processed = set()
        self._inflight = set()
        self._recent = {}
        self._index_map = {}
        self._stopped_by_user = False
        self._last_log_time = time.time()
    
    def log(self, level: str, message: str):
        """Log a message and send to callback if set."""
        self._last_log_time = time.time()
        if self.on_log:
            self.on_log(level, message)
    
    def emit_progress(self):
        """Emit progress update to UI."""
        if self.on_progress:
            self.on_progress({
                'current_page': self.current_page,
                'total_pages': self.total_pages_expected,
                'current_properties': self.current_property_count,
                'total_properties': self.total_properties_expected
            })
    
    def get_delays(self) -> Tuple[Tuple[float, float], Tuple[float, float], Tuple[float, float]]:
        """Get delay ranges based on current mode."""
        if self.mode == "fast":
            return FAST_SCROLL_PAUSE_RANGE, FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE
        elif self.mode == "stealth":
            return EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE
        return EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE
    
    async def simulate_reading_time(self, description: Optional[str]):
        """Simulate reading time based on description length (Extra Stealth only)."""
        if self.mode != "stealth" or not description:
            return
        char_count = len(description)
        reading_time = (char_count / 100) * EXTRA_STEALTH_READING_TIME_PER_100_CHARS
        reading_time = min(reading_time, 30)  # Cap at 30 seconds
        if reading_time > 0.5:
            self.log("INFO", f"⏸️ Anti-bot: Simulando tiempo de lectura ({reading_time:.1f}s)")
            
            # Interruptible sleep
            remaining = reading_time
            while remaining > 0:
                if self._stop_evt.is_set():
                    break
                chunk = min(1.0, remaining)
                await asyncio.sleep(chunk)
                remaining -= chunk
        self.log("DEBUG_TIMING", "Reading simulation finished")
    
    async def simulate_mouse_movement(self, page):
        """Simulate natural mouse movements (Extra Stealth only)."""
        if self.mode != "stealth":
            return
        try:
            # Get viewport size
            viewport = page.viewport_size or {"width": 1920, "height": 1080}
            width, height = viewport["width"], viewport["height"]
            
            # Move to 2-4 random points with organic delays
            num_moves = random.randint(2, 4)
            self.log("INFO", f"🖱️ Anti-bot: Simulando movimiento de ratón ({num_moves} posiciones)")
            for i in range(num_moves):
                if self._stop_evt.is_set():
                    break
                x = random.randint(100, width - 100)
                y = random.randint(100, height - 100)
                # Move with slight delay to simulate human movement
                await page.mouse.move(x, y, steps=random.randint(5, 15))
                await asyncio.sleep(random.uniform(0.1, 0.4))
            self.log("DEBUG_TIMING", "Mouse movement simulation finished")
        except Exception:
            pass  # Ignore mouse movement errors
    
    async def variable_scroll(self, page):
        """Perform variable scroll pattern (Extra Stealth only)."""
        scroll_pause = self.get_delays()[0]
        
        if self.mode == "stealth":
            # Sometimes scroll up a bit first
            scroll_up_first = random.random() < 0.3
            if scroll_up_first:
                self.log("INFO", "📜 Anti-bot: Scroll variable (subiendo primero)")
                await page.evaluate('window.scrollBy(0, -150)')
                await asyncio.sleep(random.uniform(0.3, 0.8))
            
            # Variable scroll amounts
            for step in range(SCROLL_STEPS):
                scroll_amount = random.randint(200, 500)
                await page.evaluate(f'window.scrollBy(0, {scroll_amount})')
                await asyncio.sleep(random.uniform(*scroll_pause))
                
                # Occasionally pause mid-scroll as if reading
                if random.random() < 0.2:
                    pause_time = random.uniform(1.0, 3.0)
                    self.log("INFO", f"📜 Anti-bot: Pausa de lectura durante scroll ({pause_time:.1f}s)")
                    await asyncio.sleep(pause_time)
            
            # Sometimes scroll back up slightly
            if random.random() < 0.2:
                self.log("INFO", "📜 Anti-bot: Scroll variable (volviendo arriba)")
                await page.evaluate('window.scrollBy(0, -100)')
                await asyncio.sleep(random.uniform(0.2, 0.5))
        else:
            # Standard scrolling for other modes
            for _ in range(SCROLL_STEPS):
                await page.evaluate('window.scrollBy(0, document.body.scrollHeight / 3)')
                await asyncio.sleep(random.uniform(*scroll_pause))
    
    async def maybe_coffee_break(self):
        """Take a random coffee break if due (Extra Stealth only)."""
        if self.mode != "stealth":
            return
        
        # Initialize next coffee break if not set
        if self._next_coffee_break == 0:
            self._next_coffee_break = random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
        
        # Check if it's time for a coffee break
        if self._session_property_count >= self._next_coffee_break:
            break_duration = random.uniform(*EXTRA_STEALTH_COFFEE_BREAK_RANGE)
            self.log("WARN", f"☕ Anti-bot: Pausa de descanso ({break_duration:.0f}s)")
            self.log("DEBUG_TIMING", f"Entering coffee break. Duration: {break_duration:.2f}s")
            
            if self.on_status:
                self.on_status("resting", duration=int(break_duration))
            
            # Wait loop
            remaining = break_duration
            
            while remaining > 0:
                if self._stop_evt.is_set():
                    self.log("INFO", "☕ Pausa interrumpida.")
                    break
                
                # Skip if mode switched to FAST
                if self.mode != "stealth":
                    self.log("INFO", "☕ Pausa omitida (cambiado a modo FAST).")
                    break
                    
                sleep_chunk = min(1.0, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
            
            self.log("DEBUG_TIMING", "Finished coffee break.")

            if self.on_status:
                self.on_status("running")
            
            # Schedule next coffee break
            self._next_coffee_break = self._session_property_count + random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
            self.log("WARN", f"☕ Anti-bot: Pausa terminada. Próxima en ~{self._next_coffee_break - self._session_property_count} propiedades")
    
    async def maybe_session_rest(self):
        """Take a long rest after session limit (Extra Stealth only)."""
        if self.mode != "stealth":
            return
        
        if self._session_property_count >= EXTRA_STEALTH_SESSION_LIMIT:
            rest_duration = random.uniform(*EXTRA_STEALTH_REST_DURATION_RANGE)
            # Round to nearest minute for cleaner display
            rest_duration = round(rest_duration / 60) * 60
            rest_mins = int(rest_duration // 60)
            self.log("WARN", f"😴 Anti-bot: Límite de sesión alcanzado ({EXTRA_STEALTH_SESSION_LIMIT} propiedades). Descansando {rest_mins} minutos...")
            self.log("DEBUG_TIMING", f"Entering session rest break. Duration: {rest_duration}s")
            
            if self.on_status:
                self.on_status("resting", duration=int(rest_duration))
            
            # Wait loop
            remaining = rest_duration
            
            while remaining > 0:
                if self._stop_evt.is_set():
                    self.log("INFO", "😴 Descanso de sesión interrumpido.")
                    break
                
                # Skip if mode switched to FAST
                if self.mode != "stealth":
                    self.log("INFO", "😴 Descanso omitido (cambiado a modo FAST).")
                    break
                    
                sleep_chunk = min(1.0, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
            
            self.log("DEBUG_TIMING", "Finished session rest break.")

            if self.on_status:
                self.on_status("running")
            
            # Reset session counter
            self._total_session_count += self._session_property_count
            self._session_property_count = 0
            self._next_coffee_break = random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
            self.log("INFO", f"😴 Anti-bot: Descanso completado. Total scrapeado: {self._total_session_count}. Nueva sesión...")
    
    def get_random_user_agent(self) -> str:
        """Get a random user agent from the rotation list."""
        return random.choice(USER_AGENTS)
    
    def get_random_viewport(self) -> dict:
        """Get a random viewport size from the rotation list."""
        width, height = random.choice(VIEWPORT_SIZES)
        return {"width": width, "height": height}
    
    def pause(self):
        """Pause scraping."""
        self._pause_evt.clear()
        self.status = "paused"
        self.log("INFO", "Scraping paused")
        if self.on_status:
            self.on_status("paused")
    
    def resume(self):
        """Resume scraping."""
        self._pause_evt.set()
        self.status = "running"
        self.log("INFO", "Scraping resumed")
        if self.on_status:
            self.on_status("running")
    
    def stop(self):
        """Stop scraping and trigger export."""
        self._stopped_by_user = True
        self._stop_evt.set()
        self._pause_evt.set()  # Unpause to allow graceful stop
        self.status = "stopping"
        self.log("INFO", "Stopping scraper...")
        if self.on_status:
            self.on_status("stopping")
        # Force close browser to unblock any stuck operations
        self._force_close_browser()
    
    def _force_close_browser(self):
        """Emergency cleanup: close browser context if still open."""
        if self._context:
            try:
                # Use scraper's loop to avoid "no running event loop" errors
                loop = self._loop or asyncio.get_event_loop()
                if loop and loop.is_running():
                    asyncio.run_coroutine_threadsafe(self._context.close(), loop)
                    self.log("INFO", "Browser context closure triggered via loop.")
                else:
                    self.log("WARN", "Could not close browser: No running event loop found.")
            except Exception as e:
                self.log("WARN", f"Error in _force_close_browser: {e}")
            self._context = None

    async def _heartbeat_monitor(self):
        """Background task to log activity periodically and detect hangs."""
        self.log("INFO", "💓 Heartbeat monitor started (60s check, 300s alarm)")
        while not self._stop_evt.is_set():
            await asyncio.sleep(60)
            idle_time = time.time() - self._last_log_time
            if idle_time > 300: # 5 minutes of silence
                self.log("WARN", f"💓 Heartbeat: No activity for {idle_time/60:.0f}m. Scraper might be hanging or waiting silently.")
                self.log("INFO", f"💓 Status: {self.status}, Page: {self.current_page}, Property: {self.current_property_count}/{self.total_properties_expected}")
            elif idle_time > 60:
                # Normal heartbeat log at DEBUG level (not seen by user unless verbose)
                pass 

    def set_mode(self, mode: str):
        """Update scraping mode dynamically."""
        if mode not in ["fast", "stealth"]:
            return
        
        old_mode = self.mode
        self.mode = mode
        self.log("INFO", f"Switched mode: {old_mode} -> {mode}")
    
    def save_state(self, current_page: int, target_file: Optional[str] = None):
        """Save current scraping state for resume functionality."""
        from datetime import datetime
        state = {
            "seed_url": self.seed_url,
            "mode": self.mode,
            "current_page": current_page,
            "processed_urls": list(self._processed),
            "target_file": target_file or self.output_file,
            "output_dir": self.output_dir,
            "total_properties_expected": self.total_properties_expected,
            "total_pages_expected": self.total_pages_expected,
            "scraped_count": len(self.scraped_properties),
            "detected_sheet": self._detected_sheet,
            "timestamp": datetime.now().isoformat()
        }
        try:
            with open(RESUME_STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state, f, indent=2, ensure_ascii=False)
            self.log("INFO", f"Resume state saved at page {current_page}")
        except Exception as e:
            self.log("WARN", f"Failed to save resume state: {e}")
    
    @staticmethod
    def load_state() -> Optional[dict]:
        """Load previously saved resume state."""
        try:
            if os.path.exists(RESUME_STATE_FILE):
                with open(RESUME_STATE_FILE, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception:
            pass
        return None
    
    @staticmethod
    def clear_state():
        """Clear saved resume state."""
        try:
            if os.path.exists(RESUME_STATE_FILE):
                os.remove(RESUME_STATE_FILE)
        except Exception:
            pass

    def handle_blocked_profile(self):
        """Archive the current profile if it has been blocked/poisoned."""
        import shutil
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"stealth_profile_BLOCKED_{timestamp}"
        backup_path = os.path.join(os.path.dirname(STEALTH_PROFILE_DIR), backup_name)
        
        self.log("WARN", "☣️  PROFILE POISONED: Dealing with blocked profile...")
        
        if os.path.exists(STEALTH_PROFILE_DIR):
            try:
                # We assume browser is already closed by now
                shutil.move(STEALTH_PROFILE_DIR, backup_path)
                self.log("WARN", f"♻️  Moved poisoned profile to: {backup_name}")
                self.log("OK", "✨ Next run will generate a fresh, clean profile.")
            except Exception as e:
                self.log("ERR", f"Failed to archive profile: {e}")

    async def _save_checkpoint(self, additions: List[dict], target_file: Optional[str], existing_df, carry_cols: Set[str]):
        """Periodically save current progress to Excel."""
        if not additions:
            return
        
        self.log("INFO", f"💾 Auto-checkpoint: Saving {len(additions)} properties to {target_file or 'Excel'}")
        try:
            # Pass stop check to prevent hangs
            check_stop = lambda: self._stop_evt.is_set()
            
            if self.smart_enrichment and self._province_target_file:
                export_split_by_distrito(existing_df, additions, os.path.join(self.output_dir, self._province_target_file), carry_cols, check_stop=check_stop)
            else:
                export_single_sheet(existing_df, additions, os.path.join(self.output_dir, target_file or self.out_xlsx), self._detected_sheet or self.sheet_name, carry_cols)
            
            self._last_checkpoint_idx = len(self.scraped_properties)
        except Exception as e:
            self.log("WARN", f"Checkpoint failed: {e}")
    
    async def _goto_with_retry(self, page, url: str) -> None:
        """Navigate to URL with retry logic. Detects browser close with 120s guard."""
        delay = RETRY_BASE_DELAY
        last_err: Optional[Exception] = None
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            if self._stop_evt.is_set():
                return
            try:
                t_nav_start = time.time()
                self.log("DEBUG_TIMING", f"Navigating to {url} (Attempt {attempt})...")
                
                # Global guard to prevent silent hangs (120s max for any navigation)
                try:
                    await asyncio.wait_for(
                        page.goto(url, wait_until=GOTO_WAIT_UNTIL, timeout=60000),
                        timeout=120.0
                    )
                except asyncio.TimeoutError:
                    self.log("ERR", f"⏰ NAVIGATION HANG: {url} timed out after 120s guard.")
                    raise Exception("NAVIGATION_HANG")

                self.log("DEBUG_TIMING", f"Navigation completed in {time.time() - t_nav_start:.2f}s")
                
                # Humanize interaction after reaching the page
                await simulate_human_interaction(page)
                
                # Check for CAPTCHA/Bot protection
                try:
                    title = await page.title()
                    t_lower = title.lower()
                    
                    # Check for permanent block (uso indebido)
                    page_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                    page_text_lower = page_text.lower() if page_text else ""
                    
                    # Check for deactivated listing
                    if "anuncio ya no está publicado" in page_text_lower or "este anuncio no está publicado" in page_text_lower:
                        self.log("WARN", f"El anuncio ya no está activo: {url}")
                        return
                    
                    if "uso indebido" in page_text_lower or "se ha bloqueado" in page_text_lower or "uso no autorizado" in page_text_lower or "access denied" in page_text_lower:
                        self.log("ERR", "🚫 BLOCK DETECTED: 'Uso indebido' or 'Access Denied'. Stopping immediately.")
                        play_blocked_alert()
                        # Mark profile as blocked for cooldown rotation
                        mark_profile_blocked(self.browser_engine)
                        self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                        # CRITICAL: Raise BlockedException to trigger profile nuking
                        raise BlockedException("Acceso bloqueado por uso indebido")
                    
                    # Common indicators for Idealista/Cloudflare blockage
                    is_captcha = (
            "attention" in t_lower or 
            "moment" in t_lower or 
            "challenge" in t_lower or 
            "robot" in t_lower or 
            "captcha" in t_lower or
            "access denied" in t_lower or
            "security" in t_lower or
            "peticiones" in t_lower or
            "verificación" in t_lower or
            "verification" in t_lower or
            "desliza" in t_lower or  # Slider CAPTCHA
            "asegurar tu acceso" in t_lower or  # Idealista CAPTCHA message
            "muchas peticiones" in page_text_lower  # Rate limit message
        )

                    if is_captcha:
                        self.log("WARN", f"CAPTCHA DETECTED on {url} (Title: '{title}')")
                        
                        # 1. Try automatic slider solve
                        self.log("INFO", "🤖 Attempting automatic slider solve...")
                        if await solve_slider_captcha(page):
                            # Check again
                            try:
                                title_after = await page.title()
                                if not any(kw in (title_after or "").lower() for kw in ["moment", "challenge", "robot", "captcha", "verification"]):
                                    self.log("OK", "✅ CAPTCHA solved automatically!")
                                    return 
                            except: pass
                            self.log("WARN", "❌ Slider moved but CAPTCHA still present.")
                        else:
                            self.log("WARN", "❌ Automatic solver could not find slider.")

                        self.log("WARN", ">>> PLEASE SOLVE THE CAPTCHA MANUALLY IN THE BROWSER <<<")
                        if self.on_status:
                            self.on_status("captcha")
                        
                        # Loop until resolved - with 60s timeout to prevent infinite hang
                        captcha_wait_start = asyncio.get_running_loop().time()
                        captcha_timeout = 60  # seconds
                        
                        while True:
                            if self._stop_evt.is_set():
                                break
                            
                            # Check timeout
                            elapsed = asyncio.get_running_loop().time() - captcha_wait_start
                            if elapsed > captcha_timeout:
                                self.log("WARN", f"⏰ CAPTCHA wait timeout ({captcha_timeout}s). Checking page state...")
                                # Check if we can proceed anyway
                                try:
                                    final_title = await page.title()
                                    if "idealista" in final_title.lower():
                                        self.log("INFO", "Page appears normal despite timeout. Continuing...")
                                        if self.on_status:
                                            self.on_status("running")
                                        break
                                except: pass
                                # Still stuck - mark as block and raise
                                self.log("ERR", "CAPTCHA timeout - triggering auto-restart")
                                mark_profile_blocked(self.browser_engine)
                                raise Exception("CAPTCHA_TIMEOUT")
                            
                            play_captcha_alert()
                            
                            # interruptible wait (10s)
                            for _ in range(100):
                                if self._stop_evt.is_set():
                                    break
                                await asyncio.sleep(0.1)
                            
                            if self._stop_evt.is_set():
                                break
                            
                            try:
                                # Check title again
                                new_title = await page.title()
                                nt_lower = new_title.lower()
                                # If title looks like normal Idealista page, assume solved
                                if "idealista" in nt_lower and "captcha" not in nt_lower and "attention" not in nt_lower:
                                    self.log("OK", "CAPTCHA solved! Resuming...")
                                    if self.on_status:
                                        self.on_status("running")
                                    break
                            except Exception:
                                pass
                except Exception:
                    pass


                await self._interruptible_sleep(3.0)
                return
            except Exception as e:
                error_msg = str(e).lower()
                # Detect browser close - pause and notify UI
                if "browser has been closed" in error_msg or "target page, context or browser has been closed" in error_msg:
                    self.log("WARN", "Browser was closed. Pausing scraper...")
                    self._browser_closed = True
                    self.pause()  # Pause instead of stop
                    if self.on_browser_closed:
                        self.on_browser_closed()
                    raise BrowserClosedException("Browser was closed by user")
                
                last_err = e
                self.log("WARN", f"goto attempt {attempt}/{RETRY_MAX_ATTEMPTS} failed: {e}")
                await self._interruptible_sleep(delay)
                delay *= 2
        if last_err:
            raise last_err
    
    async def _wait_for_pause(self):
        """Wait if paused."""
        if not self._pause_evt.is_set() and not self._stop_evt.is_set():
            self.log("WARN", "⏳ Scraper paused. Waiting for resume...")
            while not self._pause_evt.is_set() and not self._stop_evt.is_set():
                await asyncio.sleep(1.0)
            self.log("INFO", "▶️ Scraper resumed.")

    async def _interruptible_sleep(self, duration: float):
        """Sleep for duration, but wake up immediately if stopped."""
        if duration <= 0:
            return
        
        if duration > 10:
            self.log("INFO", f"⏳ Pausa larga detectada: {duration:.2f}s...")
        
        self.log("DEBUG_TIMING", f"Sleeping for {duration:.2f}s...")
        
        remaining = duration
        while remaining > 0:
            if self._stop_evt.is_set():
                break
            chunk = min(0.5, remaining)  # 0.5s check interval
            await asyncio.sleep(chunk)
            remaining -= chunk
    
    def _export_to_excel(self, additions: List[dict], target_file: Optional[str], expired_urls: List[str]):
        """Export scraped data to Excel file."""
        if not additions:
            self.log("INFO", "No new properties to export.")
            return
        
        self.log("INFO", "Exporting data to Excel...")
        
        # Guard for PermissionError hangs
        check_stop = lambda: self._stop_evt.is_set()
        
        # Use filename from registry if available, otherwise build it
        if target_file:
            out_effective = os.path.join(self.output_dir, target_file)
        else:
            ciudad = additions[0].get("Ciudad") if additions else None
            category = self._detected_sheet or "unknown"
            
            if ciudad:
                ciudad_clean = sanitize_filename_part(ciudad)
                out_effective = f"idealista_{ciudad_clean}_{category}.xlsx"
            else:
                out_effective = f"idealista_{category}.xlsx"
            out_effective = os.path.join(self.output_dir, out_effective)
        
        self.log("INFO", f"Output path: {out_effective}")
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load existing data and export with split by Distrito
        existing_df, _, _ = load_existing_single_sheet(out_effective, self._detected_sheet or self.sheet_name)
        self.log("INFO", f"Loaded {len(existing_df)} existing rows from file")
        
        # Delete expired URLs from existing data
        if expired_urls and not existing_df.empty and "URL" in existing_df.columns:
            initial_count = len(existing_df)
            existing_df = existing_df[~existing_df["URL"].isin(expired_urls)]
            deleted_count = initial_count - len(existing_df)
            if deleted_count > 0:
                self.log("OK", f"Deleted {deleted_count} expired listings from Excel")
        
        # Use heartbeat refresh during potentially long export
        self.log("DEBUG_TIMING", "Starting final export to Excel mapping...")
        export_split_by_distrito(existing_df, additions, out_effective, carry_cols=set(), check_stop=check_stop)
        
        self.output_file = os.path.abspath(out_effective)
        self.log("OK", f"Saved {len(additions)} new/updated rows to {self.output_file}")
        
        # Register this scrape in the history registry
        page_num = self.current_page or 1
        total_properties = len(existing_df) + len(additions) if existing_df is not None else len(additions)
        register_scrape(
            self.seed_url,
            os.path.basename(out_effective),
            total_properties,
            page_num
        )
        self.log("INFO", f"Registered scrape: {os.path.basename(out_effective)} ({total_properties} properties)")
        self.log("OK", f"Archivo guardado: {self.output_file}")
    
    async def run(self):
        """Main scraping loop."""
        self._loop = asyncio.get_running_loop()
        self.is_running = True
        self.status = "running"
        self._stop_evt.clear()
        self._pause_evt.set()
        
        # Start heartbeat monitor
        heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        
        try:
            self.on_status("running")
        
        self.log("INFO", f"Starting scraper in {self.mode.upper()} mode")
        self.log("INFO", f"Seed URL: {self.seed_url}")
        
        # Detect room mode based on seed URL
        self._is_room_mode = "habitacion" in self.seed_url.lower()
        if self._is_room_mode:
            self.log("INFO", "Modo habitaciones detectado - usando columnas específicas para alquiler de habitaciones")

        
        scroll_pause, card_delay, post_card_delay = self.get_delays()
        
        # Log delay configuration for Extra Stealth
        if self.mode == "stealth":
            self.log("STEALTH", f"Ultra-long delays active: scroll {scroll_pause[0]:.1f}-{scroll_pause[1]:.1f}s, page {post_card_delay[0]:.1f}-{post_card_delay[1]:.1f}s")
        
        # === SEED URL REGISTRY LOOKUP ===
        # Check if this seed URL was scraped before and pre-load existing URLs
        target_file = None
        url_dates = {}
        preloaded_urls = set()
        
        registry_entry = lookup_seed_url(self.seed_url)
        if registry_entry:
            target_file = registry_entry.get("output_file")
            target_path = os.path.join(self.output_dir, target_file) if target_file else None
            
            if target_path and os.path.exists(target_path):
                self.log("INFO", f"Found previous scrape: {target_file}")
                url_dates = load_urls_with_dates(target_path)
                preloaded_urls = set(url_dates.keys())
                self.log("OK", f"Pre-loaded {len(preloaded_urls)} existing URLs from previous scrape")
                
                # Add to processed set to skip without navigation
                self._processed.update(preloaded_urls)
            else:
                # File doesn't exist yet - keep the registered filename, it will be created
                self.log("INFO", f"Registered file not found: {target_file} - will be created during this scrape")
        
        # === SMART ENRICHMENT MODE ===
        # If smart_enrichment is enabled, try to use province-file mapping
        if self.smart_enrichment:
            self.log("INFO", "🔍 Smart Enrichment Mode enabled")
            
            # Try to detect province/operation from URL if not already set
            if not self.province_name or not self.operation_type:
                detected_province, detected_operation = detect_province_and_operation(self.seed_url)
                if detected_province:
                    self.province_name = detected_province
                if detected_operation:
                    self.operation_type = detected_operation
            
            if self.province_name and self.operation_type:
                self.log("INFO", f"📍 Province: {self.province_name}, Operation: {self.operation_type}")
                
                # Get province-based target file
                if self.forced_target_file:
                    province_file = self.forced_target_file
                    self.log("INFO", f"📂 Using forced target file: {province_file}")
                else:
                    province_file, _, _ = get_output_file_for_url(self.seed_url)
                if province_file:
                    self._province_target_file = province_file
                    province_path = os.path.join(self.output_dir, province_file)
                    
                    # Override target_file with province-based file
                    target_file = province_file
                    self.log("INFO", f"📂 Province target file: {target_file}")
                    
                    # Load already enriched URLs (to skip completely)
                    if os.path.exists(province_path):
                        self._enriched_urls = load_enriched_urls(province_path)
                        self._all_existing_urls = load_all_urls_from_excel(province_path)
                        
                        enriched_count = len(self._enriched_urls)
                        total_in_file = len(self._all_existing_urls)
                        not_enriched = total_in_file - enriched_count
                        
                        self.log("OK", f"📊 File status: {total_in_file} total, {enriched_count} enriched, {not_enriched} pending")
                        
                        # Add enriched URLs to processed set (skip completely)
                        self._processed.update(self._enriched_urls)
                        self.log("INFO", f"⏭️ Will skip {enriched_count} already enriched properties")
                    else:
                        self.log("INFO", f"Province file not found - will create: {province_file}")
            else:
                self.log("WARN", "Could not detect province/operation from URL. Smart enrichment partially disabled.")
        
        additions: List[dict] = []
        expired_urls: List[str] = []  # URLs to delete from Excel (expired listings)
        
        # Automatic Recovery Loop
        max_restarts = 5
        restart_count = 0
        self.unauthorized_restart_count = 0  # Track "uso no autorizado" restarts
        
        while not self._stop_evt.is_set():
            target_file = self.output_file # Initialize safe default
            try:
                async with async_playwright() as pw:
                    # ========== MULTI-BROWSER ENGINE SELECTION ==========
                    engine = self.browser_engine
                    profile_dir = PROFILE_DIRS.get(engine, PROFILE_DIRS["chromium"])
                    os.makedirs(profile_dir, exist_ok=True)
                    
                    self.log("INFO", f"Launching browser: {engine.upper()} (Clean Profile 2026)...")
                    
                    # Select a random viewport for this session
                    viewport_width, viewport_height = random.choice(VIEWPORT_SIZES)
                    self.log("STEALTH", f"Using randomized viewport: {viewport_width}x{viewport_height}")
                    
                    # Clean Profile Strategy (2026) - Making the browser look vanilla
                    # Removed: --start-minimized, --disable-extensions, --disable-popup-blocking
                    # Removed: --disable-ipc-flooding-protection, disable-features=IsolateOrigins
                    chromium_args = [
                        "--no-first-run",
                        "--no-default-browser-check",
                        # Essential anti-bot flag (must keep)
                        "--disable-blink-features=AutomationControlled", 
                        "--password-store=basic",
                        "--use-mock-keychain",
                        "--force-color-profile=srgb",
                        "--metrics-recording-only",
                        "--export-tagged-pdf",
                    ]
                    
                    # Firefox-specific args (different format)
                    firefox_prefs = {
                        "dom.webdriver.enabled": False,
                        "useAutomationExtension": False,
                    }
                    
                    try:
                        # Launch based on selected engine
                        if engine == "firefox":
                            ctx = await pw.firefox.launch_persistent_context(
                                user_data_dir=profile_dir,
                                headless=False,
                                viewport={"width": viewport_width, "height": viewport_height},
                                firefox_user_prefs=firefox_prefs,
                                timeout=60000, # Fail fast (1 min) instead of 3 min hang
                            )
                            self.log("OK", f"🦊 Firefox launched with profile: {os.path.basename(profile_dir)}")
                        else:
                            # Default: Chromium
                            ctx = await pw.chromium.launch_persistent_context(
                                user_data_dir=profile_dir,
                                headless=False,
                                args=chromium_args,
                                ignore_default_args=["--enable-automation"],
                                viewport={"width": viewport_width, "height": viewport_height},
                                user_agent=self.get_random_user_agent(),
                                timeout=60000, # Fail fast (1 min)
                            )
                            self.log("OK", f"🌐 Chromium launched with profile: {os.path.basename(profile_dir)}")
                        
                        browser = None  # No separate browser object with persistent context
                        self._context = ctx  # Store reference for force close on stop
                        
                        # Record which engine we're using for rotation tracking
                        set_last_engine(engine)
                        
                        # ========== PHASE 1: DEEP FINGERPRINT SPOOFING ==========
                        # Inject comprehensive anti-detection script BEFORE any navigation
                        await ctx.add_init_script(DEEP_STEALTH_SCRIPT)
                        self.log("STEALTH", "Deep fingerprint spoofing injected")
                        
                        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                        
                        # Apply playwright-stealth patches (additional layer) - works for both engines
                        if HAS_STEALTH and stealth_async:
                            await stealth_async(page)
                            self.log("STEALTH", "playwright-stealth patches applied")
                        
                        # Add realistic HTTP headers
                        await ctx.set_extra_http_headers({
                            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
                            'DNT': '1',
                            'Upgrade-Insecure-Requests': '1',
                        })
                        
                        # ========== PHASE 2: HUMAN BEHAVIOR SIMULATION ==========
                        # NOTE: Google warmup removed (2026-02-07) - added delay without evading detection
                        # Keeping only mouse jitter for human presence simulation
                        
                        # Start background mouse jitter task (maintains human presence)
                        mouse_jitter_task = asyncio.create_task(
                            continuous_mouse_jitter(page, self._stop_evt)
                        )
                        
                    except Exception as e:
                        self.log("ERR", f"Could not launch browser: {e}")
                        self.is_running = False
                        self.status = "error"
                        if self.on_status:
                            self.on_status("error", error=str(e))
                        self._stop_evt.set()
                        break
            
                    # Navigate to seed URL
                    try:
                        self.log("INFO", f"Navigating to seed URL...")
                        await page.goto(self.seed_url, wait_until="domcontentloaded", timeout=60000)
                        await asyncio.sleep(3.0)
                        self.log("OK", "Opened seed URL")
                    except Exception as e:
                        self.log("ERR", f"Could not open seed URL: {e}")
            
                    # Try to dismiss cookie consent banners that might block content
                    try:
                        await page.evaluate(r"""() => {
                            // Click common accept buttons for cookie consent
                            const acceptBtns = document.querySelectorAll(
                                '[id*="accept"], [class*="accept"], [id*="cookie"] button, ' +
                                '[class*="cookie"] button, [data-testid*="accept"], ' +
                                '.didomi-continue-without-agreeing, #didomi-notice-agree-button, ' +
                                '.onetrust-accept-btn, #onetrust-accept-btn-handler'
                            );
                            for (const btn of acceptBtns) {
                                if (btn.offsetParent !== null) { // visible
                                    btn.click();
                                    return true;
                                }
                            }
                            return false;
                        }""")
                        await asyncio.sleep(1.0)
                    except Exception:
                        pass
            
                    # =============================================================================
                    # PRIORITY BLOCK DETECTION (Moved before property count extraction)
                    # =============================================================================
                    try:
                        # minimal wait for body to stand chance of having text
                        await asyncio.sleep(2.0)
                        
                        # 1. Check page title for clear block indicators
                        page_title = await page.title()
                        title_lower = page_title.lower() if page_title else ""
                        
                        if "idealista" in title_lower and ("captcha" in title_lower or "challenge" in title_lower):
                             raise Exception("CAPTCHA detected in title")
                             
                        # 2. Check body text for specific block messages
                        # capturing innerText is fast and effective for these specific blocking pages
                        body_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                        text_lower = body_text.lower() if body_text else ""
                        
                        # Updated keywords as per user request (2026-02-07)
                        block_keywords = [
                            "el acceso se ha bloqueado", 
                            "estamos recibiendo muchas peticiones tuyas",
                            "uso indebido", # Keep as fallback
                            "access denied"
                        ]
                        
                        for kw in block_keywords:
                            if kw in text_lower:
                                self.log("WARN", f"⚠️ EARLY BLOCK DETECTED: Found '{kw}' in page body.")
                                raise BlockedException(f"Early block detection: {kw}")
                                
                    except BlockedException as be:
                        # Re-raise to be caught by the main loop handler which handles rotation
                        raise be
                    except Exception:
                        # Ignore other errors here, let main logic proceed if no clear block found
                        pass
                        
                    # =============================================================================
                    # END PRIORITY DETECTION
                    # =============================================================================

                    # Detect sheet name and total properties from page
                    self.log("INFO", "Waiting for page to load property count...")
                    h1txt = ""
                    total_count = 0
            
                    # Retry h1 extraction up to 4 times (page may navigate/redirect)
                    # Wait 4 seconds per attempt to allow slow-loading pages
                    for attempt in range(4):
                        try:
                            if self._stop_evt.is_set():
                                break
                            # Wait for page to stabilize (increased from 2s to 4s)
                            await self._interruptible_sleep(4.0)
                    
                            # Wait for network to be idle
                            try:
                                await page.wait_for_load_state("networkidle", timeout=5000)
                            except Exception:
                                pass
                    
                            # Debug: Log page title to see what page we're on
                            page_title = await page.title()
                            if attempt == 0:
                                self.log("INFO", f"Page title: '{page_title[:80] if page_title else 'empty'}'")
                    
                            # Try to get h1 text
                            h1txt = await page.evaluate(r"""() => {
                                const el = document.querySelector('#h1-container__text') || 
                                           document.querySelector('#h1-container') || 
                                           document.querySelector('h1');
                                return el ? el.textContent.trim() : '';
                            }""") or ""
                    
                            if h1txt:
                                self.log("INFO", f"H1 text: '{h1txt[:100]}'")
                                # Extract count immediately to check if we found properties
                                match = re.search(r'(\d{1,3}(?:\.\d{3})*)\s*(?:vivienda|pisos?|casas?|inmuebles?|anuncios?|habitaci[oó]n|habitaciones)', h1txt, re.IGNORECASE)
                                if match:
                                    total_count = int(match.group(1).replace('.', ''))
                                    self.log("INFO", f"Extracted count from h1: {total_count}")
                                    break  # Success, exit retry loop
                                # H1 found but no count - continue retrying
                            else:
                                # Log what we see for debugging
                                if attempt == 1:
                                    body_snippet = await page.evaluate(r"""() => {
                                        const body = document.body;
                                        return body ? body.innerText.substring(0, 200) : '';
                                    }""") or ""
                                    self.log("WARN", f"H1 empty, page content: '{body_snippet[:100]}...'")
                    
                        except Exception as e:
                            if attempt < 3:
                                self.log("WARN", f"H1 extraction attempt {attempt+1} failed: {e}. Retrying...")
                                await self._interruptible_sleep(1.0)
                            else:
                                self.log("WARN", f"H1 extraction failed after 4 attempts: {e}")
            
                    # If still no count after all retries, likely a block/CAPTCHA
                    if total_count == 0:
                        self.log("WARN", "⚠️ BLOCK DETECTED: 0 properties found on page (CAPTCHA/Block)")
                        
                        # Mark current profile as blocked
                        current_engine = self.browser_engine
                        mark_profile_blocked(current_engine)
                        self.log("WARN", f"⏳ Profile '{current_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                        
                        # Save state for resume
                        self.save_state(1, target_file)
                        
                        # Cancel mouse jitter and close browser
                        try:
                            if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                                mouse_jitter_task.cancel()
                            await (browser.close() if browser else ctx.close())
                        except:
                            pass
                        
                        # ROTATION: Try to switch to a different engine immediately
                        next_engine = select_next_engine(current_engine)
                        
                        if next_engine and next_engine != current_engine:
                            self.log("INFO", f"🔄 ROTATION: Switching to fresh engine '{next_engine.upper()}' for immediate retry!")
                            self.browser_engine = next_engine
                            wait_time = random.randint(5, 15)
                            
                            if self.on_status:
                                self.on_status("blocked", message=f"Rotando a {next_engine.upper()} en {wait_time}s...")
                            
                            await self._interruptible_sleep(float(wait_time))
                            
                            if self._stop_evt.is_set():
                                self.log("INFO", "Retry cancelled by user.")
                                self.is_running = False
                                self.status = "stopped"
                                break
                            
                            self.log("OK", f"🔄 Restarting with {next_engine.upper()}...")
                            continue  # Loop back to restart with new browser
                        else:
                            # All engines blocked - must wait for cooldown
                            cooldown_remaining = get_cooldown_remaining(current_engine)
                            wait_minutes = max(cooldown_remaining, 1)
                            self.log("WARN", f"⚠️ All browser profiles blocked. Waiting {wait_minutes} min for cooldown...")
                            
                            if self.on_status:
                                self.on_status("blocked", message=f"Esperando {wait_minutes} minutos para reintentar...")
                            
                            # Countdown
                            for remaining in range(wait_minutes, 0, -1):
                                if self._stop_evt.is_set():
                                    break
                                for _ in range(12):  # 12 * 5s = 60s
                                    if self._stop_evt.is_set():
                                        break
                                    await asyncio.sleep(5)
                            
                            if self._stop_evt.is_set():
                                self.log("INFO", "Retry cancelled by user.")
                                self.is_running = False
                                self.status = "stopped"
                                break
                            
                            self.log("OK", "🔄 Reintentando ahora...")
                            continue  # Loop back to restart browser

            
                    # Detect alquiler/venta from h1 text
                    h1_lower = h1txt.lower()
                    if "habitaci" in h1_lower and "alquiler" in h1_lower:
                        self._detected_sheet = "alquiler-habitaciones"
                    elif "alquiler" in h1_lower:
                        self._detected_sheet = "alquiler"
                    elif "venta" in h1_lower:
                        self._detected_sheet = "venta"
            
                    # Extract city from h1 text (after the comma, e.g., "39 casas en Los Cármenes, Madrid" -> "Madrid")
                    if "," in h1txt:
                        city_part = h1txt.split(",")[-1].strip()
                        # Remove any trailing text like "Capital" or clean up
                        city_part = re.sub(r'\s+(capital|provincia|centro|norte|sur|este|oeste).*$', '', city_part, flags=re.IGNORECASE).strip()
                        if city_part:
                            self._detected_city = city_part
                            self.log("INFO", f"Detected city from listing: {self._detected_city}")
            
                    # Set totals and emit progress
                    self.total_properties_expected = total_count
                    self.total_pages_expected = (total_count + 29) // 30 if total_count > 0 else 0
            
                    self.log("INFO", f"Total: {self.total_properties_expected} properties, {self.total_pages_expected} pages")
                    self.emit_progress()  # Send to UI immediately
            
                    if self._detected_sheet:
                        self.log("INFO", f"Detected category from page: '{self._detected_sheet}'")
                    else:
                        # Try to detect from seed URL
                        url_lower = self.seed_url.lower()
                        if "alquiler-habitacion" in url_lower:
                            self._detected_sheet = "alquiler-habitaciones"
                            self.log("INFO", "Detected category from URL: 'alquiler-habitaciones'")
                        elif "alquiler" in url_lower:
                            self._detected_sheet = "alquiler"
                            self.log("INFO", "Detected category from URL: 'alquiler'")
                        elif "venta" in url_lower:
                            self._detected_sheet = "venta"
                            self.log("INFO", "Detected category from URL: 'venta'")
                        else:
                            self._detected_sheet = "unknown"
                            self.log("WARN", "Could not detect category (alquiler/venta)")
            
                    # ===== SINGLE-PASS SCRAPING: Page by page, scrape immediately =====
                    self.log("INFO", "Starting page-by-page scraping...")
            
                    # Override target_file if category detection differs from registry
                    # This ensures 'alquiler-habitaciones' URLs get the correct filename
                    if self._detected_sheet and target_file:
                        expected_category = self._detected_sheet
                        # Check if registered file uses a different category
                        if f"_{expected_category}." not in target_file:
                            # Rebuild target_file with correct category
                            # Note: At this point, no property scraped yet, so use H1-detected city
                            # The filename may be updated after first property if Ciudad differs
                            ciudad = self._detected_city
                            if ciudad:
                                ciudad_clean = sanitize_filename_part(ciudad)
                                new_target_file = f"idealista_{ciudad_clean}_{expected_category}.xlsx"
                            else:
                                new_target_file = f"idealista_{expected_category}.xlsx"
                            self.log("INFO", f"Updating target file: {target_file} -> {new_target_file}")
                            target_file = new_target_file
                            # Reset url_dates since we're using a different file
                            url_dates = {}
                            preloaded_urls = set()
                            self._processed.clear()
            
                    # target_file and url_dates already set from registry lookup above (or overridden)
            
                    # Extract starting page from seed URL (e.g., /pagina-5 starts at page 5)
                    page_num = extract_page_from_url(self.seed_url)
                    if page_num > 1:
                        self.log("INFO", f"Detected starting page from URL: {page_num}")
            
                    # Calculate starting property index based on page number
                    # Page 1 starts at property 1, page 2 at 31, etc. (30 properties per page)
                    property_idx = (page_num - 1) * 30
                    self.current_property_count = property_idx
                    self.emit_progress()
                    skipped = 0
                    updated = 0
                    new_scraped = 0
                    existing_df = None  # Will be loaded by checkpoint if needed
                    scraping_finished = False  # Track clean completion
            
                    while not self._stop_evt.is_set():
                        await self._wait_for_pause()
                        if self._stop_evt.is_set():
                            break

                        self.current_page = page_num
                        list_url = build_paginated_url(self.seed_url, page_num)
                        self.log("INFO", f"Opening listing page {page_num}/{self.total_pages_expected}: {list_url}")
                
                        try:
                            await self._goto_with_retry(page, list_url)
                    
                            # Verify we are on the correct page (Idealista might redirect to previous page if blocked or bugged)
                            # Check if 'pagina-X' is in the URL if we expect it
                            current_url = page.url
                            expected_page_part = f"pagina-{page_num}"
                            if expected_page_part in list_url and expected_page_part not in current_url:
                                self.log("WARN", f"Navigation check failed: Requested page {page_num} but URL is {current_url}")
                                self.log("WARN", "Forcing reload and short wait...")
                                await page.reload(wait_until="domcontentloaded")
                                await self._interruptible_sleep(3.0)
                        
                        except BrowserClosedException:
                            break
                        except Exception as e:
                            self.log("ERR", f"Failed to open listing page: {e}")
                            break
                
                        # Wait for content and scroll
                        try:
                            await page.wait_for_selector("article, .item, [data-element-id]", timeout=10000, state="visible")
                            await self._interruptible_sleep(2.0)
                        except Exception:
                            pass
                
                        # Use variable scroll for Extra Stealth, standard for others
                        await self.variable_scroll(page)
                        await asyncio.sleep(1.0)
                
                        # Collect property links from this page
                        js_collect = r'''(() => {
                            const A = [...document.querySelectorAll("a[href*='/inmueble']")];
                            const U = A.map(a => new URL(a.getAttribute("href") || a.href, location.origin).href)
                                      .filter(u => /\/inmueble[s]?\/\d+/.test(u));
                            return [...new Set(U)].slice(0, %d);
                        })()''' % LISTING_LINKS_PER_PAGE_MAX
                
                        try:
                            hrefs: List[str] = await page.evaluate(js_collect)
                        except Exception as e:
                            self.log("WARN", f"Error collecting links: {e}")
                            hrefs = []
                
                        if not hrefs:
                            # DEBUG: Why are there no links? Let's investigate
                            self.log("WARN", f"No property links found on page {page_num}!")
                    
                            # Check if we've scraped at least expected properties
                            if len(self.scraped_properties) < self.total_properties_expected * 0.9:
                                # We haven't scraped enough - something is wrong
                                self.log("WARN", f"Only scraped {len(self.scraped_properties)}/{self.total_properties_expected} - investigating...")
                        
                                # Get page info for debugging
                                try:
                                    # =============================================================================
                                    # LOOP PRIORITY BLOCK DETECTION
                                    # =============================================================================
                                    # Check for blocks on each new page before parsing
                                    try:
                                        current_url = page.url
                                        page_content = await page.content() # Get full HTML content
                                        content_lower = page_content.lower()
                                        
                                        if "el acceso se ha bloqueado" in content_lower or "uso indebido" in content_lower or "access denied" in content_lower:
                                            self.log("ERR", f"🚫 BLOCK DETECTED on page {page_num}: 'Uso indebido/Bloqueado'.")
                                            # Raise BlockedException to trigger rotation
                                            raise BlockedException("Loop block detection: uso indebido")
                                            
                                        if "estamos recibiendo muchas peticiones tuyas" in content_lower:
                                             self.log("WARN", f"⚠️ CAPTCHA/LIMIT DETECTED on page {page_num}.")
                                             raise BlockedException("Loop block detection: Rate Limit/CAPTCHA")

                                    except BlockedException:
                                        raise # Let the handler deal with it
                                    except Exception:
                                        pass # Continue if check fails (e.g. page closed)
                                        
                                    # =============================================================================

                                    # 4. Parse content
                                    html_content = await page.content()
                                    page_title = await page.title()
                                    page_url = page.url
                                    self.log("INFO", f"Current URL: {page_url}")
                                    self.log("INFO", f"Page title: {page_title}")
                            
                                    # Check for CAPTCHA indicators
                                    captcha_check = await page.evaluate(r"""() => {
                                        const body = (document.body && document.body.innerText) ? document.body.innerText.toLowerCase() : '';
                                        const hasCaptcha = body.includes('captcha') || 
                                                           body.includes('robot') || 
                                                           body.includes('verificar') ||
                                                           body.includes('security check');
                                        const linkCount = document.querySelectorAll('a').length;
                                        const articleCount = document.querySelectorAll('article, .item, .item-link').length;
                                        return { hasCaptcha, linkCount, articleCount };
                                    }""")
                            
                                    self.log("INFO", f"CAPTCHA detected: {captcha_check.get('hasCaptcha', False)}")
                                    self.log("INFO", f"Total links on page: {captcha_check.get('linkCount', 0)}")
                                    self.log("INFO", f"Article elements: {captcha_check.get('articleCount', 0)}")
                            
                                    if captcha_check.get('hasCaptcha', False):
                                        self.log("WARN", "CAPTCHA page detected! Keeping browser open for 60s for manual solving...")
                                        self.log("DEBUG_TIMING", "Starting 60s CAPTCHA wait.")
                                        await asyncio.sleep(60)  # Give user time to solve CAPTCHA
                                        self.log("DEBUG_TIMING", "Finished 60s CAPTCHA wait.")
                                        # Try collecting links again after waiting
                                        hrefs = await page.evaluate(js_collect)
                                        if hrefs:
                                            self.log("OK", f"After waiting, found {len(hrefs)} links!")
                                    else:
                                        self.log("WARN", "No CAPTCHA, but no links either. Keeping browser open 30s for inspection...")
                                        self.log("DEBUG_TIMING", "Starting 30s manual inspection wait.")
                                        await asyncio.sleep(30)  # Keep open for inspection
                                        self.log("DEBUG_TIMING", "Finished 30s manual inspection wait.")
                                
                                except BlockedException as be:
                                    self.log("ERR", f"🛑 HARD STOP: {be}")
                                    self._stop_evt.set()
                                    # Signal that we should NOT dual-mode continue
                                    self.dual_mode_url = None 
                                    raise be # Re-raise to be caught by outer loop
                                
                                except Exception as debug_e:
                                    self.log("ERR", f"Debug check failed: {debug_e}")
                    
                            # Still no links after debug - exit
                            if not hrefs:
                                self.log("INFO", f"End of listings at page {page_num}.")
                                break
                
                        self.log("INFO", f"Page {page_num}: Found {len(hrefs)} properties to check")
                
                        # === VERBOSE SKIP: Check each URL individually to log skips ===
                        original_count = len(hrefs)
                        # We do NOT filter hrefs here anymore, we iterate all to log skips
                
                        # If ALL URLs on this page are already in our set, skip to next page
                        if not hrefs:
                            self.log("OK", f"Page {page_num}: All {original_count} properties already scraped → skipping to next page")
                            skipped += original_count
                            property_idx += original_count  # Update counter for UI
                            self.current_property_count = property_idx
                            self.emit_progress()
                            page_num += 1
                            self.current_page = page_num
                            continue
                
                        skipped_on_page = 0
                
                        # Update page progress
                        self.current_page = page_num
                        self.emit_progress()
                
                        # Scrape each property on this page (only NEW ones now)
                        for href in hrefs:
                            # Update delays on every iteration to respect dynamic mode switching
                            _, card_delay, post_card_delay = self.get_delays()
                    
                            await self._wait_for_pause()
                            if self._stop_evt.is_set():
                                break
                    
                            property_idx += 1
                            key = canonical_listing_url(href)
                    
                            # Double-check (should not happen after filtering, but safety net)
                            if key in self._processed:
                                self.log("INFO", f"({property_idx}/{self.total_properties_expected}) Skipping already scraped: {key}")
                                skipped_on_page += 1
                                skipped += 1
                                self.current_property_count = property_idx
                                self.emit_progress()
                                continue
                    
                            try:
                                t_pre = time.time()
                                await self._interruptible_sleep(random.uniform(*card_delay))
                                self.log("DEBUG_TIMING", f"Pre-card sleep took {time.time() - t_pre:.2f}s")

                                t_goto = time.time()
                                await self._goto_with_retry(page, href)
                                t_goto_end = time.time()
                                self.log("DEBUG_TIMING", f"Navigation to {key} took {t_goto_end - t_goto:.2f}s")

                                t_post = time.time()
                                await self._interruptible_sleep(random.uniform(*post_card_delay))
                                self.log("DEBUG_TIMING", f"Post-card sleep took {time.time() - t_post:.2f}s")
                        
                                # If this is the first property, determine target file
                                if target_file is None:
                                    await page.wait_for_timeout(PAGE_WAIT_MS)
                                    d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                                    row = {"URL": key, **d}
                            
                                    # Build target filename: idealista_[Ciudad]_[venta/alquiler].xlsx
                                    # Prioritize city from first scraped property's Ciudad field
                                    ciudad = row.get("Ciudad") or self._detected_city
                                    category = self._detected_sheet or "unknown"
                            
                                    if ciudad:
                                        ciudad_clean = sanitize_filename_part(ciudad)
                                        target_file = f"idealista_{ciudad_clean}_{category}.xlsx"
                                    else:
                                        target_file = f"idealista_{category}.xlsx"
                            
                                    target_path = os.path.join(self.output_dir, target_file)
                                    self.log("INFO", f"Target Excel file: {target_path}")
                            
                                    # Load existing URLs from this file
                                    # import time  <-- REMOVED to fix UnboundLocalError
                                    t_start_load = time.time()
                                    url_dates = load_urls_with_dates(target_path)
                                    t_end_load = time.time()
                                    self.log("INFO", f"Loaded {len(url_dates)} existing URLs from file in {t_end_load - t_start_load:.2f}s")
                            
                                    # CRITICAL FIX: Add existing URLs to processed set immediately
                                    # This prevents re-scraping subsequent properties in this loop that are already in the file
                                    if url_dates:
                                        self._processed.update(url_dates.keys())
                            
                                    # Process first property - check for missing fields (CAPTCHA)
                                    miss = missing_fields(row, is_room_mode=self._is_room_mode)
                                    if miss:
                                        self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA detectado en primera propiedad. Esperando 30s...")
                                
                                        if self.on_status:
                                            self.on_status("captcha")
                                
                                        # CAPTCHA DETECTED - AUTO RESTART STRATEGY
                                        self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA DETECTED - Waiting 30s then aborting for auto-restart...")
                                        
                                        # Wait briefly to see if it clears (e.g. passive solve)
                                        for i in range(3): # 3 * 10s = 30s
                                            if self._stop_evt.is_set(): break
                                            self.log("DEBUG_TIMING", f"Starting 10s CAPTCHA check wait (Attempt {i+1}/3).")
                                            await asyncio.sleep(10.0)
                                            self.log("DEBUG_TIMING", f"Finished 10s CAPTCHA check wait.")
                                            # Retry extraction check
                                            try:
                                                d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                                                row = {"URL": key, **d}
                                                if not missing_fields(row, is_room_mode=self._is_room_mode):
                                                     self.log("OK", "CAPTCHA cleared! Resuming...")
                                                     miss = False
                                                     break 
                                            except: pass

                                        if miss:
                                            self.log("ERR", "CAPTCHA_BLOCK_DETECTED")
                                            # Mark profile as blocked for cooldown rotation
                                            mark_profile_blocked(self.browser_engine)
                                            self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                                            try:
                                                 if len(additions) > self._last_checkpoint_idx and target_file:
                                                      t_start_save = time.time()
                                                      await self._save_checkpoint(additions, target_file, existing_df, set())
                                                      self.log("INFO", f"Saved captcha checkpoint in {time.time() - t_start_save:.2f}s")
                                            except: pass
                                            raise Exception("CAPTCHA_BLOCK_DETECTED")
                                        
                                        # CAPTCHA cleared - resume normal operation
                                        if self.on_status: self.on_status("running")
                                    
                                        if self._stop_evt.is_set():
                                            self.log("WARN", f"First property CAPTCHA - stopped by user: {key}")
                                            continue
                            
                                    # First property scraped successfully (or CAPTCHA cleared)
                                    # Add scraping date
                                    from datetime import datetime
                                    row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                    
                                    # Smart Enrichment: Mark as enriched with current date
                                    if self.smart_enrichment:
                                        row = mark_as_enriched(row)
                                
                                    additions.append(row)
                                    self.scraped_properties.append(row)
                                    new_scraped += 1
                                    self.log("OK", f"({property_idx}/{self.total_properties_expected}) Scraped: {key}")
                                    if self.on_property:
                                        self.on_property(row)
                            
                                    self._processed.add(key)
                                    self.current_property_count = property_idx
                                    self.emit_progress()
                                    continue
                        
                                # URLs reaching here are NEW - not in _processed (filtered above)
                                new_scraped += 1
                        
                                # Scrape the property
                                await page.wait_for_timeout(PAGE_WAIT_MS)
                                d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                        
                                row = {"URL": key, **d}
                                miss = missing_fields(row, is_room_mode=self._is_room_mode)
                        
                                # Check if this is a "listing not found" page (not a CAPTCHA)
                                if miss:
                                    page_text = await page.evaluate("() => (document.body && document.body.innerText) ? document.body.innerText : ''")
                                    is_not_found = (
                                        "no encontramos" in page_text.lower() or
                                        "anuncio no disponible" in page_text.lower() or
                                        "este anuncio ya no está disponible" in page_text.lower() or
                                        "enlace antiguo" in page_text.lower() or
                                        "anuncio ya no está publicado" in page_text.lower() or
                                        "lo sentimos" in page_text.lower()
                                    )
                            
                                    if is_not_found:
                                        # Listing is unavailable - skip without pausing for CAPTCHA
                                        self.log("WARN", f"({property_idx}/{self.total_properties_expected}) Anuncio no disponible: {key}")
                                        self._processed.add(key)
                                        self.current_property_count = property_idx
                                        self.emit_progress()
                                        self.emit_progress()
                                        continue
                        
                                # Check for BLOCK (uso indebido) inside loop before CAPTCHA
                                page_text_lower = page_text.lower() if 'page_text' in locals() else (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower()
                                if "uso indebido" in page_text_lower or "se ha bloqueado" in page_text_lower or "uso no autorizado" in page_text_lower:
                                    self.log("ERR", "🚫 Loop detection: 'Uso indebido' detected. Triggering auto-restart...")
                                    # Mark profile as blocked for cooldown rotation
                                    mark_profile_blocked(self.browser_engine)
                                    self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                                    raise BlockedException("Acceso bloqueado por uso indebido detected in loop")

                                # If missing fields and not a "not found" page, might be CAPTCHA
                                if miss:
                                    self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA detectado. Resuelve el CAPTCHA y pulsa Resume.")
                            
                                    if self.on_status:
                                        self.on_status("captcha")
                            
                                    # CAPTCHA DETECTED - AUTO RESTART STRATEGY
                                    self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA DETECTED - Waiting 30s then aborting for auto-restart...")
                                    
                                    # Wait briefly to see if it clears (e.g. passive solve)
                                    for _ in range(3): # 3 * 10s = 30s
                                        if self._stop_evt.is_set(): break
                                        await asyncio.sleep(10.0)
                                        # Retry extraction check
                                        try:
                                            d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                                            row = {"URL": key, **d}
                                            if not missing_fields(row, is_room_mode=self._is_room_mode):
                                                 self.log("OK", "CAPTCHA cleared! Resuming...")
                                                 miss = False
                                                 break 
                                        except: pass

                                    if miss:
                                        self.log("ERR", "CAPTCHA_BLOCK_DETECTED")
                                        # Mark profile as blocked for cooldown rotation
                                        mark_profile_blocked(self.browser_engine)
                                        self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                                        try:
                                             if len(additions) > self._last_checkpoint_idx and target_file:
                                                  t_start_save = time.time()
                                                  await self._save_checkpoint(additions, target_file, existing_df, set())
                                                  self.log("INFO", f"Saved captcha checkpoint in {time.time() - t_start_save:.2f}s")
                                        except: pass
                                        raise Exception("CAPTCHA_BLOCK_DETECTED")
                                    
                                    # If cleared, proceed (miss is False)
                                    if not miss:
                                         if self.on_status: self.on_status("running")
                                
                                # Add scraping date in dd/mm/yyyy format
                                from datetime import datetime
                                row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                
                                # Smart Enrichment: Mark as enriched with current date
                                if self.smart_enrichment:
                                    row = mark_as_enriched(row)
                        
                                additions.append(row)
                                self.scraped_properties.append(row)
                                self._processed.add(key)
                        
                                # Checkpoint saving: save every 100 properties
                                if len(additions) > 0 and len(additions) % self._checkpoint_interval == 0:
                                    t_start_save = time.time()
                                    await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                                    self.log("INFO", f"Saved periodic checkpoint in {time.time() - t_start_save:.2f}s")
                        
                                # Extra Stealth: Simulate reading time
                                t_read = time.time()
                                await self.simulate_reading_time(row.get("Descripción"))
                                t_read_end = time.time()
                                if t_read_end - t_read > 2.0:
                                    self.log("DEBUG_TIMING", f"Reading simulation took {t_read_end - t_read:.2f}s")
                                
                                if self._stop_evt.is_set():
                                    break

                                # Extra Stealth: Mouse movement simulation
                                t_mouse = time.time()
                                await self.simulate_mouse_movement(page)
                                t_mouse_end = time.time()
                                self.log("DEBUG_TIMING", f"Mouse movements took {t_mouse_end - t_mouse:.2f}s")
                                if self._stop_evt.is_set():
                                    break
                        
                                # Extra Stealth: Increment session counter and check for breaks
                                if self.mode == "stealth":
                                    self._session_property_count += 1
                                    await self.maybe_coffee_break()
                                    if self._stop_evt.is_set():
                                        break
                                    await self.maybe_session_rest()
                                    if self._stop_evt.is_set():
                                        break
                        
                                # Always log successful scrape, even if it's an update
                                self.log("OK", f"({property_idx}/{self.total_properties_expected}) Scraped: {key}")
                        
                                if self.on_property:
                                    self.on_property(row)
                        
                                self.current_property_count = property_idx
                                self.current_property_count = property_idx
                                self.emit_progress()
                                
                                # Loop heartbeat - kept silent unless debug is needed
                                # self.log("DEBUG", f"Finished loop for {property_idx}")
                        
                            except BrowserClosedException:
                                # Save state for resume before exiting
                                self.save_state(page_num, target_file)
                                break
                            except Exception as e:
                                if str(e) == "CAPTCHA_BLOCK_DETECTED":
                                    # Save state for resume before failing
                                    self.log("WARN", "Saving resume state due to CAPTCHA block")
                                    self.save_state(page_num, target_file)
                                    raise e
                                self.log("ERR", f"({property_idx}/{self.total_properties_expected}) {key} -> {e}")
                                self._processed.add(key)
                
                
                        # Check if we should continue to next page
                        if self._stop_evt.is_set():
                            # Save state for resume before stopping
                            self.save_state(page_num, target_file)
                            break
                    
                        # Case 1: Less links than max = Last Page
                        if original_count < LISTING_LINKS_PER_PAGE_MAX:
                            self.log("INFO", f"Last page reached (found {original_count} links < {LISTING_LINKS_PER_PAGE_MAX}).")
                            self.clear_state()
                            scraping_finished = True
                            break

                        # Case 2: All properties on this page were skipped
                        if len(hrefs) > 0 and skipped_on_page == len(hrefs):
                            self.log("WARN", f"Página {page_num}: todas las propiedades ya existen en el fichero")
                            # Wait a bit longer to let things settle
                            await asyncio.sleep(2.0)
                            # Explicitly advance to next page
                            page_num += 1
                            self._pages_scraped += 1
                            continue

                        # Case 3: Exceeded expected pages based on H1 count
                        if self.total_pages_expected > 0 and page_num >= self.total_pages_expected:
                            self.log("INFO", f"Reached expected page limit ({self.total_pages_expected} pages). Finishing scrape.")
                            self.clear_state()
                            scraping_finished = True
                            break

                        # Case 4: Max pages reached (hard limit to avoid infinite loops)
                        if page_num >= 60:
                            self.log("INFO", f"Reached page {page_num} (maximum listing pages). Finishing scrape.")
                            self.clear_state()
                            scraping_finished = True
                            break

                        # Default: Next page
                        page_num += 1
                        self._pages_scraped += 1
                
                    # After phase 1 loop completes successfully
                    self.log("INFO", f"Summary: {new_scraped} new, {updated} updated, {skipped} skipped, {len(expired_urls)} expired")
                    self._export_to_excel(additions, target_file, expired_urls)

                    # CRITICAL FIX: If we finished cleanly (last page or max page), STOP the outer recovery loop
                    if scraping_finished and not self._stop_evt.is_set():
                        self.log("INFO", "Scraping completed successfully. Exiting.")
                        break
                
                    # === DUAL MODE: Run second phase in same browser ===
                    # === DUAL MODE: Run second phase in same browser ===
                    if self.dual_mode_url and not self._stop_evt.is_set():
                        self.log("INFO", "=== DUAL MODE: Starting second phase in same browser ===")
                        self.log("INFO", f"Switching to: {self.dual_mode_url}")
                    
                        # Cooldown period to appear more human-like
                        cooldown = random.randint(30, 60)
                        self.log("INFO", f"Cooldown pause: {cooldown} seconds before continuing...")
                    
                        for _ in range(cooldown):
                            if self._stop_evt.is_set():
                                break
                            await asyncio.sleep(1)
                    
                        # Reset state for second phase
                        self.seed_url = self.dual_mode_url
                        self.dual_mode_url = None  # Prevent infinite loop
                        self._processed.clear()
                        self._detected_sheet = None
                        self._detected_city = None
                        self.scraped_properties = []
                        self.current_page = 0
                        self.current_property_count = 0
                        additions = []
                        expired_urls = []
                    
                        # Detect new category
                        self._is_room_mode = "habitacion" in self.seed_url.lower()
                    
                        # Navigate to new seed URL
                        try:
                            self.log("INFO", f"Navigating to second seed URL...")
                            await page.goto(self.seed_url, wait_until="domcontentloaded", timeout=60000)
                            await asyncio.sleep(3.0)
                            self.log("OK", "Opened second seed URL")
                        except Exception as e:
                            self.log("ERR", f"Could not open second seed URL: {e}")
                            if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                                mouse_jitter_task.cancel()
                            await (browser.close() if browser else ctx.close())
                            self.log("INFO", "✅ Browser closed successfully.")
                            self._stop_evt.set()
                            break
                    
                        # Re-detect properties count and category for phase 2
                        h1txt = ""
                        total_count = 0
                    
                        for attempt in range(4):
                            try:
                                await asyncio.sleep(4.0)
                                try:
                                    await page.wait_for_load_state("networkidle", timeout=5000)
                                except Exception:
                                    pass
                            
                                page_title = await page.title()
                                if attempt == 0:
                                    self.log("INFO", f"Page title: '{page_title[:80] if page_title else 'empty'}'")
                            
                                h1txt = await page.evaluate(r"""() => {
                                    const el = document.querySelector('#h1-container__text') || 
                                               document.querySelector('#h1-container') || 
                                               document.querySelector('h1');
                                    return el ? el.textContent.trim() : '';
                                }""") or ""
                            
                                if h1txt:
                                    self.log("INFO", f"H1 text: '{h1txt[:100]}'")
                                    match = re.search(r'(\d{1,3}(?:\.\d{3})*)\s*(?:vivienda|pisos?|casas?|inmuebles?|anuncios?|habitaci[oó]n|habitaciones)', h1txt, re.IGNORECASE)
                                    if match:
                                        total_count = int(match.group(1).replace('.', ''))
                                        self.log("INFO", f"Extracted count from h1: {total_count}")
                                        break
                            except Exception as e:
                                if attempt < 3:
                                    self.log("WARN", f"H1 extraction attempt {attempt+1} failed: {e}. Retrying...")
                    
                        if total_count == 0:
                            self.log("ERR", "Could not detect properties on second URL. Skipping phase 2.")
                            # We don't return here, we break to finish cleanly
                            break
                    
                        # Detect category for phase 2
                        h1_lower = h1txt.lower()
                        if "habitaci" in h1_lower and "alquiler" in h1_lower:
                            self._detected_sheet = "alquiler-habitaciones"
                        elif "alquiler" in h1_lower:
                            self._detected_sheet = "alquiler"
                        elif "venta" in h1_lower:
                            self._detected_sheet = "venta"
                    
                        if "," in h1txt:
                            city_part = h1txt.split(",")[-1].strip()
                            city_part = re.sub(r'\s+(capital|provincia|centro|norte|sur|este|oeste).*$', '', city_part, flags=re.IGNORECASE).strip()
                            if city_part:
                                self._detected_city = city_part
                    
                        self.total_properties_expected = total_count
                        self.total_pages_expected = (total_count + 29) // 30 if total_count > 0 else 0
                        self.log("INFO", f"Phase 2: {self.total_properties_expected} properties, {self.total_pages_expected} pages")
                        self.emit_progress()
                    
                        # Determine target file for phase 2
                        target_file = None
                        registry_entry = lookup_seed_url(self.seed_url)
                        if registry_entry:
                            target_file = registry_entry.get("output_file")
                            target_path = os.path.join(self.output_dir, target_file) if target_file else None
                            if target_path and os.path.exists(target_path):
                                url_dates = load_urls_with_dates(target_path)
                                self._processed.update(url_dates.keys())
                                self.log("INFO", f"Pre-loaded {len(url_dates)} existing URLs for phase 2")
                    
                        # Re-run the main scraping loop for phase 2
                        page_num = 1
                        new_scraped = 0
                        updated = 0
                        skipped = 0
                    
                        while not self._stop_evt.is_set():
                            await self._wait_for_pause()
                            if self._stop_evt.is_set():
                                break
                        
                            list_url = build_paginated_url(self.seed_url, page_num)
                            self.log("INFO", f"Opening listing page {page_num}/{self.total_pages_expected}: {list_url}")
                        
                            try:
                                await self._goto_with_retry(page, list_url)
                            except BrowserClosedException:
                                break
                            except Exception as e:
                                self.log("ERR", f"Failed to open listing page: {e}")
                                break
                        
                            try:
                                await page.wait_for_selector("article, .item, [data-element-id]", timeout=10000, state="visible")
                                await asyncio.sleep(2.0)
                            except Exception:
                                pass
                        
                            await self.variable_scroll(page)
                            await asyncio.sleep(1.0)
                        
                            js_collect = r'''(() => {
                                const A = [...document.querySelectorAll("a[href*='/inmueble']")];
                                const U = A.map(a => new URL(a.getAttribute("href") || a.href, location.origin).href)
                                          .filter(u => /\/inmueble[s]?\/\d+/.test(u));
                                return [...new Set(U)].slice(0, %d);
                            })()''' % LISTING_LINKS_PER_PAGE_MAX
                        
                            try:
                                hrefs: List[str] = await page.evaluate(js_collect)
                            except Exception as e:
                                self.log("WARN", f"Error collecting links: {e}")
                                hrefs = []
                        
                            if not hrefs:
                                self.log("INFO", f"End of listings at page {page_num}.")
                                break
                        
                            original_count = len(hrefs)
                            hrefs = [h for h in hrefs if canonical_listing_url(h) not in self._processed]
                        
                            if not hrefs:
                                self.log("OK", f"Page {page_num}: All {original_count} properties already scraped")
                                page_num += 1
                                continue
                        
                            self.log("INFO", f"Page {page_num}: {len(hrefs)} new properties to scrape")
                            self.current_page = page_num
                            self.emit_progress()
                        
                            for href in hrefs:
                                await self._wait_for_pause()
                                if self._stop_evt.is_set():
                                    break
                            
                                key = canonical_listing_url(href)
                                if key in self._processed:
                                    continue
                            
                                try:
                                    await asyncio.sleep(random.uniform(*card_delay))
                                    await self._goto_with_retry(page, href)
                                    await asyncio.sleep(random.uniform(*post_card_delay))
                                
                                    await page.wait_for_timeout(PAGE_WAIT_MS)
                                    d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                                    row = {"URL": key, **d}
                                
                                    miss = missing_fields(row, is_room_mode=self._is_room_mode)
                                    if miss:
                                        page_text = await page.evaluate("() => (document.body && document.body.innerText) ? document.body.innerText : ''")
                                        is_not_found = any(x in page_text.lower() for x in ["no encontramos", "anuncio no disponible", "este anuncio ya no está disponible", "anuncio ya no está publicado", "lo sentimos"])
                                        if is_not_found:
                                            self.log("WARN", f"Anuncio no disponible: {key}")
                                            self._processed.add(key)
                                            continue
                                
                                    # Check for BLOCK (uso indebido) inside loop before CAPTCHA
                                    page_text_lower = page_text.lower() if 'page_text' in locals() else (await page.evaluate("() => document.body ? document.body.innerText : ''")).lower()
                                    if "uso indebido" in page_text_lower or "se ha bloqueado" in page_text_lower or "uso no autorizado" in page_text_lower:
                                        self.log("ERR", "🚫 Loop detection: 'Uso indebido' detected. Triggering auto-restart...")
                                        # Mark profile as blocked for cooldown rotation
                                        mark_profile_blocked(self.browser_engine)
                                        self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                                        raise BlockedException("Acceso bloqueado por uso indebido detected in loop")
                                
                                    row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                    additions.append(row)
                                    self.scraped_properties.append(row)
                                    self._processed.add(key)
                                    new_scraped += 1
                                
                                    self.current_property_count += 1
                                    self.emit_progress()
                                    self.log("OK", f"({self.current_property_count}/{self.total_properties_expected}) Scraped: {key}")
                                
                                    if self.on_property:
                                        self.on_property(row)
                                
                                    t_start_read = time.time()
                                    await self.simulate_reading_time(row.get("Descripción"))
                                    self.log("INFO", f"Simulated reading time: {time.time() - t_start_read:.2f}s")
                                    await self.simulate_mouse_movement(page)
                                
                                except BrowserClosedException:
                                    break
                                except Exception as e:
                                    self.log("ERR", f"({self.current_property_count}/{self.total_properties_expected}) {key} -> {e}")
                                    self._processed.add(key)
                        
                            if self._stop_evt.is_set():
                                break
                            if original_count < LISTING_LINKS_PER_PAGE_MAX:
                                self.log("INFO", f"Last page reached.")
                                break
                            if page_num >= 60:
                                self.log("INFO", f"Reached page limit (60).")
                                break
                            page_num += 1
                    
                        self.log("INFO", f"Phase 2 Summary: {new_scraped} new properties")
                        self._export_to_excel(additions, target_file, expired_urls)
                    
                        # Successfully finished phase 2
                        break
            
            except BlockedException:
                self.log("ERR", "🛑 HARD STOP: Scraper blocked by Idealista (Uso Indebido).")
                self.save_state(self.current_page or 1, target_file)
                self.log("WARN", "Resume state saved for later retry.")
                self.handle_blocked_profile()
                
                # Mark this engine's profile as blocked (15-minute cooldown)
                current_engine = self.browser_engine
                mark_profile_blocked(current_engine)
                self.log("WARN", f"⏳ Profile '{current_engine}' marked as blocked. Cooldown: {PROFILE_COOLDOWN_MINUTES} min.")
                
                # ROTATION LOGIC: Try to switch engine immediately
                next_engine = select_next_engine(current_engine)
                
                if next_engine and next_engine != current_engine:
                    self.log("INFO", f"🔄 ROTATION: Switching to fresh engine '{next_engine.upper()}' for immediate restart.")
                    self.browser_engine = next_engine
                    wait_time = random.randint(5, 15)  # Short pause before switch
                else:
                    # All engines blocked - wait for cooldown
                    cooldown = get_cooldown_remaining(current_engine)
                    wait_time = max(cooldown * 60, random.randint(60, 180))
                    self.log("WARN", f"⚠️ All profiles blocked. Waiting {wait_time}s for cooldown...")

                self.log("WARN", f"🔄 Initiating Auto-Restart sequence in {wait_time} seconds...")
                
                if self.on_status:
                    self.on_status("error", error=f"Bloqueado. Reiniciando en {wait_time}s...")
                
                # Close browser explicitly
                try:
                    if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                        mouse_jitter_task.cancel()
                    if browser:
                        await browser.close()
                    elif ctx:
                        await ctx.close()
                except:
                    pass
                
                # === VPN IP Rotation on Block ===
                if self.use_vpn:
                    self.log("INFO", "🌐 VPN: Rotating IP after block detection...")
                    try:
                        from idealista_scraper.nordvpn import rotate_ip
                        rotate_ip()
                        self.log("OK", "🌐 VPN: IP rotated successfully.")
                    except Exception as vpn_err:
                        self.log("WARN", f"VPN: IP rotation failed: {vpn_err}. Continuing with cooldown...")
                
                # Wait cooldown
                await self._interruptible_sleep(wait_time)
                
                if self._stop_evt.is_set():
                    break
                    
                self.log("INFO", "🔄 Restarting browser now...")
                continue # Loop back to start (and reuse persistent profile handling which will be fresh)

            except Exception as e:
                # Catch generic CAPTCHA blocks raised mid-scrape
                err_str = str(e).upper()
                if "CAPTCHA" in err_str:
                    self.log("WARN", "⚠️ Se ha detectado un bloqueo por CAPTCHA durante el scraping.")
                    self.log("WARN", "Guardando estado y esperando 15 minutos antes de reintentar...")
                    
                    # Mark this engine's profile as blocked (15-minute cooldown)
                    engine = self.browser_engine
                    mark_profile_blocked(engine)
                    self.log("WARN", f"⏳ Profile '{engine}' marked as blocked. Cooldown: {PROFILE_COOLDOWN_MINUTES} min.")
                    
                    # ROTATION LOGIC: Try to switch engine immediately
                    next_engine = select_next_engine(engine)
                    if next_engine and next_engine != engine:
                         self.log("INFO", f"🔄 ROTATION: Switching to fresh engine '{next_engine.upper()}' for immediate restart.")
                         self.browser_engine = next_engine
                         wait_time = random.randint(5, 15)
                    else:
                         cooldown = get_cooldown_remaining(engine)
                         wait_time = max(cooldown * 60, random.randint(60, 180))
                         self.log("WARN", f"⚠️ All profiles blocked. Waiting {wait_time}s for cooldown...")

                    self.log("WARN", f"🔄 Initiating Auto-Restart sequence in {wait_time} seconds...")
                    if self.on_status:
                        self.on_status("blocked", error=f"CAPTCHA block. Rotating in {wait_time}s...")
                    
                    # Close browser explicitly
                    try:
                        if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                            mouse_jitter_task.cancel()
                        if browser:
                            await browser.close()
                        elif ctx:
                            await ctx.close()
                    except:
                        pass
                    
                    if target_file and self.current_page:
                         self.save_state(self.current_page, target_file)

                    # === VPN IP Rotation on CAPTCHA ===
                    if self.use_vpn:
                        self.log("INFO", "🌐 VPN: Rotating IP after CAPTCHA detection...")
                        try:
                            from idealista_scraper.nordvpn import rotate_ip
                            rotate_ip()
                            self.log("OK", "🌐 VPN: IP rotated successfully.")
                        except Exception as vpn_err:
                            self.log("WARN", f"VPN: IP rotation failed: {vpn_err}. Continuing with wait...")

                    if self.on_status:
                         self.on_status("blocked", message="Esperando 15 minutos para reintentar...")

                    self.log("OK", f"✅ Browser closed. Waiting {wait_time} seconds before restart...")

                    # Wait for calculated duration (short for switch, long for cooldown)
                    cycles = max(1, int(wait_time / 5))
                    for _ in range(cycles): 
                         if self._stop_evt.is_set(): break
                         await asyncio.sleep(5)
                    
                    if self._stop_evt.is_set():
                         self.log("INFO", "Retry cancelled by user.")
                         break
                    
                    self.log("OK", "🔄 Reintentando ahora...")
                    continue

                # Re-raise other unexpected errors
                raise e
        
                # Reset self.is_running = False etc will happen at the very end of run()
            
            self.log("INFO", "Scraping finished.")
            # Close browser/context properly based on mode (if not already closed)
            try:
                if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                    mouse_jitter_task.cancel()
                if browser is not None:
                    await browser.close()
                elif ctx is not None:
                    await ctx.close()  # Persistent context in Stealth mode
                self._context = None  # Clear reference
                self._browser = None
                self.log("OK", "✅ Browser closed successfully.")
            except:
                # Browser might be already closed
                self._context = None
                self._browser = None
                pass
        
        # Clear resume state file ONLY on successful completion (not manual stop)
        if not self._stop_evt.is_set():
            self.clear_state()
            self.status = "completed"
            self.log("INFO", "Resume state cleared (scraping completed successfully)")
        elif self._stopped_by_user:
            self.status = "stopped"
            self.log("INFO", "Scraper stopped by user. State preserved for resume.")
        else:
            # Automatic stop (error, blocked, etc.)
            # Preserve existing status if it's already an error/block/captcha
            if self.status not in ["error", "blocked", "captcha"]:
                self.status = "error"
        
        self.is_running = False
        
        if self.on_status:
            self.on_status(self.status, file=self.output_file, count=len(self.scraped_properties))

