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
# import pandas as pd # MOVED TO _import_libs
import random
import re
import sys
import time
import threading
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

# from playwright.async_api import async_playwright # MOVED TO _import_libs

# Optional playwright-stealth for enhanced anti-detection (Stealth mode only)
# try:
#     from playwright_stealth import stealth_async
#     HAS_STEALTH = True
# except ImportError:
#     HAS_STEALTH = False
#     stealth_async = None

# Placeholders for lazy-loaded libraries
pd = None
async_playwright = None
stealth_async = None
HAS_STEALTH = False
 
def _lazy_import():
    global pd, async_playwright, stealth_async, HAS_STEALTH
    if pd is None:
        import pandas as _pd
        pd = _pd
    if async_playwright is None:
        from playwright.async_api import async_playwright as _ap
        async_playwright = _ap
    if stealth_async is None:
        try:
            from playwright_stealth import stealth_async as _sa
            stealth_async = _sa
            HAS_STEALTH = True
        except ImportError:
            HAS_STEALTH = False
 
# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Silence Mozilla Remote Settings DevTools warnings during automation
os.environ["MOZ_REMOTE_SETTINGS_DEVTOOLS"] = "1"

from shared.proxy_config import PROXY_LABEL, get_proxy_pool, build_playwright_proxy

from idealista_scraper.config import (
    HARVEST_DEBOUNCE_SECONDS, PAGE_WAIT_MS, RETRY_MAX_ATTEMPTS, RETRY_BASE_DELAY,
    GOTO_WAIT_UNTIL, SCROLL_STEPS, LISTING_LINKS_PER_PAGE_MAX,
    EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    FAST_SCROLL_PAUSE_RANGE, FAST_CARD_DELAY_RANGE, FAST_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SCROLL_PAUSE_RANGE, EXTRA_STEALTH_CARD_DELAY_RANGE, EXTRA_STEALTH_POST_CARD_DELAY_RANGE,
    EXTRA_STEALTH_SESSION_LIMIT, EXTRA_STEALTH_REST_DURATION_RANGE,
    EXTRA_STEALTH_COFFEE_BREAK_RANGE, EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY,
    EXTRA_STEALTH_READING_TIME_PER_100_CHARS, USER_AGENTS, VIEWPORT_SIZES,
    BROWSER_ROTATION_POOL, MAX_PROFILE_POOL_SIZE, PROFILE_COOLDOWN_MINUTES
)
from idealista_scraper.utils import same_domain, canonical_listing_url, is_listing_url, sanitize_filename_part, play_captcha_alert, play_blocked_alert, simulate_human_interaction, solve_captcha_advanced, cleanup_stealth_profiles, reset_captcha_stats, get_captcha_stats, get_tbv_count, reset_tbv_counter

try:
    from app.shared_url_queue import SharedURLQueue
except ImportError:
    from shared_url_queue import SharedURLQueue

# Proxy para el browser: necesario para que la IP del browser coincida con la IP
# que usa 2Captcha al resolver DataDome (de lo contrario DataDome rechaza la cookie).
def _build_browser_proxy():
    """Build fresh Playwright proxy dict using current sticky session ID."""
    try:
        from shared.proxy_config import PROXY_CONFIG as _PC
        login = _PC['login']
        sid = _PC.get('sticky_session_id')
        if sid:
            login = f"{login}-session-{sid}"
        return {
            "server": f"http://{_PC['host']}:{_PC['port']}",
            "username": login,
            "password": _PC['password'],
        }
    except Exception:
        return None

_browser_proxy = _build_browser_proxy()


async def _launch_headless_worker(pw, engine: str, channel, profile_slot: int, proxy=None, user_agent=None):
    """Lanza un contexto Playwright headless para workers paralelos de enriquecimiento.

    Si se proporciona proxy (dict con server/username/password), se aplica al contexto.
    Devuelve el contexto lanzado, o None si el ejecutable no está disponible.
    user_agent: UA explícito para evitar HeadlessChrome en el UA del worker (Fix RC-3).
    """
    profile_dir = get_profile_dir(profile_slot)
    os.makedirs(profile_dir, exist_ok=True)
    if engine == "webkit":
        # WebKit en Windows no soporta proxies autenticados: siempre sin proxy
        kwargs_wk = dict(
            user_data_dir=profile_dir,
            headless=True,
            viewport={"width": 1280, "height": 800},
            timeout=60000,
            ignore_https_errors=True,
        )
        if user_agent:
            kwargs_wk["user_agent"] = user_agent
        return await pw.webkit.launch_persistent_context(**kwargs_wk)
    else:  # chromium (opera, etc.)
        executable_path = get_browser_executable_path(channel) if channel else None
        if channel and not executable_path:
            return None  # Ejecutable no encontrado — omitir este worker
        kwargs = dict(
            user_data_dir=profile_dir,
            headless=True,
            viewport={"width": 1280, "height": 800},
            timeout=60000,
            ignore_https_errors=True,
        )
        if executable_path:
            kwargs["executable_path"] = executable_path
            kwargs["channel"] = None  # channel=None cuando se usa executable_path
        if user_agent:
            kwargs["user_agent"] = user_agent
        if proxy:
            kwargs["proxy"] = proxy
        return await pw.chromium.launch_persistent_context(**kwargs)


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
        return urlunsplit((parts.scheme, parts.netloc, new_path, parts.query, parts.fragment))


def extract_page_from_url(url: str) -> int:
    """Extract page number from URL like /pagina-4 or /pagina-16.
    
    Returns 1 if no page number found in URL.
    """
    match = re.search(r'/pagina-(\d+)', url)
    return int(match.group(1)) if match else 1


# Default output directory - now uses 'salidas' subfolder
DEFAULT_OUTPUT_DIR = str(Path(__file__).parent.parent / "salidas")

# Worker isolation: each worker gets its own state files and browser profiles
_WORKER_ID = os.environ.get("SCRAPER_WORKER_ID", "").strip()
_WORKER_PREFIX = f"worker_{_WORKER_ID}_" if _WORKER_ID else ""

# Resume state file path
RESUME_STATE_FILE = str(Path(__file__).parent / f"{_WORKER_PREFIX}resume_state.json")

# Scrape history registry file path
SCRAPE_HISTORY_FILE = str(Path(DEFAULT_OUTPUT_DIR) / "scrape_history.json")

# =============================================================================
# MULTI-BROWSER IDENTITY ROTATION (ADVANCED EVASION)
# =============================================================================

# Identity Rotation State File
IDENTITY_STATE_FILE = str(Path(__file__).parent / f"{_WORKER_PREFIX}identity_state.json")

def load_identity_state() -> dict:
    """Load current identity state (current_profile_index) and cooldowns."""
    if not os.path.exists(IDENTITY_STATE_FILE):
        return {"current_index": 0, "cooldowns": {}}
    try:
        with open(IDENTITY_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {"current_index": 0, "cooldowns": {}}

def save_identity_state(state: dict) -> None:
    """Save identity state to file."""
    try:
        with open(IDENTITY_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
    except IOError:
        pass

def get_current_profile_config() -> dict:
    """Get the configuration for the currently active profile."""
    state = load_identity_state()
    # Ensure index is within bounds of our new POOL
    idx = state.get("current_index", 0)
    
    # Auto-heal invalid indices (e.g. from pool downsizing)
    if idx >= len(BROWSER_ROTATION_POOL) or idx < 0:
        print(f"⚠️ Repairing invalid profile index {idx} -> 0")
        idx = 0
        state["current_index"] = 0
        save_identity_state(state)
        
    return BROWSER_ROTATION_POOL[idx]

def get_profile_dir(profile_index: int) -> str:
    """Get the user data directory for a specific profile index (1-based from pool)."""
    base_dir = Path(__file__).parent.parent
    if _WORKER_ID:
        return str(base_dir / f"stealth_w{_WORKER_ID}_profile_{profile_index}")
    return str(base_dir / f"stealth_profile_{profile_index}")

def get_browser_executable_path(channel: Optional[str]) -> Optional[str]:
    """Get the executable path for custom browsers like Brave or Opera."""
    if not channel or channel in ["chrome", "msedge"]:
        return None
    
    if sys.platform != "win32":
        return None # Only Windows detection for now

    local_app_data = os.environ.get("LOCALAPPDATA", "")
    program_files = os.environ.get("ProgramFiles", "C:\\Program Files")
    program_files_x86 = os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)")

    # Path detection for portable browsers
    # app -> scraper -> RealEstateMaster
    project_root = Path(__file__).parent.parent.parent
    
    # Try ONE standard location for the 'browsers' folder (Strict Portable Mode)
    # Priority: python_portable/browsers (Self-contained)
    possible_browsers_dirs = [
        project_root / "python_portable" / "browsers"
    ]
    
    browsers_dir = None
    for d in possible_browsers_dirs:
        # Pick the FIRST one that exists
        if d.exists():
            browsers_dir = str(d)
            break
            
    if not browsers_dir:
        # Fallback to creating it if missing, but strictly in portable structure
        browsers_dir = str(project_root / "python_portable" / "browsers")
        try:
             os.makedirs(browsers_dir, exist_ok=True)
        except: pass
    
    if channel == "chrome":
        # Check for Google Chrome Portable in browsers dir
        # IMPORTANT: Inner binary FIRST to avoid PortableApps wrapper issues
        paths = [
            os.path.join(browsers_dir, "GoogleChromePortable", "App", "Chrome-bin", "chrome.exe"),
            os.path.join(browsers_dir, "GoogleChromePortable", "GoogleChromePortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p
        return None # Fallback to default system chrome via channel

    elif channel == "opera":
        paths = [
            os.path.join(browsers_dir, "OperaPortable", "App", "Opera", "opera.exe"),
            os.path.join(browsers_dir, "OperaPortable", "OperaPortable.exe"),
            os.path.join(browsers_dir, "Opera", "opera.exe"),
            os.path.join(local_app_data, "Programs", "Opera", "opera.exe"),
            os.path.join(program_files, "Opera", "opera.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p

    elif channel == "iron":
        paths = [
            os.path.join(browsers_dir, "IronPortable", "App", "Iron", "iron.exe"),
            os.path.join(browsers_dir, "IronPortable", "IronPortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p


    elif channel == "falkon":
        paths = [
            os.path.join(browsers_dir, "FalkonPortable", "App", "Falkon", "falkon.exe"),
            os.path.join(browsers_dir, "FalkonPortable", "FalkonPortable.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p

    elif channel == "brave":
        paths = [
            os.path.join(browsers_dir, "BravePortable", "App", "Brave", "brave.exe"),
            os.path.join(browsers_dir, "BravePortable", "BravePortable.exe"),
            os.path.join(browsers_dir, "Brave", "brave.exe"),
            os.path.join(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p

    elif channel == "vivaldi":
        paths = [
            os.path.join(browsers_dir, "VivaldiPortable", "App", "Vivaldi", "Application", "vivaldi.exe"),
            os.path.join(browsers_dir, "VivaldiPortable", "VivaldiPortable.exe"),
            os.path.join(browsers_dir, "Vivaldi", "Application", "vivaldi.exe"),
            os.path.join(program_files, "Vivaldi", "Application", "vivaldi.exe"),
        ]
        for p in paths:
            if os.path.exists(p): return p

    return None

def mark_current_profile_blocked() -> None:
    """Mark the current profile as blocked and start its cooldown."""
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    
    # Safety check for invalid index
    if current_idx >= len(BROWSER_ROTATION_POOL) or current_idx < 0:
        current_idx = 0
    
    # We use the pool config's 'index' (1-based) as the key for readability
    config = BROWSER_ROTATION_POOL[current_idx]
    pool_id = str(config["index"])
    
    state["cooldowns"][pool_id] = time.time()
    save_identity_state(state)
    print(f"🚫 Profile {pool_id} ({config['name']}) marked as BLOCKED at {time.ctime()}")

def rotate_identity():
    """
    Rotate to the NEXT available profile in the sequence.
    If multiple profiles are available, pick the next one in round-robin.
    If ALL are in cooldown, wait for the one with the minimum remaining time.
    Strictly ensures infinite rotation and waiting for cooldown expiration.
    """
    state = load_identity_state()
    current_idx = state.get("current_index", 0)
    pool_size = len(BROWSER_ROTATION_POOL)
    
    # Update cooldowns status based on current time
    cooldown_seconds = PROFILE_COOLDOWN_MINUTES * 60
    now = time.time()
    for pid in list(state["cooldowns"].keys()):
        blocked_time = state["cooldowns"][pid]
        if now - blocked_time >= cooldown_seconds:
            del state["cooldowns"][pid]

    # 1. Try to find the next available index in the pool (Round-Robin)
    available_indices = []
    for i in range(pool_size):
        idx = (current_idx + 1 + i) % pool_size
        config = BROWSER_ROTATION_POOL[idx]
        pid = str(config["index"])
        if pid not in state["cooldowns"]:
            available_indices.append(idx)
            
    if available_indices:
        # Pick the first available in the round-robin sequence starting from current_idx + 1
        next_idx = available_indices[0]
        state["current_index"] = next_idx
        save_identity_state(state)
        return BROWSER_ROTATION_POOL[next_idx], 0
    
    # 2. If NO profiles are available, find the one with the SHORTEST remaining wait
    wait_info = []
    for i in range(pool_size):
        config = BROWSER_ROTATION_POOL[i]
        pid = str(config["index"])
        blocked_time = state["cooldowns"].get(pid, now)
        remaining = max(1, cooldown_seconds - (now - blocked_time))
        wait_info.append((remaining, i))
    
    # Sort by remaining time (shortest first)
    wait_info.sort()
    min_wait, next_idx = wait_info[0]
    
    # Commit the rotation to the one we will wait for
    state["current_index"] = next_idx
    save_identity_state(state)
    
    return BROWSER_ROTATION_POOL[next_idx], min_wait

# Constants for backward compatibility (mapped to current profile)
# These will be dynamically resolved in the class, but we keep the variables
STEALTH_PROFILE_DIR = get_profile_dir(get_current_profile_config()["index"])
# Legacy compatibility for run_batch.py and others (Chromium-only; Firefox/Webkit removed)
BROWSER_ENGINES = ["chromium"]

def load_profile_cooldowns() -> dict: return {}
def save_profile_cooldowns(cooldowns: dict) -> None: pass
def mark_profile_blocked(engine: str) -> None: 
    """Legacy wrapper to mark current profile as blocked."""
    mark_current_profile_blocked()

def is_profile_available(engine: str) -> bool: 
    # For legacy scripts, we just say True and let the controller handle rotation
    return True

def get_cooldown_remaining(engine: str) -> int: 
    state = load_identity_state()
    # We can't easily map 'engine' string to a specific profile index without ambiguity,
    # so we return 0 for legacy compatibility.
    return 0


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
LAST_ENGINE_FILE = str(Path(__file__).parent / f"{_WORKER_PREFIX}last_engine.txt")


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

# Deep fingerprint spoofing script - injected before any page load
# Uses f-string to inject randomized GPU values
def generate_stealth_script(gpu_vendor=None, gpu_renderer=None):
    """Generate stealth script with randomized GPU fingerprint and advanced noise.

    GPU values are randomized per call (per browser session) if not provided.
    """
    if gpu_vendor is None or gpu_renderer is None:
        gpu_vendor, gpu_renderer = get_random_gpu()
    return f'''
// ==================== PHASE 1: DEEP FINGERPRINT SPOOFING ====================

// 1. Remove Chrome DevTools Protocol (CDP) signatures
try {{
    // Delete chrome.runtime which is a CDP indicator
    if (window.chrome && window.chrome.runtime) {{
        delete window.chrome.runtime;
    }}
    // cdc_ variables are cleaned up directly in section 8 below (delete window.cdc_...)
    // Note: Function.prototype.call hook removed — intercepting ALL .call() invocations
    // can break legitimate JS on Idealista and trigger detection by DataDome.
}} catch (e) {{}}

// 2. Spoof WebGL to match a real GPU (randomized per session)
try {{
    const getParameterProto = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(param) {{
        // UNMASKED_VENDOR_WEBGL
        if (param === 37445) return '{gpu_vendor}';
        // UNMASKED_RENDERER_WEBGL
        if (param === 37446) return '{gpu_renderer}';
        return getParameterProto.call(this, param);
    }};

    // Also patch WebGL2
    if (typeof WebGL2RenderingContext !== 'undefined') {{
        const getParameter2Proto = WebGL2RenderingContext.prototype.getParameter;
        WebGL2RenderingContext.prototype.getParameter = function(param) {{
            if (param === 37445) return '{gpu_vendor}';
            if (param === 37446) return '{gpu_renderer}';
            return getParameter2Proto.call(this, param);
        }};
    }}
}} catch (e) {{}}

// 3. Add realistic navigator.plugins (PluginArray-like with Symbol.iterator)
try {{
    const pluginData = [
        {{type: 'application/x-google-chrome-pdf', suffixes: 'pdf', description: 'Portable Document Format', name: 'Chrome PDF Plugin'}},
        {{type: 'application/pdf', suffixes: 'pdf', description: '', name: 'Chrome PDF Viewer'}},
        {{type: 'application/x-nacl', suffixes: '', description: 'Native Client Executable', name: 'Native Client'}}
    ];
    const plugins = Object.create(PluginArray.prototype);
    pluginData.forEach((p, i) => {{ plugins[i] = p; }});
    Object.defineProperty(plugins, 'length', {{value: pluginData.length, writable: false, enumerable: true}});
    plugins[Symbol.iterator] = function*() {{ for (let i = 0; i < this.length; i++) yield this[i]; }};
    plugins.item = function(i) {{ return this[i] || null; }};
    plugins.namedItem = function(name) {{ for (let i = 0; i < this.length; i++) {{ if (this[i].name === name) return this[i]; }} return null; }};
    plugins.refresh = function() {{}};
    Object.defineProperty(navigator, 'plugins', {{
        get: () => plugins
    }});
}} catch (e) {{}}

// 4. Fix navigator.languages
try {{
    Object.defineProperty(navigator, 'languages', {{
        get: () => ['es-ES', 'es', 'en-US', 'en']
    }});
}} catch (e) {{}}

// 5. Patch Permissions API
try {{
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (params) => {{
        if (params.name === 'notifications') {{
            return Promise.resolve({{state: 'denied', onchange: null}});
        }}
        return originalQuery.call(window.navigator.permissions, params);
    }};
}} catch (e) {{}}

// 6. Timing randomization
try {{
    const originalNow = Date.now;
    const randomOffset = Math.floor(Math.random() * 50);
    Date.now = function() {{
        return originalNow() + randomOffset;
    }};
}} catch (e) {{}}

// 7. Override connection info (jittered per session)
try {{
    const rtt = [50, 75, 100, 150][Math.floor(Math.random() * 4)];
    const downlink = [1.5, 5, 10, 15][Math.floor(Math.random() * 4)];
    Object.defineProperty(navigator, 'connection', {{
        get: () => ({{
            effectiveType: '4g',
            rtt: rtt,
            downlink: downlink,
            saveData: false
        }})
    }});
}} catch (e) {{}}

// 8. Hide automation indicators
try {{
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Array;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Promise;
    delete window.cdc_adoQpoasnfa76pfcZLmcfl_Symbol;
    
    Object.defineProperty(navigator, 'webdriver', {{
        get: () => undefined
    }});
}} catch (e) {{}}

// 9. Extra Hardware Randomization (varied per session)
try {{
    const cores = [4, 6, 8, 12, 16][Math.floor(Math.random() * 5)];
    const mem = [4, 8, 8, 16][Math.floor(Math.random() * 4)];
    Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => cores }});
    Object.defineProperty(navigator, 'deviceMemory', {{ get: () => mem }});
}} catch (e) {{}}

// 11. ADVANCED: Canvas Noise Fingerprinting (toDataURL + getImageData)
try {{
    const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
    HTMLCanvasElement.prototype.toDataURL = function(type) {{
        if (type === 'image/png') {{
            const ctx = this.getContext('2d');
            if (ctx) {{
                const imageData = ctx.getImageData(0, 0, 1, 1);
                imageData.data[0] = (imageData.data[0] + 1) % 255;
                ctx.putImageData(imageData, 0, 0);
            }}
        }}
        return originalToDataURL.apply(this, arguments);
    }};
    const origGetImageData = CanvasRenderingContext2D.prototype.getImageData;
    CanvasRenderingContext2D.prototype.getImageData = function() {{
        const data = origGetImageData.apply(this, arguments);
        data.data[0] = (data.data[0] + 1) % 255;
        return data;
    }};
}} catch (e) {{}}

// 12. ADVANCED: WebRTC IP Protection (Complete — blocks all real IP leaks via STUN/TURN)
try {{
    // Replace RTCPeerConnection with a stub that never exposes real IPs.
    // DataDome doesn't require functional WebRTC to pass validation.
    const RTCStub = function() {{
        return {{
            close: () => {{}},
            createDataChannel: () => ({{}}),
            createOffer: () => Promise.resolve({{}}),
            createAnswer: () => Promise.resolve({{}}),
            setLocalDescription: () => Promise.resolve(),
            setRemoteDescription: () => Promise.resolve(),
            addIceCandidate: () => Promise.resolve(),
            addEventListener: () => {{}},
            removeEventListener: () => {{}},
            getStats: () => Promise.resolve(new Map()),
            getSenders: () => [],
            getReceivers: () => [],
            onicecandidate: null,
            ontrack: null,
            ondatachannel: null,
            onnegotiationneeded: null,
            onsignalingstatechange: null,
            oniceconnectionstatechange: null,
            onicegatheringstatechange: null,
            onconnectionstatechange: null,
            signalingState: 'closed',
            iceConnectionState: 'closed',
            connectionState: 'closed',
            iceGatheringState: 'complete'
        }};
    }};
    if (window.RTCPeerConnection) {{
        window.RTCPeerConnection = RTCStub;
    }}
    if (window.webkitRTCPeerConnection) {{
        window.webkitRTCPeerConnection = RTCStub;
    }}
}} catch (e) {{}}

// 13. ADVANCED: Font List Obfuscation
try {{
    const originalFT = document.fonts.check;
    document.fonts.check = function(font) {{
        const standardFonts = ['arial', 'times new roman', 'helvetica', 'sans-serif'];
        if (standardFonts.some(f => font.toLowerCase().includes(f))) {{
            return originalFT.apply(document.fonts, arguments);
        }}
        return false;
    }};
}} catch (e) {{}}

// 14. MODERN: userAgentData Spoofing
try {{
    if (navigator.userAgentData) {{
        const majorVersion = (navigator.userAgent.match(/Chrome\\/(\\d+)/) || [null, '137'])[1];
        const brands = [
            {{ brand: 'Not(A:Brand', version: '99' }},
            {{ brand: 'Google Chrome', version: majorVersion }},
            {{ brand: 'Chromium', version: majorVersion }}
        ];
        Object.defineProperty(navigator, 'userAgentData', {{
            get: () => ({{
                brands: brands,
                mobile: false,
                platform: 'Windows',
                getHighEntropyValues: (hints) => Promise.resolve({{
                    brands: brands,
                    mobile: false,
                    platform: 'Windows',
                    platformVersion: ['10.0.0', '15.0.0'][Math.floor(Math.random() * 2)],
                    architecture: 'x86',
                    model: '',
                    uaFullVersion: `${{majorVersion}}.0.0.0`
                }})
            }})
        }});
    }}
}} catch (e) {{}}

'''

# For backward compatibility, generate a default script at module load
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
    Background task that subtly moves the mouse using organic Bézier curves.
    Helps maintain "human presence" during page loads.
    """
    import random
    import asyncio
    import math

    def bezier_curve(p0, p1, p2, p3, t):
        """Calculate point on cubic Bézier curve."""
        return (
            (1-t)**3 * p0 +
            3 * (1-t)**2 * t * p1 +
            3 * (1-t) * t**2 * p2 +
            t**3 * p3
        )

    async def organic_move(target_x, target_y, steps=20):
        # Current mouse position estimation (Playwright doesn't expose it easily)
        # We start from a random point or the last known point
        start_x, start_y = random.randint(100, 500), random.randint(100, 500)
        
        # Control points for the curve
        cp1_x = start_x + (target_x - start_x) * random.random()
        cp1_y = start_y + (target_y - start_y) * random.random()
        cp2_x = start_x + (target_x - start_x) * random.random()
        cp2_y = start_y + (target_y - start_y) * random.random()
        
        for i in range(1, steps + 1):
            t = i / steps
            x = bezier_curve(start_x, cp1_x, cp2_x, target_x, t)
            y = bezier_curve(start_y, cp1_y, cp2_y, target_y, t)
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.01, 0.03))

    while not stop_event.is_set():
        try:
            await asyncio.sleep(random.uniform(4, 12))
            
            if stop_event.is_set():
                break
                
            # Random target in the viewport
            target_x = random.randint(100, 1000)
            target_y = random.randint(100, 800)
            
            try:
                # Use fewer steps for jitter, more for "interest"
                await organic_move(target_x, target_y, steps=random.randint(10, 25))
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


class StopException(Exception):
    """Raised when the user stops the scraper."""
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
    browser_engine: str = "chromium"  # "chromium" or "firefox" - for multi-browser rotation
    
    # Smart Enrichment Mode
    smart_enrichment: bool = False  # If True, use province-file mapping and skip already enriched URLs
    parallel_enrichment: bool = False  # If True, Phase 3 runs two concurrent workers (proxy + WebKit sin proxy)
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
    start_time: float = 0
    
    # Internal state
    _stop_evt: Optional[asyncio.Event] = None
    _pause_evt: Optional[asyncio.Event] = None
    _processed: Set[str] = field(default_factory=set)
    _seen_in_search: Set[str] = field(default_factory=set) # Track URLs seen in search pages
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
    
    # Profile efficacy tracking
    _active_profile_name: str = "Unknown"
    _profile_stats: Dict[str, int] = field(default_factory=dict)
    
    # Checkpoint saving state
    _last_checkpoint_idx: int = 0  # Index of last saved property (usado para captcha checkpoints)
    _checkpoint_interval: int = 20  # Guardar cada N cambios reales
    _real_changes_for_checkpoint: int = 0  # Cambios reales desde último checkpoint (loop principal)
    _target_file: Optional[str] = None  # Cached target filename for checkpoints
    
    # Smart Enrichment state
    _enriched_urls: Set[str] = field(default_factory=set)  # URLs already enriched (skip completely)
    _all_existing_urls: Dict[str, dict] = field(default_factory=dict)  # All URLs in file with metadata
    _province_target_file: Optional[str] = None  # Province-based target file
    # Enrichment resume state (persisted across browser restarts)
    _in_enrichment: bool = False  # True while in the targeted deactivation check phase
    _enrichment_done_urls: Set[str] = field(default_factory=set)  # URLs already checked in this run
    _enrichment_missing_urls: List[str] = field(default_factory=list)  # Full missing list at enrichment start
    
    # Cross-thread stop signal (instantly visible from any thread, unlike asyncio.Event)
    _thread_stop_evt: Optional[threading.Event] = None
    
    @property
    def _should_stop(self) -> bool:
        """Check if stop has been requested from ANY thread (Flask or asyncio) or cross-process flag."""
        if self._stopped_by_user:
            return True
        if self._thread_stop_evt and self._thread_stop_evt.is_set():
            return True
        if self._stop_evt and self._stop_evt.is_set():
            return True
            
        # Cross-process signal sensing (Flag Files)
        scraper_dir = Path(__file__).parent.parent
        if (scraper_dir / "BATCH_STOP.flag").exists() or (scraper_dir / "batch_stop.flag").exists():
            self._stopped_by_user = True
            return True

        # Worker-specific stop flag
        if _WORKER_ID and (scraper_dir / f"WORKER_{_WORKER_ID}_STOP.flag").exists():
            self._stopped_by_user = True
            return True

        return False
    
    def __post_init__(self):
        _lazy_import()
        self._stop_evt = None
        self._pause_evt = None
        self._thread_stop_evt = threading.Event()  # Instant cross-thread signal
        self.scraped_properties = []
        self._processed = set()
        self._inflight = set()
        self._recent = {}
        self._index_map = {}
        self._stopped_by_user = False
        self._last_log_time = time.time()
        
        # Cleanup any orphaned blocked folders from previous sessions
        try:
            self._cleanup_old_blocked_profiles()
        except:
            pass

        # Initialize output_file with forced_target_file if provided
        if self.forced_target_file:
            self.output_file = self.forced_target_file
    
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
                if self._should_stop:
                    break
                chunk = min(1.0, remaining)
                await asyncio.sleep(chunk)
                remaining -= chunk
        # Simulated reading time logic
        delta = 0
    
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
                if self._should_stop:
                    break
                x = random.randint(100, width - 100)
                y = random.randint(100, height - 100)
                # Move with slight delay to simulate human movement
                await page.mouse.move(x, y, steps=random.randint(5, 15))
                await asyncio.sleep(random.uniform(0.1, 0.4))
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
            self.log("INFO", f"☕ Anti-bot: Pausa de descanso ({break_duration:.2f}s)")
            
            if self.on_status:
                self.on_status("resting", duration=int(break_duration))
            
            # Wait loop
            remaining = break_duration
            
            while remaining > 0:
                if self._should_stop:
                    self.log("INFO", "☕ Pausa interrumpida.")
                    break
                
                # Skip if mode switched to FAST
                if self.mode != "stealth":
                    self.log("INFO", "☕ Pausa omitida (cambiado a modo FAST).")
                    break
                    
                sleep_chunk = min(1.0, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
            
            # Finished coffee break

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
            self.log("INFO", f"😴 Anti-bot: Límite de sesión alcanzado ({self._session_property_limit} propiedades). Descansando {rest_duration}s...")
            
            if self.on_status:
                self.on_status("resting", duration=int(rest_duration))
            
            # Wait loop
            remaining = rest_duration
            
            while remaining > 0:
                if self._should_stop:
                    self.log("INFO", "😴 Descanso de sesión interrumpido.")
                    break
                
                # Skip if mode switched to FAST
                if self.mode != "stealth":
                    self.log("INFO", "😴 Descanso omitido (cambiado a modo FAST).")
                    break
                    
                sleep_chunk = min(1.0, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
            
            # Finished session rest break

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
        """Pause scraping and save state."""
        if self._pause_evt:
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._pause_evt.clear)
            else:
                self._pause_evt.clear()
        
        self.status = "paused"
        self.log("INFO", "Scraping paused")
        
        # Save state on pause
        try:
            cur_page = getattr(self, 'current_page', 1) or 1
            self.save_state(cur_page)
        except Exception as e:
            self.log("WARN", f"Could not save state on pause: {e}")
            
        if self.on_status:
            self.on_status("paused")
    
    def resume(self):
        """Resume scraping."""
        if self._pause_evt:
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._pause_evt.set)
            else:
                self._pause_evt.set()
            
        self.status = "running"
        self.log("INFO", "Scraping resumed")
        if self.on_status:
            self.on_status("running")
    
    def stop(self):
        """Stop scraping, save state and trigger export."""
        self._stopped_by_user = True
        
        # INSTANT cross-thread signal (visible immediately from any thread)
        if self._thread_stop_evt:
            self._thread_stop_evt.set()
        
        # Signal asyncio stop/pause events safely
        try:
            # Check if loop exists and is still running
            if self._loop and not self._loop.is_closed():
                if self._stop_evt: self._loop.call_soon_threadsafe(self._stop_evt.set)
                if self._pause_evt: self._loop.call_soon_threadsafe(self._pause_evt.set) # Unpause to allow graceful stop
            else:
                # Loop closed or not set - just set events directly (though nobody might be listening)
                if self._stop_evt: self._stop_evt.set()
                if self._pause_evt: self._pause_evt.set()
        except Exception as e:
            self.log("WARN", f"Could not signal stop/pause events (loop closed?): {e}")
            # Fallback
            if self._stop_evt: self._stop_evt.set()
            if self._pause_evt: self._pause_evt.set()
            
        self.status = "stopping"
        self.log("INFO", "Stopping scraper...")
        
        # Save state on stop
        try:
            cur_page = getattr(self, 'current_page', 1) or 1
            self.save_state(cur_page)
        except Exception as e:
            self.log("WARN", f"Could not save state on stop: {e}")
            
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

    def _clear_profile_locks(self, profile_dir: str):
        """Remove parent.lock and other lock files to prevent startup hangs.
        
        OPTIMIZED: Instead of walking the entire profile directory tree (which can
        contain thousands of cache files and takes 20-30s), we check only the known
        locations where lock files are created by each browser engine.
        """
        if not os.path.exists(profile_dir):
            return

        # Lock files are ALWAYS in root or in a fixed 'Default' subfolder — never deep.
        # Chromium: profile_dir/SingletonLock, profile_dir/Default/
        # Firefox:  profile_dir/parent.lock, profile_dir/.parentlock
        # WebKit:   profile_dir/WebsiteData/
        LOCK_NAMES = {
            "parent.lock", "singleton_lock", "singletonlock", "lock",
            ".parentlock", "lockfile", "sessionstore.js",
        }

        # Only scan the root and one level of known subdirectories
        scan_dirs = [profile_dir]
        for sub in ("Default", "default", "WebsiteData"):
            candidate = os.path.join(profile_dir, sub)
            if os.path.isdir(candidate):
                scan_dirs.append(candidate)

        removed_count = 0
        for scan_dir in scan_dirs:
            try:
                entries = os.listdir(scan_dir)
            except OSError:
                continue
            for name in entries:
                lname = name.lower()
                if lname in LOCK_NAMES or lname.endswith(".lock"):
                    lock_path = os.path.join(scan_dir, name)
                    if os.path.isfile(lock_path):
                        try:
                            os.remove(lock_path)
                            removed_count += 1
                        except Exception as e:
                            self.log("WARN", f"Failed to remove lock {name}: {e}")

        if removed_count > 0:
            self.log("WARN", f"🔓 Cleaned up {removed_count} stale lock files in profile.")

    def _kill_browser_by_channel(self, channel):
        """
        Kill processes for a specific channel. 
        SAFEGUARDED: Only kills if we can identify it as a portable isolation or Falkon.
        Never kills system Chrome/Firefox/Edge.
        
        NOTE: _cleanup_zombie_browsers is NOT called here anymore to avoid redundancy.
        It is already called once at the start of the main launch block.
        """
        if not channel: return

        targets = []
        # ONLY include executables that are strictly portable/isolated and NOT common user browsers
        # We explicitly EXCLUDE "chrome.exe", "firefox.exe", "msedge.exe", "opera.exe", "brave.exe"
        # to avoid killing the user's personal sessions.
        if channel == "iron": targets = ["iron.exe", "IronPortable.exe"]
        elif channel == "falkon": targets = ["falkon.exe", "FalkonPortable.exe"]
        elif channel == "vivaldi": targets = ["VivaldiPortable.exe"] # Standard vivaldi.exe might be user's

        # Portable versions often use specific names, but if they use the standard name (e.g. chrome.exe),
        # we cannot safe-kill them by name alone. We rely on _cleanup_zombie_browsers() for those.
        if targets:
            self.log("INFO", f"🔪 Pre-launch cleanup: Ensuring {', '.join(targets)} are closed...")
            for exe in targets:
                subprocess.run(
                    ["taskkill", "/F", "/IM", exe],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL
                )
            time.sleep(0.5)


    def _cleanup_zombie_browsers(self):
        """Kill only left-behind browser processes tied to the scraper's profiles.

        OPTIMIZED: Uses a single bulk WMI query (WHERE CommandLine LIKE '%stealth_profile%')
        instead of the previous approach that queried WMI once per-process in a loop.
        This reduces execution time from ~25s to <2s.
        """
        import subprocess
        if sys.platform == "win32":
            # Single bulk WMI query — vastly faster than per-process lookups.
            # Matches any browser process whose CommandLine contains 'stealth_profile'
            # OR whose path contains 'ms-playwright' (Playwright-managed binaries).
            ps_command = (
                "$procs = Get-CimInstance Win32_Process -Filter \""
                "CommandLine LIKE '%stealth_profile%' OR CommandLine LIKE '%ms-playwright%'\"; "
                "if ($procs) { "
                "  $procs | ForEach-Object { "
                "    Write-Output ('Killing PID ' + $_.ProcessId + ' (' + $_.Name + ')'); "
                "    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue "
                "  } "
                "}"
            )
            try:
                result = subprocess.run(
                    ["powershell", "-ExecutionPolicy", "Bypass", "-NoProfile", "-Command", ps_command],
                    capture_output=True, check=False, text=True, timeout=30
                )
                if result.stdout.strip():
                    self.log("WARN", f"🔪 Zombie cleanup: {result.stdout.strip()}")
            except Exception as e:
                self.log("WARN", f"Cleanup error: {e}")
        else:
            # Linux/macOS targeted cleanup
            targets = ["firefox", "chrome", "edge"]
            for target in targets:
                try:
                    subprocess.run(["pkill", "-9", "-f", f"{target}.*stealth_profile"],
                                   capture_output=True, check=False)
                except:
                    pass

    async def _heartbeat_monitor(self):
        """Background task to log activity periodically and detect hangs."""
        self.log("INFO", "💓 Heartbeat monitor started (60s check, 300s alarm)")
        while not self._should_stop:
            await asyncio.sleep(60)
            idle_time = time.time() - self._last_log_time
            if idle_time > 300: # 5 minutes of silence
                self.log("WARN", f"💓 Heartbeat: No activity for {idle_time/60:.0f}m. Scraper might be hanging or waiting silently.")
                self.log("INFO", f"💓 Status: {self.status}, Page: {self.current_page}, Property: {self.current_property_count}/{self.total_properties_expected}")
                # Dump stack traces to debug the hang
                await self._dump_async_stack_trace()
            elif idle_time > 60:
                # Normal heartbeat log at DEBUG level (not seen by user unless verbose)
                pass 

    async def _dump_async_stack_trace(self):
        """Dump stack traces of all running async tasks to log."""
        self.log("ERR", "📋 DUMPING ASYNC STACK TRACES (Potential Deadlock):")
        try:
            for task in asyncio.all_tasks():
                if task.done(): continue
                name = task.get_name()
                self.log("ERR", f"Task: {name}")
                stack = task.get_stack()
                if stack:
                    for frame in stack:
                        self.log("ERR", f"  {frame.f_code.co_filename}:{frame.f_lineno} in {frame.f_code.co_name}")
                else:
                    self.log("ERR", "  (No stack available)")
        except Exception as e:
            self.log("ERR", f"Failed to dump stack traces: {e}")

    def set_mode(self, mode: str):
        """Update scraping mode dynamically."""
        if mode not in ["fast", "stealth"]:
            return
        
        old_mode = self.mode
        self.mode = mode
        self.log("INFO", f"Switched mode: {old_mode} -> {mode}")
    
    async def _is_verification_screen(self, page) -> bool:
        """Checks if the page is showing Idealista's 'Device Verification' screen."""
        try:
            content = await page.evaluate("""() => {
                const bodyText = document.body ? document.body.innerText.toLowerCase() : '';
                return bodyText;
            }""")
            return "verificación del dispositivo" in content or "verificando su dispositivo" in content
        except:
            return False

    async def _wait_for_verification(self, page, max_attempts=3) -> bool:
        """Adaptive wait for Idealista's verification screen to disappear."""
        for i in range(1, max_attempts + 1):
            if await self._is_verification_screen(page):
                self.log("INFO", f"⏳ Idealista device verification detected. Waiting {i*10}s (Attempt {i}/{max_attempts})...")
                await self._interruptible_sleep(10.0)
                if self._should_stop:
                    return False
            else:
                if i > 1:
                    self.log("OK", "✅ Verification completed.")
                return True
        return False

    async def _check_for_blocks(self, page) -> Optional[str]:
        """
        Check if the page is a CAPTCHA or a block.
        Returns "block", "captcha", or None.
        Verification screens ('Verificación del dispositivo') are NOT blocks.
        """
        try:
            # Verification screen is NEVER a block — caller must wait for it to resolve
            if await self._is_verification_screen(page):
                return None

            page_data = await page.evaluate("""
                () => ({
                    title: document.title,
                    text: document.documentElement ? document.documentElement.innerText : (document.body ? document.body.innerText : ''),
                    hasDatadome: !!(document.querySelector('iframe[src*="captcha-delivery.com"]') ||
                                    window.dd ||
                                    document.querySelector('script[src*="captcha-delivery.com"]'))
                })
            """)

            title = (page_data.get("title") or "").lower()
            text_lower = (page_data.get("text") or "").lower()
            text_lower = re.sub(r'\s+', ' ', text_lower).strip()
            is_datadome = page_data.get("hasDatadome", False)

            # HARD BLOCK: only this exact phrase
            if "el acceso se ha bloqueado" in text_lower:
                self.log("WARN", "🛑 HARD BLOCK detected (El acceso se ha bloqueado). Rotando...")
                return "block"

            # CAPTCHA: DataDome iframe or "muchas peticiones tuyas"
            if is_datadome:
                return "captcha"
            if "muchas peticiones tuyas" in text_lower:
                self.log("WARN", "CAPTCHA detected (muchas peticiones tuyas)")
                return "captcha"

            # SSL/proxy certificate errors — treat as block to rotate
            if "conexión no es privada" in text_lower or "error de privacidad" in text_lower:
                self.log("WARN", "🛑 SSL certificate error detected. Rotando...")
                return "block"

            return None
        except Exception as e:
            return None

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
            "timestamp": datetime.now().isoformat(),
            # Enrichment resume fields
            "in_enrichment": self._in_enrichment,
            "enrichment_done_urls": list(self._enrichment_done_urls) if self._in_enrichment else [],
            "enrichment_missing_urls": self._enrichment_missing_urls if self._in_enrichment else [],
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
        """Delete the current profile if it has been blocked/poisoned to ensure next run is fresh."""
        import shutil
        
        self.log("WARN", "☣️  PROFILE POISONED: Purging blocked profile directory...")
        
        if os.path.exists(STEALTH_PROFILE_DIR):
            try:
                # Ensure browser is closed (this is double-checking as it should be closed by caller)
                shutil.rmtree(STEALTH_PROFILE_DIR, ignore_errors=True)
                self.log("OK", "✨ Poisoned profile deleted. Next run will generate a fresh, clean identity.")
            except Exception as e:
                self.log("ERR", f"Failed to delete poisoned profile: {e}")
        
        # Also trigger a general cleanup of any old residual blocked folders
        self._cleanup_old_blocked_profiles()

    def _cleanup_old_blocked_profiles(self):
        """Find and delete any old folders matching the BLOCKED pattern."""
        import shutil
        import glob
        
        # Target the parent directory of profiles
        base_dir = os.path.dirname(STEALTH_PROFILE_DIR)
        pattern = os.path.join(base_dir, "stealth_profile_BLOCKED_*")
        
        blocked_folders = glob.glob(pattern)
        if not blocked_folders:
            return
            
        for folder in blocked_folders:
            try:
                if os.path.isdir(folder):
                    shutil.rmtree(folder, ignore_errors=True)
            except:
                pass

    async def _save_checkpoint(self, additions: List[dict], target_file: Optional[str], existing_df, carry_cols: Set[str]):
        """Periodically save current progress to Excel."""
        if not additions:
            return
        
        log_file = self._province_target_file or target_file or 'Excel'
        self.log("INFO", f"💾 Auto-checkpoint: Saving {len(additions)} properties to {log_file}")
        try:
            # Pass stop check to prevent hangs
            check_stop = lambda: self._should_stop
            
            if self.smart_enrichment and self._province_target_file:
                export_split_by_distrito(existing_df, additions, os.path.join(self.output_dir, self._province_target_file), carry_cols, check_stop=check_stop)
            else:
                export_single_sheet(existing_df, additions, os.path.join(self.output_dir, target_file or self.out_xlsx), self._detected_sheet or self.sheet_name, carry_cols)
            
            self._last_checkpoint_idx = len(self.scraped_properties)
        except Exception as e:
            self.log("WARN", f"Checkpoint failed: {e}")
    
    async def _scrape_property_detail(self, page, url: str, *, label: str = "", use_proxy: bool = True, proxy_label: str = "", proxy_config=None):
        """Navega a una página de detalle de propiedad, extrae datos y resuelve captchas.

        Returns:
            dict: fila con datos scrapeados (enriched, merged) si éxito.
            None:  si la propiedad está dada de baja / no disponible.
        Raises:
            BlockedException, StopException, Exception("CAPTCHA_BLOCK_DETECTED").
        """
        _pfx = f"[{label}] " if label else ""
        key = canonical_listing_url(url)

        # 1) Navegar a la propiedad
        await self._goto_with_retry(page, url, use_proxy=use_proxy, label=label, proxy_label=proxy_label, proxy_config=proxy_config)
        if self._should_stop:
            raise StopException("Scraping interrumpido por el usuario")

        # 2) Extraer datos
        await page.wait_for_timeout(PAGE_WAIT_MS)
        try:
            d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
        except (IndexError, AttributeError, TypeError, asyncio.TimeoutError) as extraction_err:
            self.log("WARN", f"{_pfx}Extraction failed: {extraction_err}")
            d = {}

        # 3) Block inmediato
        if d.get("isBlocked"):
            self.log("ERR", f"{_pfx}🚫 BLOCK detected during extraction. Triggering rotation...")
            mark_current_profile_blocked()
            raise BlockedException("Acceso bloqueado durante extracción")

        row = {"URL": key, **d}
        miss = missing_fields(row, is_room_mode=self._is_room_mode)

        # 4) Página de baja / no disponible
        page_text = None
        if miss:
            page_text = await page.evaluate(
                "() => (document.body && document.body.innerText) ? document.body.innerText : ''"
            )
            is_not_found = any(msg in page_text.lower() for msg in [
                "no encontramos", "anuncio no disponible",
                "este anuncio ya no está disponible", "enlace antiguo",
                "anuncio ya no está publicado", "lo sentimos"
            ])
            if is_not_found:
                self.log("WARN", f"{_pfx}Anuncio no disponible: {key}")
                return None

            # 5) Uso indebido
            page_text_lower = page_text.lower()
            if any(kw in page_text_lower for kw in ["uso indebido", "se ha bloqueado", "uso no autorizado"]):
                self.log("ERR", f"{_pfx}🚫 'Uso indebido' detected. Triggering rotation...")
                mark_profile_blocked(self.browser_engine)
                raise BlockedException("Acceso bloqueado por uso indebido")

            # 6) CAPTCHA — auto-solve + espera pasiva
            try:
                _ctype = await page.evaluate("""() => {
                    if (document.querySelector('iframe[src*="captcha-delivery.com"]')) return 'DataDome';
                    if (typeof window.initGeetest !== 'undefined' || document.querySelector('.geetest_holder')) return 'GeeTest';
                    if (document.querySelector('.px-captcha-container, .nc-container, .captcha_slider')) return 'slider';
                    return 'desconocido';
                }""")
            except Exception:
                _ctype = 'desconocido'
            self.log("WARN", f"{_pfx}CAPTCHA detectado (tipo: {_ctype}). Intentando resolución automática...")

            if not label and self.on_status:
                self.on_status("captcha")

            # 6a) Auto-solve
            try:
                solved = await asyncio.wait_for(
                    solve_captcha_advanced(page, logger=self.log, use_proxy=use_proxy, proxy_config=proxy_config), timeout=180.0
                )
                if solved:
                    self.log("OK", f"{_pfx}✅ CAPTCHA resuelto automáticamente")
                    try:
                        d = await asyncio.wait_for(
                            extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode),
                            timeout=20.0,
                        )
                        row = {"URL": key, **d}
                        miss = missing_fields(row, is_room_mode=self._is_room_mode)
                    except Exception as re_ex:
                        self.log("WARN", f"{_pfx}Re-extracción tras auto-solve falló: {re_ex}")
            except asyncio.TimeoutError:
                self.log("WARN", f"{_pfx}Auto-solver timeout (180s)")
            except BlockedException:
                raise
            except Exception as solve_err:
                self.log("WARN", f"{_pfx}Auto-solver error: {solve_err}")

            # 6b) Espera pasiva 30 s
            if miss:
                self.log("WARN", f"{_pfx}Auto-solve insuficiente. Esperando 30s por resolución pasiva...")
                for _ in range(3):
                    if self._should_stop:
                        break
                    await asyncio.sleep(10.0)
                    try:
                        d = await asyncio.wait_for(
                            extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode),
                            timeout=20.0,
                        )
                        if d.get("isBlocked"):
                            self.log("ERR", f"{_pfx}🚫 Espera pasiva: página sigue BLOQUEADA.")
                            mark_current_profile_blocked()
                            raise BlockedException("Acceso bloqueado persistente")
                        row = {"URL": key, **d}
                        if not missing_fields(row, is_room_mode=self._is_room_mode):
                            self.log("OK", f"{_pfx}✅ CAPTCHA resuelto durante espera pasiva!")
                            miss = False
                            break
                    except BlockedException:
                        raise
                    except Exception:
                        pass

            if miss:
                self.log("ERR", f"{_pfx}CAPTCHA_BLOCK_DETECTED tras auto-solve + espera pasiva")
                mark_profile_blocked(self.browser_engine)
                raise Exception("CAPTCHA_BLOCK_DETECTED")

            # CAPTCHA resuelto
            if not label and self.on_status:
                self.on_status("running")

        # 7) Fecha + merge + enriquecimiento
        from datetime import datetime
        row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")

        if key in self._all_existing_urls:
            existing_data = self._all_existing_urls[key].get('full_row', {})
            if existing_data:
                row = {**existing_data, **row}

        row = mark_as_enriched(row)
        return row

    async def _goto_with_retry(self, page, url: str, use_proxy: bool = True, label: str = "", proxy_label: str = "", proxy_config=None) -> None:
        """Navigate to URL with retry logic. Detects browser close with 120s guard.

        use_proxy: False para workers lanzados sin proxy (ej. WebKit). Se pasa a solve_captcha_advanced
        para que omita los solvers de pago que producirían IP mismatch.
        label: Prefijo para logs (ej. 'chromium-w2', 'opera') para distinguir workers headless del browser principal.
        proxy_label: Etiqueta de proxy (ej. '[Proxy #2]'). Si vacío, usa el global PROXY_LABEL.
        proxy_config: dict raw del proxy del worker (host/port/login/password/sticky_session_id).
        """
        _plbl = proxy_label or PROXY_LABEL
        _pfx = f"[{label}] " if label else ""
        delay = RETRY_BASE_DELAY
        last_err: Optional[Exception] = None
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            if self._should_stop:
                raise StopException("Navegación interrumpida por el usuario")
            try:
                t_nav_start = time.time()
                self.log("INFO", f"{_plbl} {_pfx}Navigating to {url} (Attempt {attempt})...")
                
                # Global guard to prevent silent hangs (120s max for any navigation)
                try:
                    await asyncio.wait_for(
                        page.goto(url, wait_until=GOTO_WAIT_UNTIL, timeout=60000),
                        timeout=120.0
                    )
                except asyncio.TimeoutError:
                    self.log("ERR", f"{_pfx}⏰ NAVIGATION HANG: {url} timed out after 120s guard.")
                    raise Exception("NAVIGATION_HANG")
                except Exception as e:
                    # Specific handling for "Failed sending data to the peer" (Playwright connection error)
                    if "failed sending data to the peer" in str(e).lower():
                        self.log("WARN", f"{_pfx}🔌 Peer connection error detected. Cooling down 5s before retry...")
                        await asyncio.sleep(5.0)
                    raise e

                # Humanize interaction after reaching the page (Wrapped in timeout)
                try:
                    await asyncio.wait_for(simulate_human_interaction(page), timeout=5.0)
                except (asyncio.TimeoutError, Exception) as e:
                    if not isinstance(e, asyncio.TimeoutError):
                        self.log("WARN", f"{_pfx}⚠️ Human interaction failed: {e}")
                    pass

                # Check for CAPTCHA/Bot protection using unified helper
                try:
                    # 0. Immediate Soft Block Check (Hompage Redirect)
                    current_url = page.url
                    current_title = (await page.title()).lower()

                    if "idealista.com" == current_title and url != current_url and "idealista.com" not in url:
                         self.log("ERR", f"{_pfx}🚫 SOFT BLOCK DETECTED: Redirected to homepage from {url}")
                         mark_current_profile_blocked()
                         raise BlockedException("Soft Block: Homepage Redirect")

                    block_type = await self._check_for_blocks(page)

                    if block_type == "block":
                        self.log("ERR", f"{_pfx}🚫 BLOCK DETECTED on {url}: 'Uso indebido/Bloqueado'.")
                        if not label:  # solo browser visible; workers headless no emiten sonido
                            play_blocked_alert()
                        mark_current_profile_blocked()
                        raise BlockedException("Acceso bloqueado por uso indebido")

                    # Check for deactivated listing (specific text patterns)
                    page_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                    if "anuncio ya no está publicado" in page_text or "este anuncio no está publicado" in page_text:
                        self.log("WARN", f"{_pfx}El anuncio ya no está activo: {url}")
                        return

                    if block_type == "captcha":
                        curr_title = await page.title()
                        self.log("WARN", f"{_pfx}CAPTCHA DETECTED on {url} (Title: '{curr_title}')")

                        # Automatic/Manual solver logic...
                        self.log("INFO", f"{_pfx}Attempting automatic captcha solver...")
                        # Wrap logger to prepend worker label so messages route to correct UI panel
                        _solver_logger = self.log
                        if label:
                            _solver_logger = lambda lvl, msg, _l=label, _orig=self.log: _orig(lvl, f"[{_l}] {msg}")
                        try:
                            solved = await asyncio.wait_for(solve_captcha_advanced(page, logger=_solver_logger, use_proxy=use_proxy, proxy_config=proxy_config), timeout=180.0)
                            if solved:
                                self.log("OK", f"{_pfx}CAPTCHA solved automatically!")
                                return
                        except asyncio.TimeoutError:
                            self.log("WARN", f"{_pfx}Automatic solver timed out (180s).")
                        except Exception as e:
                            self.log("WARN", f"{_pfx}Automatic solver error: {e}")

                        # Workers headless no pueden resolver captchas manualmente → error inmediato
                        if label:
                            self.log("ERR", f"{_pfx}CAPTCHA no resuelto automáticamente en worker headless. Triggering auto-restart.")
                            mark_current_profile_blocked()
                            raise Exception("CAPTCHA_TIMEOUT")

                        self.log("WARN", f"{_pfx}>>> PLEASE SOLVE THE CAPTCHA MANUALLY IN THE BROWSER <<<")
                        if self.on_status: self.on_status("captcha")

                        captcha_wait_start = asyncio.get_running_loop().time()
                        captcha_timeout = 60

                        while True:
                            if self._should_stop: raise StopException("Interrumpido por el usuario")
                            elapsed = asyncio.get_running_loop().time() - captcha_wait_start
                            if elapsed > captcha_timeout:
                                self.log("ERR", f"{_pfx}CAPTCHA timeout - triggering auto-restart")
                                mark_current_profile_blocked()
                                raise Exception("CAPTCHA_TIMEOUT")

                            if not label:  # solo browser visible; workers headless no emiten sonido
                                play_captcha_alert()
                            await asyncio.sleep(2.0)

                            try:
                                current_block = await self._check_for_blocks(page)
                                if current_block == "block":
                                    self.log("ERR", f"{_pfx}🚫 Manual wait failed: Page is still BLOCKED. Triggering rotation.")
                                    mark_current_profile_blocked()
                                    raise BlockedException("Acceso bloqueado persistente")

                                new_title = (await page.title()).lower()
                                if "idealista" in new_title and new_title != "idealista.com" and "captcha" not in new_title:
                                    self.log("OK", f"{_pfx}✅ CAPTCHA solved! Resuming...")
                                    if self.on_status: self.on_status("running")
                                    break
                            except BlockedException: raise
                            except: pass
                except (BlockedException, StopException):
                    raise
                except Exception as e:
                    if str(e) == "CAPTCHA_TIMEOUT":
                        raise BlockedException("CAPTCHA_TIMEOUT")
                    self.log("DEBUG", f"{_pfx}Internal nav check ignored error: {e}")

                self.consecutive_datadome_fails = 0  # Reset on successful navigation
                await self._interruptible_sleep(3.0)
                return
            except StopException:
                raise
            except Exception as e:
                if self._should_stop:
                     raise StopException("Stop event detected during navigation error.")
                     
                error_msg = str(e).lower()
                # Detect browser close OR crash - pause and notify UI
                if any(msg in error_msg for msg in [
                    "browser has been closed",
                    "target page, context or browser has been closed",
                    "page crashed",
                    "target closed"
                ]):
                    self.log("ERR", f"{_pfx}🛑 BROWSER CRASH/CLOSE DETECTED on {url}: {e}")
                    # If stop event is set, it means the user initiated the close, so just log and raise
                    if self._should_stop:
                        self.log("INFO", f"{_pfx}Browser closed during stop sequence.")
                    else:
                        # Otherwise, it's an unexpected close/crash, so pause and notify
                        self.log("WARN", f"{_pfx}Browser was closed or crashed unexpectedly. Pausing scraper...")
                        self._browser_closed = True
                        self.pause()  # Pause instead of stop
                        if self.on_browser_closed:
                            self.on_browser_closed()
                    raise BrowserClosedException("Browser was closed")

                last_err = e
                self.log("WARN", f"{_pfx}goto attempt {attempt}/{RETRY_MAX_ATTEMPTS} failed: {e}")
                await self._interruptible_sleep(delay)
                delay *= 2
        if last_err:
            if "CAPTCHA_TIMEOUT" in str(last_err):
                self.consecutive_datadome_fails += 1
                self.log("WARN", f"{_pfx}🌩️ DataDome fail #{self.consecutive_datadome_fails} consecutivo (URL: {url})")
                if self.consecutive_datadome_fails >= 3:
                    pause_min = 20
                    self.log("ERR", f"{_pfx}🌩️ DataDome STORM: {self.consecutive_datadome_fails} fallos consecutivos. "
                             f"Pausa de {pause_min}min para dejar enfriar la IP antes de continuar.")
                    self.consecutive_datadome_fails = 0
                    await self._interruptible_sleep(pause_min * 60)
            raise last_err

    async def _wait_for_pause(self):
        """Wait if paused."""
        if not self._pause_evt.is_set() and not self._should_stop:
            self.log("WARN", "⏳ Scraper paused. Waiting for resume...")
            while not self._pause_evt.is_set() and not self._should_stop:
                await asyncio.sleep(1.0)
            
            if self._should_stop:
                self.log("INFO", "Stop signal received during pause.")
                raise StopException("Stop event detected during pause wait.")
                
            self.log("INFO", "▶️ Scraper resumed.")

    async def _interruptible_sleep(self, duration: float):
        """Sleep for duration, but wake up immediately if stopped."""
        if duration <= 0:
            return
        
        if duration > 10:
            self.log("INFO", f"⏳ Pausa larga detectada: {duration:.2f}s...")
        
        remaining = duration
        while remaining > 0:
            await self._wait_for_pause()
            if self._should_stop:
                raise StopException("Stop event detected during sleep.")
            chunk = min(0.5, remaining)  # 0.5s check interval
            await asyncio.sleep(chunk)
            remaining -= chunk
            # Refresh heartbeat timestamp to avoid false "potential hang" alarms
            self._last_log_time = time.time()

    
    def _export_to_excel(self, additions: List[dict], target_file: Optional[str], expired_urls: List[str]):
        """Export scraped data to Excel file."""
        if not additions:
            self.log("INFO", "No new properties to export.")
            return
        
        self.log("INFO", "Exporting data to Excel...")
        
        # Guard for PermissionError hangs
        check_stop = lambda: self._should_stop
        
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
        # Final export logic
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
        
        # CRITICAL FIX: Events MUST be created inside the target event loop, otherwise
        # set() / wait() operations from threadsafe callbacks will crash cross-loop.
        self._stop_evt = asyncio.Event()
        self._pause_evt = asyncio.Event()
        
        # Reset cross-thread stop signal for this new run
        if self._thread_stop_evt:
            self._thread_stop_evt.clear()
        self._stopped_by_user = False
        
        self.is_running = True
        self.status = "running"
        self.start_time = time.time()
        self._pause_evt.set()
        reset_captcha_stats()
        
        # Start heartbeat monitor
        heartbeat_task = asyncio.create_task(self._heartbeat_monitor())
        
        if self.on_status:
            self.on_status("running")
        
        self.log("INFO", f"Starting scraper in {self.mode.upper()} mode")
        
        # PROACTIVE CLEANUP (2026): Clear any leftover profiles from crashed sessions
        try:
            cleanup_stealth_profiles()
        except Exception as e:
            self.log("WARN", f"Initial cleanup failed: {e}")

        # === AUTOMATIC PRICE FILTERS (2026) ===
        # Apply mandatory limits: Alquiler <= 2000, Venta <= 300.000
        original_url = self.seed_url
        if "/alquiler-viviendas/" in self.seed_url.lower() and "con-precio-hasta_" not in self.seed_url.lower():
            if not self.seed_url.endswith("/"): self.seed_url += "/"
            self.seed_url += "con-precio-hasta_2000/"
            self.log("INFO", f"🏷️ Applied rental price filter: {self.seed_url}")
        elif "/venta-viviendas/" in self.seed_url.lower() and "con-precio-hasta_" not in self.seed_url.lower():
            if not self.seed_url.endswith("/"): self.seed_url += "/"
            self.seed_url += "con-precio-hasta_300000/"
            self.log("INFO", f"🏷️ Applied sale price filter: {self.seed_url}")
        
        # Detect room mode based on seed URL
        self._is_room_mode = "habitacion" in self.seed_url.lower()
        if self._is_room_mode:
            self.log("INFO", "Modo habitaciones detectado - usando columnas específicas para alquiler de habitaciones")

        
        scroll_pause, card_delay, post_card_delay = self.get_delays()
        
        # Log delay configuration for Extra Stealth
        if self.mode == "stealth":
            self.log("STEALTH", f"Ultra-long delays active: scroll {scroll_pause[0]:.1f}-{scroll_pause[1]:.1f}s, page {post_card_delay[0]:.1f}-{post_card_delay[1]:.1f}s")
        
        # === SEED URL REGISTRY LOOKUP & RESUME LOADING ===
        # CRITICAL: Load resume state BEFORE the first navigation to prevent state overwrite on immediate block.
        target_file = None
        url_dates = {}
        preloaded_urls = set()
        
        # 1. Load resume state first (prioritize it over everything else for continuity)
        resume_state = self.load_state()
        if resume_state and resume_state.get("seed_url") == self.seed_url:
            self.current_page = resume_state.get("current_page", 1)
            self.total_properties_expected = resume_state.get("total_properties_expected", 0)
            self.total_pages_expected = resume_state.get("total_pages_expected", 0)
            self._detected_sheet = resume_state.get("detected_sheet")
            
            if self.current_page > 1:
                self.log("INFO", f"🔄 INIT: Found resume state for Page {self.current_page}")

            # Restore processed URLs to avoid duplicates in the same run
            saved_processed = resume_state.get("processed_urls", [])
            if saved_processed:
                self._processed.update(saved_processed)
                self.log("INFO", f"🔄 INIT: Restored {len(saved_processed)} processed URLs from session file.")

            # Restore enrichment phase state if interrupted mid-enrichment
            if resume_state.get("in_enrichment", False):
                self._in_enrichment = True
                self._enrichment_done_urls = set(resume_state.get("enrichment_done_urls", []))
                self._enrichment_missing_urls = resume_state.get("enrichment_missing_urls", [])
                # Navigate directly to last listing page so the listing loop exits immediately
                self.current_page = self.total_pages_expected or self.current_page
                self.log("INFO", f"🔄 INIT: Resuming enrichment phase — {len(self._enrichment_done_urls)} done, {len(self._enrichment_missing_urls)} in original list")

        # 2. Lookup in Registry
        registry_entry = lookup_seed_url(self.seed_url)
        if registry_entry:
            target_file = registry_entry.get("output_file")
            target_path = os.path.join(self.output_dir, target_file) if target_file else None
            
            if target_path and os.path.exists(target_path):
                self.log("INFO", f"Found previous scrape: {target_file}")
                url_meta = load_all_urls_from_excel(target_path)
                self._all_existing_urls.update(url_meta)

                preloaded_urls = set(url_meta.keys())
                self.log("OK", f"Pre-loaded {len(preloaded_urls)} existing URLs from previous scrape")
                
                # Add to processed set to skip without navigation (Standard behavior: skip if in target file)
                self._processed.update(preloaded_urls)
            else:
                # File doesn't exist yet - keep the registered filename, it will be created
                self.log("INFO", f"Registered file not found: {target_file} - will be created during this scrape")
        
        if self.smart_enrichment:
            # Try to detect province/operation from URL if not already set
            if not self.province_name or not self.operation_type:
                detected_province, detected_operation = detect_province_and_operation(self.seed_url)
                if detected_province:
                    self.province_name = detected_province
                if detected_operation:
                    self.operation_type = detected_operation
            
            if self.province_name and self.operation_type:
                # Get province-based target file
                if self.forced_target_file:
                    province_file = self.forced_target_file
                else:
                    province_file, _, _ = get_output_file_for_url(self.seed_url)
                
                if province_file:
                    self._province_target_file = province_file
                    province_path = os.path.join(self.output_dir, province_file)
                    
                    # Override target_file with province-based file
                    target_file = province_file
                    self.log("INFO", f"🔍 Smart Enrichment: {self.province_name} ({self.operation_type}) -> {target_file}")
                    
                    # Load already enriched URLs (to skip completely)
                    if os.path.exists(province_path):
                        self._enriched_urls = load_enriched_urls(province_path)
                        self._all_existing_urls = load_all_urls_from_excel(province_path)
                        
                        enriched_count = len(self._enriched_urls)
                        total_in_file = len(self._all_existing_urls)
                        not_enriched = total_in_file - enriched_count
                        
                        self.log("OK", f"📊 File status: {total_in_file} total, {enriched_count} enriched, {not_enriched} pending")
                        
                        # Skip completely is disabled to allow 'Seen in search' tracking to update Last Seen dates
                        # self._processed.update(self._enriched_urls)
                        self.log("INFO", f"📊 Will identify {enriched_count} already enriched properties during search to skip detail pages")
                    else:
                        self.log("INFO", f"Province file not found - will create: {province_file}")
                    
                    # PROACTIVE FIX (2026): Load existing_df early to prevent session data loss
                    if province_path and os.path.exists(province_path):
                        try:
                            # Load full data once at start of session
                            # We use existing_df = ... at the run level to maintain it
                            existing_df, _, _ = load_existing_single_sheet(province_path, self._detected_sheet or self.sheet_name)
                            self.log("INFO", f"✅ Session Init: Loaded {len(existing_df)} rows from existing file as base.")
                        except Exception as e:
                            self.log("WARN", f"Failed to load existing base data: {e}. Starting fresh.")
                            existing_df = pd.DataFrame()
                else:
                    self.log("WARN", "Could not detect province/operation from URL. Smart enrichment partially disabled.")
        
        # Ensure self.output_file stays in sync with any detected/registered target_file
        if target_file and not self.output_file:
            self.output_file = target_file
        
        additions: List[dict] = []
        expired_urls: List[str] = []  # URLs to delete from Excel (expired listings)
        
        # Automatic Recovery Loop
        max_restarts = 5
        restart_count = 0
        self.unauthorized_restart_count = 0  # Track "uso no autorizado" restarts
        # Session-level blacklist: engines that consistently fail to launch this session
        # Stored as pool config 'index' values (1-based, per BROWSER_ROTATION_POOL)
        _launch_fail_counts: dict = {}  # pool_index -> consecutive launch failures
        LAUNCH_FAIL_BLACKLIST_THRESHOLD = 3  # Mark engine as session-dead after this many failures
        
        self.consecutive_skips = 0  # Track consecutive skipped properties to stop dead-end deep scrapes
        self.consecutive_datadome_fails = 0  # Track consecutive DataDome CAPTCHA_TIMEOUT failures
        
        # Initialize existing_df outside the recovery loop to persist across browser restarts
        if 'existing_df' not in locals():
            existing_df = pd.DataFrame()
        
        while not self._should_stop:
            target_file = self.output_file # Initialize safe default
            try:
                async with async_playwright() as pw:
                    # ── Regenerate proxy session for fresh exit IP ──
                    from shared.proxy_config import regenerate_session
                    new_sid = regenerate_session()
                    _browser_proxy = _build_browser_proxy()
                    self.log("INFO", f"🔑 New proxy session: {new_sid}")
                    # Reset IP-logged flag so new exit IP gets logged
                    try:
                        solve_datadome_2captcha = getattr(
                            __import__('idealista_scraper.utils', fromlist=['solve_datadome_2captcha']),
                            'solve_datadome_2captcha'
                        )
                        solve_datadome_2captcha._proxy_ip_logged = False
                    except Exception:
                        pass

                    # ========== ADVANCED IDENTITY ROTATION (2026) ==========
                    # Motores incompatibles con proxy autenticado en Windows — nunca deben
                    # ser el browser principal ya que navegarían sin proxy.
                    _PROXY_INCOMPATIBLE_ENGINES = {"webkit", "firefox"}
                    current_config = get_current_profile_config()
                    while current_config and current_config.get("engine") in _PROXY_INCOMPATIBLE_ENGINES:
                        self.log("WARN", f"⚠️ Saltando perfil '{current_config['name']}' "
                                         f"(motor '{current_config['engine']}' incompatible con proxy autenticado en Windows)")
                        mark_current_profile_blocked()
                        current_config, _ = rotate_identity()
                    engine = current_config["engine"]
                    channel = current_config["channel"]
                    profile_index = current_config["index"]
                    profile_dir = get_profile_dir(profile_index)

                    os.makedirs(profile_dir, exist_ok=True)

                    self.log("INFO", f"🎭 Identity: {current_config['name']} (Profile {profile_index})")
                    self.log("INFO", f"📂 Directory: {os.path.basename(profile_dir)}")
                    self.log("INFO", f"PLAYWRIGHT_BROWSERS_PATH: {os.environ.get('PLAYWRIGHT_BROWSERS_PATH', 'NOT SET')}")
                    
                    # Store current engine for block tracking
                    self.browser_engine = engine
                    self._active_profile_name = current_config["name"]
                    
                    # Initialize stats for this profile if not exists
                    if self._active_profile_name not in self._profile_stats:
                        self._profile_stats[self._active_profile_name] = 0
                    
                    # Select a random viewport for this session
                    viewport_width, viewport_height = random.choice(VIEWPORT_SIZES)
                    self.log("STEALTH", f"Using randomized viewport: {viewport_width}x{viewport_height}")
                    
                    # Clean Profile Strategy (2026) - Optimized for Portable Stealth
                    chromium_args = [
                        "--no-first-run",
                        "--no-default-browser-check",
                        # "--disable-blink-features=AutomationControlled",  # Removed: triggers Chromium warning bar that DataDome immediately flags
                        "--password-store=basic",
                        "--use-mock-keychain",
                        "--force-color-profile=srgb",
                        "--metrics-recording-only",
                        "--export-tagged-pdf",
                        "--disable-infobars",
                        # "--disable-web-security",  # REMOVED: Suspicious
                        # "--allow-running-insecure-content", # REMOVED: Suspicious
                        "--lang=es-ES,es", # FORCE SPANISH
                        "--disable-popup-blocking", # Reduce indicators
                        "--enable-features=NetworkService,NetworkServiceInProcess",
                    ]
                    
                    firefox_prefs = {
                        "dom.webdriver.enabled": False,
                        "useAutomationExtension": False,
                        "browser.tabs.warnOnClose": False,
                        "browser.shell.checkDefaultBrowser": False,
                        "browser.startup.homepage": "about:blank",
                        "datareporting.healthreport.uploadEnabled": False,
                        "datareporting.policy.dataSubmissionEnabled": False,
                        "toolkit.telemetry.enabled": False,
                        "toolkit.telemetry.unified": False,
                        "toolkit.telemetry.archive.enabled": False,
                        "extensions.update.enabled": False,
                        "extensions.getAddons.cache.enabled": False,
                        "app.update.auto": False,
                        "app.update.enabled": False,
                        "app.update.silent": True,
                        "identity.fxaccounts.enabled": False,
                        "services.sync.engine.prefs": False,
                        "marionette.log.level": "Error",
                        "accessibility.force_disabled": 1,
                        # NEW STEALTH PREFS (2026)
                        "general.useragent.locale": "es-ES",
                        "intl.accept_languages": "es-ES, es, en-US, en",
                        "network.http.accept-encoding": "gzip, deflate, br",
                        "privacy.resistFingerprinting": False, # CRITICAL: True makes you unique
                        "canvas.poisondata": False, 
                    }
                    # Silence Firefox remote settings warnings
                    os.environ["MOZ_REMOTE_SETTINGS_DEVTOOLS"] = "1"
                    
                    # PRE-LAUNCH: Verify browsers dir exists and is reachable
                    pw_browsers_path = os.environ.get('PLAYWRIGHT_BROWSERS_PATH', '')
                    if pw_browsers_path and not os.path.isdir(pw_browsers_path):
                        self.log("ERR", f"PLAYWRIGHT_BROWSERS_PATH no existe: {pw_browsers_path}")
                        self.log("ERR", "Ejecuta python_portable/SETUP.bat para instalar los navegadores.")
                    elif pw_browsers_path:
                        browser_dirs = [d for d in os.listdir(pw_browsers_path) if os.path.isdir(os.path.join(pw_browsers_path, d))]
                        self.log("INFO", f"Navegadores disponibles en browsers/: {browser_dirs}")

                    # PRE-LAUNCH CLEANUP
                    self._cleanup_zombie_browsers()
                    self._clear_profile_locks(profile_dir)
                    
                    try:
                        # LEVERAGE CHANNEL & ENGINE with launch retries
                        max_launch_retries = 4
                        ctx = None
                        
                        # Pre-launch specific cleanup for this channel (portable/Falkon only)
                        self._kill_browser_by_channel(channel)

                        for launch_attempt in range(1, max_launch_retries + 1):
                            # CHECK STOP AT START OF EVERY ITERATION
                            if self._should_stop:
                                self.log("INFO", "🛑 Stop requested. Aborting browser launch.")
                                raise StopException("Stop requested during browser launch")
                            try:
                                if engine == "firefox":
                                    executable_path = get_browser_executable_path(channel)
                                    # Add extra stability env vars for Firefox on Windows
                                    os.environ["MOZ_PROXY_ALLOW_BYPASS_FROM_SETTINGS"] = "1"
                                    os.environ["MOZ_REMOTE_SETTINGS_DEVTOOLS"] = "1"

                                    ctx = await pw.firefox.launch_persistent_context(
                                        user_data_dir=profile_dir,
                                        headless=True,  # Firefox siempre headless (sin ventana visible)
                                        viewport={"width": viewport_width, "height": viewport_height},
                                        firefox_user_prefs=firefox_prefs,
                                        ignore_default_args=["-foreground"],
                                        executable_path=executable_path,
                                        timeout=120000, # Increased to 120s for Windows Juggler stability
                                        proxy=_browser_proxy,
                                        ignore_https_errors=True,
                                    )
                                elif engine == "webkit": # Webkit (Safari-like)
                                    ctx = await pw.webkit.launch_persistent_context(
                                        user_data_dir=profile_dir,
                                        headless=False,
                                        viewport={"width": viewport_width, "height": viewport_height},
                                        timeout=120000, # Increased to 120s
                                        proxy=_browser_proxy,
                                        ignore_https_errors=True,
                                    )
                                else:
                                    # Chromium / Chrome / Edge / Brave / Opera / Iron / Falkon
                                    executable_path = get_browser_executable_path(channel)
                                    # If channel is 'brave', 'opera', or 'vivaldi', Playwright needs 'channel' to be None 
                                    # and 'executable_path' to be set.
                                    launch_channel = channel
                                    
                                    # EXPLICIT BLOCK: Falkon is unstable/unsupported
                                    if channel == "falkon":
                                        self.log("WARN", "🚫 Falkon is blacklisted due to stability issues. Skipping.")
                                        mark_current_profile_blocked()
                                        rotate_identity()
                                        break

                                    # If we have a custom portable path for these, use it by setting channel to None
                                    if channel in ["brave", "opera", "vivaldi", "iron", "chrome"]:
                                        if executable_path:
                                            launch_channel = None
                                            self.log("INFO", f"🚀 Launching Portable: {os.path.basename(executable_path)}")
                                        elif channel in ["brave", "opera", "vivaldi", "iron"]:
                                            # These MUST exist if specified, except for 'chrome' which can fallback to system
                                            self.log("WARN", f"⚠️ Portable Browser {channel} not found. Skipping identity...")
                                            # Induce a rotation to the next one
                                            mark_current_profile_blocked() # Mark as "bad" to avoid immediate re-selection
                                            rotate_identity()
                                            break # Out of launch attempts, will retry recovery loop which picks new identity

                                    ctx = await pw.chromium.launch_persistent_context(
                                        user_data_dir=profile_dir,
                                        headless=(channel == "opera"),  # Opera siempre headless; el resto visible
                                        viewport={"width": viewport_width, "height": viewport_height},
                                        args=chromium_args,
                                        ignore_default_args=["--enable-automation", "--no-sandbox"],
                                        channel=launch_channel,
                                        executable_path=executable_path,
                                        timeout=60000, # 60s for Chromium
                                        proxy=_browser_proxy,
                                        ignore_https_errors=True,
                                    )
                                if ctx: break
                            except Exception as le:
                                if launch_attempt < max_launch_retries:
                                    # Fail-Fast: Detect "Unsupported chromium channel" or likely configuration errors
                                    err_msg = str(le).lower()
                                    if "unsupported chromium channel" in err_msg or ("unsupported" in err_msg and "channel" in err_msg):
                                        self.log("ERR", f"🚨 Fatal configuration error: {le}")
                                        mark_current_profile_blocked()
                                        rotate_identity()
                                        break # Break launch loop to restart main loop with new identity
                                        
                                    if self._should_stop:
                                        break
                                    


                                    # Progressive sleep with randomization
                                    sleep_time = 3 + (launch_attempt * 2) 
                                    self.log("WARN", f"🚀 Launch attempt {launch_attempt} failed: {le}.")
                                    
                                    # Aggressive cleanup for Firefox/Webkit on repeated failure
                                    if engine in ["firefox", "webkit"]:
                                        if launch_attempt >= 1:
                                            self.log("WARN", f"☣️ {engine.capitalize()} hang detected. Killing processes & purging locks...")
                                            # Aggressive kill of zombies - SAFEGUARDED
                                            # ONLY kill if we can be reasonably sure it's not the user's main browser.
                                            # Actually, for Firefox, 'taskkill /IM firefox.exe' KILLS ALL INSTANCES.
                                            # We must NOT do this if the user uses Firefox personally.
                                            # DISABLED GLOBAL KILL for safety based on user report.
                                            # if sys.platform == "win32":
                                            #     try: subprocess.run(["taskkill", "/F", "/IM", "firefox.exe", "/T"], capture_output=True)
                                            #     except: pass
                                            
                                            self._cleanup_zombie_browsers()
                                            self._clear_profile_locks(profile_dir)
                                        
                                        # If it fails twice, it's likely corrupt. Nuke it.
                                        if launch_attempt >= 2:
                                            self.log("ERR", f"💣 {engine.capitalize()} persistent hang. DELETING profile directory for fresh start.")
                                            self.log("ERR", f"💣 {engine.capitalize()} persistent hang. DELETING profile directory for fresh start.")
                                            # Redundant kill - DISABLED for safety
                                            # if sys.platform == "win32":
                                            #     try: subprocess.run(["taskkill", "/F", "/IM", "firefox.exe", "/T"], capture_output=True)
                                            #     except: pass
                                            
                                            import shutil
                                            try:
                                                shutil.rmtree(profile_dir, ignore_errors=True)
                                                os.makedirs(profile_dir, exist_ok=True)
                                            except Exception as e:
                                                self.log("ERR", f"Failed to nuke profile: {e}")

                                    self.log("INFO", f"Retrying in {int(sleep_time)}s...")
                                    # Use interruptible sleep to allow immediate stop
                                    await self._interruptible_sleep(sleep_time)
                                    if self._should_stop:
                                        break
                                else:
                                    raise le

                        if not ctx:
                            raise Exception("Could not initialize browser context after retries.")
                            
                        self.log("OK", f"🚀 {current_config['name']} launched successfully")
                        
                        browser = None  # No separate browser object with persistent context
                        self._context = ctx  # Store reference for force close on stop
                        
                        # Record which engine we're using for rotation tracking
                        set_last_engine(engine)
                        
                        # ========== PHASE 1: DEEP FINGERPRINT SPOOFING ==========
                        # Inject comprehensive anti-detection script BEFORE any navigation
                        stealth_script = generate_stealth_script()
                        await ctx.add_init_script(stealth_script)
                        self.log("STEALTH", "Deep fingerprint spoofing injected (fresh GPU per session)")
                        
                        page = ctx.pages[0] if ctx.pages else await ctx.new_page()
                        
                        # Close extra tabs potentially restored by portable browsers (like Opera)
                        for p in ctx.pages:
                            if p != page:
                                try: await p.close()
                                except: pass
                        
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
                        
                    except (StopException, BrowserClosedException):
                        # Propagate stop signal
                        raise
                    except Exception as e:
                        self.log("ERR", f"Could not launch browser: {e}")
                        # --- Bug Fix: Rotate instead of stopping ---
                        # Track consecutive failures for this browser profile
                        pool_id = current_config.get('index', 0)
                        _launch_fail_counts[pool_id] = _launch_fail_counts.get(pool_id, 0) + 1
                        if _launch_fail_counts[pool_id] >= LAUNCH_FAIL_BLACKLIST_THRESHOLD:
                            self.log("ERR", f"🚫 Profile {pool_id} ({current_config.get('name','?')}) ha fallado {LAUNCH_FAIL_BLACKLIST_THRESHOLD}+ veces. Marcando como bloqueado permanentemente en esta sesión.")
                            mark_current_profile_blocked()  # 10-min cooldown
                        else:
                            self.log("WARN", f"⏭️ Fallo de lanzamiento {_launch_fail_counts[pool_id]}/{LAUNCH_FAIL_BLACKLIST_THRESHOLD}. Rotando al siguiente perfil...")
                            mark_current_profile_blocked()
                        rotate_identity()
                        await self._interruptible_sleep(3)
                        continue  # <-- Added: continue the outer while loop to try next browser
                    except Exception as e:
                        # --- Handle Peer Connection Error (Browser Crash) ---
                        if "Failed sending data to the peer" in str(e) or "peer connection error" in str(e).lower():
                            self.log("ERR", f"🔌 Peer connection error detected: {e}. Rotating browser identity...")
                            mark_current_profile_blocked()
                            rotate_identity()
                            await self._interruptible_sleep(5)
                            continue

                        self.log("ERR", f"Could not launch browser: {e}")
            
                    # Navigate to seed URL (or direct page resume)
                    seed_nav_failed = False
                    page_loaded_ok = False
                    try:
                        target_url = self.seed_url
                        if self.current_page > 1:
                            target_url = build_paginated_url(self.seed_url, self.current_page)
                            self.log("INFO", f"⏭️ Resuming directly from Page {self.current_page}...")

                        self.log("INFO", f"{PROXY_LABEL} Navigating to: {target_url}")
                        await page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
                        await asyncio.sleep(3.0)
                        self.log("OK", "Page opened successfully")
                        page_loaded_ok = True
                    except Exception as e:
                        self.log("ERR", f"Could not open seed URL: {e}")
                        seed_nav_failed = True

                    # --- Seed navigation failed (engine issue, not a block) → rotate without cooldown ---
                    if seed_nav_failed:
                        self.log("WARN", "⚡ Seed URL navigation failed (engine issue, not a block). Rotating without cooldown...")
                        try:
                            if 'mouse_jitter_task' in locals() and mouse_jitter_task:
                                mouse_jitter_task.cancel()
                            if ctx:
                                await ctx.close()
                        except Exception:
                            pass
                        rotate_identity()
                        await self._interruptible_sleep(3)
                        continue
            
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
                        
                        # 1. Early check (Simplified: check title only if needed or skip as per request)
                        # Page title checks are removed to strictly follow the text-based keywords requested.
                             
                        # 2. Check body text for specific block messages
                        body_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
                        text_lower = body_text.lower() if body_text else ""

                        # SSL/Certificate error detection — proxy cert not trusted by browser
                        ssl_error_keywords = [
                            "conexión no es privada",
                            "error de privacidad",
                            "err_cert_authority_invalid",
                            "your connection is not private",
                        ]
                        if any(kw in text_lower for kw in ssl_error_keywords):
                            self.log("ERR", "🔒 SSL CERTIFICATE ERROR detectado (proxy cert rechazado). Rotando identidad...")
                            raise BlockedException("SSL certificate error: proxy not trusted by browser")

                        # Only these two specific triggers as requested
                        if "el acceso se ha bloqueado" in text_lower:
                            self.log("WARN", "⚠️ BLOCK DETECTED: 'El acceso se ha bloqueado'")
                            raise BlockedException("Early block detection: El acceso se ha bloqueado")
                        
                        if "estamos recibiendo muchas peticiones tuyas" in text_lower:
                            self.log("INFO", "⚠️ CAPTCHA DETECTED: Page indicates high traffic. Allowing solver...")
                            # DO NOT raise BlockedException here. 
                            # Let the count extraction (which will return 0) trigger the solve_captcha_advanced logic.
                            pass
                                
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
                            if self._should_stop:
                                break

                            # --- EARLY CAPTCHA BAIL-OUT ---
                            # On the first attempt, check for DataDome before waiting 4s.
                            # If blocked, exit immediately instead of spinning 4 rounds.
                            if attempt == 0:
                                # Adaptive wait for Idealista's automatic verification to complete
                                # before checking for DataDome. This prevents false positives.
                                await self._wait_for_verification(page)
                                if self._should_stop:
                                    break
                                try:
                                    is_datadome_early = await page.evaluate(
                                        "() => !!document.querySelector('iframe[src*=\"captcha-delivery.com\"]')"
                                    )
                                    if is_datadome_early:
                                        self.log("WARN", "⚡ DataDome detectado en carga inicial. Saltando espera de H1...")
                                        break  # Exit retry loop → total_count stays 0 → solver is called immediately
                                except Exception:
                                    pass

                            # Wait for page to stabilize
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
                                
                                # --- MANDATORY BLOCK DETECTION (User Request) ---
                                if "el acceso se ha bloqueado" in h1txt.lower():
                                    self.log("WARN", "⚠️ BLOCK DETECTED in H1: 'El acceso se ha bloqueado'")
                                    raise BlockedException("Acceso bloqueado detectado en H1")

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
                        self.log("WARN", "⚠️ 0 properties found. Checking for CAPTCHA/Block...")
                        
                        # Try to solve captcha if present
                        try:
                            _captcha_solved = await asyncio.wait_for(
                                solve_captcha_advanced(page, logger=self.log),
                                timeout=180.0
                            )
                        except asyncio.TimeoutError:
                            _captcha_solved = False
                            self.log("WARN", "⏰ Timeout (180s) en solve_captcha_advanced (handler 0 props)")
                        if _captcha_solved:
                            self.log("OK", "CAPTCHA solve attempted. Re-verifying page...")
                            await self._interruptible_sleep(5.0)
                            h1txt = await page.evaluate(r"() => { const el = document.querySelector('#h1-container__text') || document.querySelector('#h1-container') || document.querySelector('h1'); return el ? el.textContent.trim() : ''; }")
                            if h1txt:
                                match = re.search(r'(\d{1,3}(?:\.\d{3})*)\s*(?:vivienda|pisos?|casas?|inmuebles?|anuncios?|habitaci[oó]n|habitaciones)', h1txt, re.IGNORECASE)
                                if match:
                                    total_count = int(match.group(1).replace('.', ''))
                                    self.log("OK", f"Success after solve: {total_count} properties found")

                        if total_count == 0:
                            self.log("WARN", "⚠️ HARD BLOCK/CAPTCHA FAIL: 0 properties found. Initiating rotation.")
                            # ROTATION LOGIC (2026): Strict Sequential with Cooldown
                            # Only mark as blocked if the page actually loaded (not an engine/nav issue)
                            if page_loaded_ok:
                                mark_current_profile_blocked()
                            else:
                                self.log("INFO", "⚡ Page never loaded successfully — skipping block mark (engine issue, not a real block)")
                            
                            # Save state for resume
                            self.save_state(self.current_page or 1, target_file)
                            
                            # Cancel mouse jitter and close browser
                            current_profile_idx = get_current_profile_config()["index"]
                            try:
                                if 'mouse_jitter_task' in locals() and mouse_jitter_task:
                                    mouse_jitter_task.cancel()
                                if ctx:
                                    await ctx.close()
                                
                                # GRANULAR CLEANUP: Erase the blocked profile's data immediately
                                self.log("INFO", f"🧼 Cleaning up blocked Profile {current_profile_idx}...")
                                cleanup_stealth_profiles(index=current_profile_idx)
                            except:
                                pass
                            
                            # This function handles sequential overflow
                            next_config, wait_time = rotate_identity()

                            # Circuit breaker: si hay muchos t=bv acumulados, pausar AQUÍ donde
                            # _interruptible_sleep no está limitado por el wait_for(timeout=180s)
                            try:
                                from idealista_scraper.config import TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN
                            except ImportError:
                                TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN = 8, 30
                            _cb_count = get_tbv_count()
                            if _cb_count >= TBV_CIRCUIT_BREAKER_THRESHOLD:
                                reset_tbv_counter()
                                self.log("WARN", f"🚨 CIRCUIT BREAKER [{_cb_count} t=bv]: pausa de {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min para enfriar IPs...")
                                if self.on_status:
                                    self.on_status("blocked", message=f"Circuit breaker: pausa {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min...")
                                try:
                                    await self._interruptible_sleep(TBV_CIRCUIT_BREAKER_PAUSE_MIN * 60)
                                except StopException:
                                    break
                                self.log("INFO", "Circuit breaker expirado. Reanudando con nueva identidad...")
                                wait_time = 0  # ya esperamos suficiente, no añadir cooldown adicional

                            self.log("WARN", f"🔄 ROLLING OVER to Profile {next_config['index']} ({next_config['name']})...")
                            if wait_time > 0:
                                self.log("INFO", f"⏳ Profile is in cooldown ({int(wait_time)}s). Waiting...")
                            self.log("INFO", f"Restarting in {int(wait_time) + 5} seconds with fresh identity...")

                            if self.on_status:
                                self.on_status("blocked", message=f"Rotando a Perfil {next_config['index']}...")

                            # Wait cooldown
                            try:
                                await self._interruptible_sleep(wait_time + 5.0)
                            except StopException:
                                self.log("INFO", "Rollover wait cancelled by stop event.")
                                break

                            if self._should_stop:
                                break

                            continue  # Loop back to restart with new browser identity

            
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
                    if total_count > 0:
                        reset_tbv_counter()  # scrape exitoso: limpiar contador t=bv para evitar falsos positivos del circuit breaker

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
                            # Priority: If user forced a file, never redetect from city
                            if self.forced_target_file:
                                new_target_file = self.forced_target_file
                            else:
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
                            
                            # CRITICAL: Reload existing data for the NEW target file
                            out_effective_new = os.path.join(self.output_dir, target_file)
                            if os.path.exists(out_effective_new):
                                self.log("INFO", f"🔄 Switching base data to {target_file}...")
                                existing_df, _, _ = load_existing_single_sheet(out_effective_new, self._detected_sheet or self.sheet_name)
                                self._all_existing_urls = load_all_urls_from_excel(out_effective_new)
                                self._enriched_urls = load_enriched_urls(out_effective_new)
                                preloaded_urls = set(self._all_existing_urls.keys())
                                self.log("OK", f"📊 New file base: {len(existing_df)} rows loaded.")
                            else:
                                existing_df = pd.DataFrame()
                                self._all_existing_urls = {}
                                self._enriched_urls = set()
            
                    # target_file and url_dates already set from registry lookup above (or overridden)
            
                    # === RUN-LOOP RESUME CHECK (Double check) ===
                    # If we just rotated after a block, we want to start from the page we were on.
                    # self.current_page is already set by the INIT logic above, but we refresh local variables.
                    page_num = self.current_page or extract_page_from_url(self.seed_url) or 1
                    
                    if page_num > 1:
                        self.log("INFO", f"🔄 RESUME: Initializing scraper at Page {page_num}")
                    # ====================

                    if page_num > 1:
                        self.log("INFO", f"Detected starting page: {page_num}")
            
                    # Calculate starting property index based on page number
                    # Page 1 starts at property 1, page 2 at 31, etc. (30 properties per page)
                    property_idx = (page_num - 1) * 30
                    self.current_property_count = property_idx
                    self.emit_progress()
                    skipped = 0
                    updated = 0
                    new_scraped = 0
                    smart_skipped = 0
                    deactivated_count = 0

                    # ===== HEADLESS WORKERS: Scraping paralelo de detalle de propiedades =====
                    _scrape_queue = SharedURLQueue()  # Cola productor-consumidor vacía
                    _worker_tasks = []
                    _worker_counters = {"scraped": 0, "real_changes": 0, "last_checkpoint_real_changes": 0}
                    _worker_checkpoint_lock = asyncio.Lock()
                    _worker_webkit_ctx = None
                    _worker_opera_ctx = None
                    _worker_webkit_page = None
                    _worker_opera_page = None
                    if self.parallel_enrichment and not self._in_enrichment:

                        async def _detail_scrape_worker(
                            initial_page, worker_label: str,
                            engine: str, channel, profile_slot: int,
                            proxy=None, use_proxy: bool = True,
                            proxy_label: str = "",
                            proxy_config=None,
                        ):
                            nonlocal new_scraped
                            nonlocal _worker_webkit_ctx, _worker_webkit_page
                            nonlocal _worker_opera_ctx, _worker_opera_page

                            _WORKER_MAX_RECOVERY = 3
                            _WORKER_RECOVERY_COOLDOWN = 45  # segundos entre intentos de recuperación

                            current_page = initial_page
                            _w_scraped = 0
                            _recovery_count = 0

                            while not self._should_stop and _recovery_count <= _WORKER_MAX_RECOVERY:
                                _blocked_url = None
                                try:
                                    # ── Bucle interno de consumo de cola ──────────────────
                                    while not self._should_stop:
                                        m_url = await _scrape_queue.claim()
                                        if m_url is None:
                                            return  # Cola cerrada y vacía — fin normal
                                        key = canonical_listing_url(m_url)
                                        if key in self._processed:
                                            continue
                                        _blocked_url = m_url  # por si hay excepción antes de completar
                                        try:
                                            await self._interruptible_sleep(random.uniform(3, 8))
                                            if self._should_stop:
                                                await _scrape_queue.release(m_url)
                                                return
                                            self.log("INFO", f"[{worker_label}] Scraping ({_w_scraped+1}): {m_url}")
                                            row = await self._scrape_property_detail(
                                                current_page, m_url, label=worker_label, use_proxy=use_proxy, proxy_label=proxy_label, proxy_config=proxy_config
                                            )
                                            _blocked_url = None  # completado con éxito
                                            if row is not None:
                                                additions.append(row)
                                                self.scraped_properties.append(row)
                                                self._processed.add(key)
                                                _w_scraped += 1
                                                new_scraped += 1
                                                self.log("OK", f"[{worker_label}] Scraped: {key}")
                                                if self.on_property:
                                                    self.on_property(row)
                                                _worker_counters["real_changes"] += 1
                                                real_since = _worker_counters["real_changes"] - _worker_counters["last_checkpoint_real_changes"]
                                                if real_since >= self._checkpoint_interval:
                                                    async with _worker_checkpoint_lock:
                                                        real_since = _worker_counters["real_changes"] - _worker_counters["last_checkpoint_real_changes"]
                                                        if real_since >= self._checkpoint_interval:
                                                            t_cp = time.time()
                                                            await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                                                            _worker_counters["last_checkpoint_real_changes"] = _worker_counters["real_changes"]
                                                            self.log("INFO", f"Saved periodic checkpoint in {time.time() - t_cp:.2f}s")
                                            else:
                                                self._processed.add(key)
                                        except (BlockedException, BrowserClosedException):
                                            raise  # propagar al bucle de recuperación externo
                                        except StopException:
                                            return
                                        except Exception as e:
                                            if "CAPTCHA_BLOCK_DETECTED" in str(e):
                                                raise BlockedException(str(e))  # tratar como bloqueo → recuperación
                                            self.log("WARN", f"[{worker_label}] Error scraping {m_url}: {e}")
                                            self._processed.add(key)
                                            _blocked_url = None
                                            continue

                                    break  # salida normal del bucle externo (cola agotada)

                                except (BlockedException, BrowserClosedException) as block_exc:
                                    if _blocked_url:
                                        await _scrape_queue.release(_blocked_url)
                                        _blocked_url = None

                                    if _recovery_count >= _WORKER_MAX_RECOVERY:
                                        self.log("WARN", f"[{worker_label}] ⛔ Max recuperaciones ({_WORKER_MAX_RECOVERY}) alcanzadas. Deteniendo worker.")
                                        break

                                    _recovery_count += 1
                                    self.log("WARN", f"[{worker_label}] 🔄 Bloqueo detectado ({block_exc}). "
                                                     f"Recuperación {_recovery_count}/{_WORKER_MAX_RECOVERY} — "
                                                     f"cooldown {_WORKER_RECOVERY_COOLDOWN}s...")

                                    # Cerrar contexto bloqueado
                                    _old_ctx = _worker_webkit_ctx if worker_label == "chromium-w2" else _worker_opera_ctx
                                    try:
                                        if _old_ctx:
                                            await _old_ctx.close()
                                    except Exception:
                                        pass

                                    # Esperar cooldown (cancelable si se solicita stop)
                                    for _ in range(_WORKER_RECOVERY_COOLDOWN):
                                        if self._should_stop:
                                            return
                                        await asyncio.sleep(1)

                                    # Re-lanzar con UA fresco
                                    if worker_label == "chromium-w2":
                                        _new_ua_pool = [ua for ua in USER_AGENTS if 'OPR' not in ua and 'Edg' not in ua]
                                    else:  # opera — Chrome UA para compatibilidad con CapSolver (OPR excluido)
                                        _new_ua_pool = [ua for ua in USER_AGENTS if 'OPR' not in ua and 'Edg' not in ua]
                                    _new_ua = random.choice(_new_ua_pool) if _new_ua_pool else random.choice(USER_AGENTS)

                                    try:
                                        _new_ctx = await _launch_headless_worker(
                                            pw, engine, channel, profile_slot,
                                            proxy=proxy, user_agent=_new_ua
                                        )
                                    except Exception as launch_err:
                                        self.log("WARN", f"[{worker_label}] Error re-lanzando worker: {launch_err}. Deteniendo.")
                                        break

                                    if not _new_ctx:
                                        self.log("WARN", f"[{worker_label}] No se pudo re-lanzar (ejecutable no disponible). Deteniendo.")
                                        break

                                    try:
                                        await _new_ctx.add_init_script(generate_stealth_script())
                                        current_page = _new_ctx.pages[0] if _new_ctx.pages else await _new_ctx.new_page()
                                    except Exception as page_err:
                                        self.log("WARN", f"[{worker_label}] Error creando página tras re-lanzamiento: {page_err}. Deteniendo.")
                                        break

                                    # Actualizar referencias externas para que Phase 2 use el nuevo contexto
                                    if worker_label == "chromium-w2":
                                        _worker_webkit_ctx = _new_ctx
                                        _worker_webkit_page = current_page
                                    else:
                                        _worker_opera_ctx = _new_ctx
                                        _worker_opera_page = current_page

                                    self.log("OK", f"[{worker_label}] ✅ Worker recuperado con nueva identidad. Continuando scraping...")

                            self.log("INFO", f"[{worker_label}] Worker finalizado. Scrapeadas: {_w_scraped} propiedades.")

                        # ── Asignar proxies a los workers secundarios ──
                        # El browser principal usa proxy pool[0] (Proxy #1).
                        # Workers 2 y 3 usan pool[1] y pool[2] si están disponibles.
                        _proxy_pool = get_proxy_pool()
                        _worker2_proxy = build_playwright_proxy(_proxy_pool[1]) if len(_proxy_pool) > 1 else _browser_proxy
                        _worker3_proxy = build_playwright_proxy(_proxy_pool[2]) if len(_proxy_pool) > 2 else _browser_proxy
                        _w2_label = f"Proxy #{2 if len(_proxy_pool) > 1 else 1}"
                        _w3_label = f"Proxy #{3 if len(_proxy_pool) > 2 else 1}"

                        # Launch Chromium worker (slot 98) — con Proxy #2
                        # (antes era WebKit sin proxy; WebKit no soporta auth proxies en Windows)
                        try:
                            _wk_ua_pool = [ua for ua in USER_AGENTS if 'OPR' not in ua and 'Edg' not in ua]
                            _wk_ua = random.choice(_wk_ua_pool) if _wk_ua_pool else random.choice(USER_AGENTS)
                            _worker_webkit_ctx = await _launch_headless_worker(pw, "chromium", None, 98, proxy=_worker2_proxy, user_agent=_wk_ua)
                            if _worker_webkit_ctx:
                                await _worker_webkit_ctx.add_init_script(generate_stealth_script())
                                _worker_webkit_page = _worker_webkit_ctx.pages[0] if _worker_webkit_ctx.pages else await _worker_webkit_ctx.new_page()
                                _worker_tasks.append(asyncio.create_task(_detail_scrape_worker(
                                    _worker_webkit_page, "chromium-w2",
                                    engine="chromium", channel=None, profile_slot=98,
                                    proxy=_worker2_proxy, use_proxy=True,
                                    proxy_label=f"[{_w2_label}]",
                                    proxy_config=_proxy_pool[1] if len(_proxy_pool) > 1 else None,
                                )))
                                self.log("INFO", f"✅ [{_w2_label}] Chromium scraping worker lanzado (slot 98)")
                        except Exception as e:
                            self.log("WARN", f"⚠️ Chromium worker (slot 98) no pudo lanzar: {e}")

                        # Launch Opera/chromium (slot 96) — con Proxy #3
                        try:
                            _opr_ua_pool = [ua for ua in USER_AGENTS if 'OPR' not in ua and 'Edg' not in ua]
                            _opr_ua = random.choice(_opr_ua_pool) if _opr_ua_pool else random.choice(USER_AGENTS)
                            _worker_opera_ctx = await _launch_headless_worker(pw, "chromium", "opera", 96, proxy=_worker3_proxy, user_agent=_opr_ua)
                            if _worker_opera_ctx:
                                await _worker_opera_ctx.add_init_script(generate_stealth_script())
                                _worker_opera_page = _worker_opera_ctx.pages[0] if _worker_opera_ctx.pages else await _worker_opera_ctx.new_page()
                                _worker_tasks.append(asyncio.create_task(_detail_scrape_worker(
                                    _worker_opera_page, "opera",
                                    engine="chromium", channel="opera", profile_slot=96,
                                    proxy=_worker3_proxy, use_proxy=True,
                                    proxy_label=f"[{_w3_label}]",
                                    proxy_config=_proxy_pool[2] if len(_proxy_pool) > 2 else None,
                                )))
                                self.log("INFO", f"✅ [{_w3_label}] Opera scraping worker lanzado (slot 96)")
                            else:
                                self.log("INFO", "ℹ️ Opera portable no disponible para scraping worker.")
                        except Exception as e:
                            self.log("WARN", f"⚠️ Opera worker (slot 96) no pudo lanzar: {e}")

                    scraping_finished = False  # Track clean completion
                    # Skip main listing loop if resuming an interrupted enrichment phase
                    if self._in_enrichment:
                        self.log("INFO", "🔄 Saltando bucle de listado: reanudando fase de enrichment interrumpida...")
                        scraping_finished = True
                    while not self._should_stop and not scraping_finished:
                        await self._wait_for_pause()
                        if self._should_stop or scraping_finished:
                            break

                        self.current_page = page_num
                        list_url = build_paginated_url(self.seed_url, page_num)
                        current_url = page.url
                        
                        # Fixed: Use precise extraction instead of 'in' to avoid false positives
                        # (e.g., Page 1 URL is a substring of Page 2 URL)
                        # Also guard against property detail pages (/inmueble/) which have no /pagina-X
                        # and would make extract_page_from_url return 1, falsely matching page_num=1
                        # after a Deep Scrape Mode transition.
                        current_page_in_browser = extract_page_from_url(current_url)
                        is_listing_page = "/inmueble" not in current_url
                        # Deep Scrape transition sets _force_navigate to bypass false positives
                        # when extract_page_from_url returns 1 for the old listing page
                        force_nav = getattr(self, '_force_navigate', False)
                        is_already_on_target = (not force_nav) and ((list_url.rstrip('/') == current_url.rstrip('/')) or (is_listing_page and current_page_in_browser == page_num))

                        try:
                            if is_already_on_target and page_num == self.current_page:
                                self.log("INFO", f"Already on Page {page_num} listing. Skipping redundant navigation.")
                            else:
                                self.log("INFO", f"{PROXY_LABEL} Opening listing page {page_num}/{self.total_pages_expected}: {list_url}")
                                await self._goto_with_retry(page, list_url)
                                self._force_navigate = False  # Clear flag after successful navigation

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
                        
                                try:
                                    # 1. Broad block check
                                    block_type = await self._check_for_blocks(page)
                                    
                                    if block_type == "block":
                                        self.log("ERR", f"🚫 BLOCK DETECTED on page {page_num}: 'Uso indebido/Bloqueado'.")
                                        mark_current_profile_blocked()
                                        raise BlockedException("Listing loop block detection: uso indebido")
                                        
                                    if block_type == "captcha":
                                        self.log("WARN", f"⚠️ CAPTCHA/LIMIT DETECTED on page {page_num}. Intentando resolver antes de rotar...")
                                        try:
                                            solved = await asyncio.wait_for(
                                                solve_captcha_advanced(page, logger=self.log, use_proxy=use_proxy),
                                                timeout=180.0
                                            )
                                        except asyncio.TimeoutError:
                                            solved = False
                                            self.log("WARN", "Captcha solver timeout (180s) en página de listado.")
                                        except Exception as _ce:
                                            solved = False
                                            self.log("WARN", f"Captcha solver error en página de listado: {_ce}")
                                        if solved:
                                            self.log("OK", "✅ Captcha resuelto en página de listado. Reintentando extracción de links...")
                                            # Recargar hrefs desde la misma página ya desbloqueada
                                            hrefs = await page.evaluate("""() => {
                                                return Array.from(document.querySelectorAll('a[href*="/inmueble/"]')).map(a => a.href);
                                            }""")
                                            # Continuar con los hrefs obtenidos (pueden estar vacíos si la página no se cargó bien)
                                        else:
                                            self.log("ERR", "Captcha no resuelto en página de listado. Rotando identidad...")
                                            raise BlockedException("Listing loop: captcha no resuelto tras intento de solver")

                                    # 2. Detailed info for logs if not blocked
                                    page_title = await page.title()
                                    self.log("INFO", f"Current URL: {page.url}")
                                    self.log("INFO", f"Page title: {page_title}")
                                    
                                    link_count = await page.evaluate("() => document.querySelectorAll('a').length")
                                    article_count = await page.evaluate("() => document.querySelectorAll('article, .item, .item-link').length")
                                    
                                    self.log("INFO", f"Total links on page: {link_count}")
                                    self.log("INFO", f"Article elements: {article_count}")
                                    
                                    # If no links/articles, it's a silent block or empty results
                                    if link_count == 0 or article_count == 0:
                                        self.log("WARN", "Zero property links found and no obvious block text. Checking specifically for 'Uso indebido'...")
                                        # One last check on raw text
                                        raw_text = await page.evaluate("() => document.documentElement.innerText")
                                        if "uso indebido" in raw_text.lower() or "bloqueado" in raw_text.lower():
                                            self.log("ERR", "🚫 BLOCK confirmed after deep text check.")
                                            mark_current_profile_blocked()
                                            raise BlockedException("Deep block detection")
                                        
                                        self.log("WARN", "Keeping browser open 30s for manual inspection...")
                                        await asyncio.sleep(30)
                                
                                except BlockedException as be:
                                    self.log("ERR", f"🛑 BLOCK in loop: {be}")
                                    # self._stop_evt.set()  <-- REMOVED: Rogue stop trigger. We want ROTATION, not termination.
                                    self.dual_mode_url = None 
                                    raise be
                                
                                except Exception as debug_e:
                                    self.log("ERR", f"Debug check failed: {debug_e}")
                    
                            # Still no links after debug - determine if we should exit or rotate
                            if not hrefs:
                                # ONLY exit if we've reached the expected total pages
                                if self.total_pages_expected > 0 and page_num >= self.total_pages_expected:
                                    self.log("INFO", f"End of listings reached at page {page_num}.")
                                    break
                                else:
                                    # If no links but not at the end, it's a silent block
                                    body_snippet = (await page.evaluate("() => document.body ? document.body.innerText : ''"))[:200].replace('\n', ' ')
                                    self.log("ERR", f"Zero property links found on page {page_num} (Expected {self.total_pages_expected} pages).")
                                    self.log("DEBUG", f"Page Context: {body_snippet}...")
                                    self.log("INFO", "Forcing identity ROTATION to bypass silent block.")
                                    mark_current_profile_blocked()
                                    raise BlockedException("Silent block: zero links on intermediate page")
                
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
                
                        # Scrape each property on this page
                        for href in hrefs:
                            # Update delays on every iteration to respect dynamic mode switching
                            _, card_delay, post_card_delay = self.get_delays()
                    
                            await self._wait_for_pause()
                            if self._should_stop:
                                break
                    
                            property_idx += 1
                            key = canonical_listing_url(href)
                            self._seen_in_search.add(key) # Mark as active (seen in search)
                            
                            # --- GLOBAL DEACTIVATION SKIP: Always skip properties already marked as inactive ---
                            orig_row = self._all_existing_urls.get(key, {})
                            if orig_row.get('is_inactive'):
                                self.log("INFO", f"({property_idx}/{self.total_properties_expected}) [SKIP] Already deactivated: {key}")
                                self._processed.add(key)
                                skipped += 1
                                self.current_property_count = property_idx
                                self.emit_progress()
                                
                                self.consecutive_skips += 1
                                if self.consecutive_skips >= 300:
                                    self.log("INFO", "[AUTO-STOP] 300 listados consecutivos saltados. Finalizando scrape actual para proteger proxy/ancho de banda.")
                                    scraping_finished = True
                                    break
                                continue
                                
                            # Smart Enrichment Optimization: Skip detail visit if already enriched & active
                            if self.smart_enrichment and key in self._enriched_urls and key not in self._processed:

                                self.log("INFO", f"({property_idx}/{self.total_properties_expected}) [SMART SKIP] Active & already enriched: {key}")
                                
                                from datetime import datetime
                                # Update last seen date
                                row_to_save = orig_row.get("full_row", {}).copy()
                                if not row_to_save:
                                    # Fallback if full_row is missing for some reason
                                    row_to_save = {"URL": key}
                                
                                row_to_save["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                row_to_save["Anuncio activo"] = "Sí"
                                row_to_save = mark_as_enriched(row_to_save)

                                additions.append(row_to_save)
                                self.scraped_properties.append(row_to_save)
                                self._processed.add(key)
                                smart_skipped += 1
                                
                                # Update profile efficacy stats (even if skipped, this profile found it in search)
                                self._profile_stats[self._active_profile_name] = self._profile_stats.get(self._active_profile_name, 0) + 1
                                
                                self.current_property_count = property_idx
                                self.emit_progress()
                                
                                # Check for stop even in smart skip
                                if self._should_stop:
                                    break
                                    
                                self.consecutive_skips += 1
                                if self.consecutive_skips >= 300:
                                    self.log("INFO", "[AUTO-STOP] 300 listados consecutivos saltados. Finalizando scrape actual para proteger proxy/ancho de banda.")
                                    scraping_finished = True
                                    break
                                continue
                    
                            # Double-check (should not happen after filtering, but safety net)
                            if key in self._processed:
                                self.log("INFO", f"({property_idx}/{self.total_properties_expected}) Skipping already scraped: {key}")
                                skipped_on_page += 1
                                skipped += 1
                                self.current_property_count = property_idx
                                self.emit_progress()
                                
                                self.consecutive_skips += 1
                                if self.consecutive_skips >= 300:
                                    self.log("INFO", "[AUTO-STOP] 300 listados consecutivos saltados. Finalizando scrape actual para proteger proxy/ancho de banda.")
                                    scraping_finished = True
                                    break
                                continue
                    
                            try:
                                await self._interruptible_sleep(random.uniform(*card_delay))
                                await self._goto_with_retry(page, href)
                                
                                # CRITICAL: Check for stop immediately after navigation
                                if self._should_stop:
                                    break
                        
                                # If this is the first property, determine target file
                                if target_file is None:
                                    await page.wait_for_timeout(PAGE_WAIT_MS)
                                    d = await extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode)
                                    if d.get("isBlocked"):
                                         self.log("ERR", "🚫 BLOCK detected on first property. Triggering rotation...")
                                         mark_current_profile_blocked()
                                         raise BlockedException("Acceso bloqueado en primera propiedad")
                                    row = {"URL": key, **d}
                            
                                    # Build target filename only if not already forced or detected from province
                                    # Priority: User selection > automatic detection
                                    if self.forced_target_file:
                                        target_file = self.forced_target_file
                                    else:
                                        ciudad = row.get("Ciudad") or self._detected_city
                                        category = self._detected_sheet or "unknown"
                                
                                        if ciudad:
                                            ciudad_clean = sanitize_filename_part(ciudad)
                                            target_file = f"idealista_{ciudad_clean}_{category}.xlsx"
                                        else:
                                            target_file = f"idealista_{category}.xlsx"
                                    # Update persistent reference
                                    self.output_file = target_file
                            
                                    target_path = os.path.join(self.output_dir, target_file)
                                    self.log("INFO", f"Target Excel file: {target_path}")
                            
                                    # Load existing URLs from this file
                                    # import time  <-- REMOVED to fix UnboundLocalError
                                    t_start_load = time.time()
                                    url_meta = load_all_urls_from_excel(target_path)
                                    # Copy metadata to state (no sobreescribir entradas ya presentes)
                                    for u, meta in url_meta.items():
                                        if u not in self._all_existing_urls:
                                            self._all_existing_urls[u] = meta
                                    t_end_load = time.time()
                                    self.log("INFO", f"Loaded {len(url_meta)} existing URLs from file in {t_end_load - t_start_load:.2f}s")
                            
                                    # CRITICAL FIX: Add existing URLs to processed set immediately
                                    # This prevents re-scraping subsequent properties in this loop that are already in the file
                                    if url_meta:
                                        self._processed.update(url_meta.keys())
                            
                                    # Process first property - check for missing fields (CAPTCHA)
                                    miss = missing_fields(row, is_room_mode=self._is_room_mode)
                                    if miss:
                                        self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA detectado en primera propiedad. Intentando resolución automática...")

                                        if self.on_status:
                                            self.on_status("captcha")

                                        # 1) Intentar resolución automática con solve_captcha_advanced
                                        try:
                                            solved = await asyncio.wait_for(
                                                solve_captcha_advanced(page, logger=self.log, use_proxy=True),
                                                timeout=180.0
                                            )
                                            if solved:
                                                self.log("OK", f"({property_idx}/{self.total_properties_expected}) ✅ CAPTCHA resuelto automáticamente en primera propiedad")
                                                # Re-extraer datos tras resolver
                                                try:
                                                    d = await asyncio.wait_for(extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode), timeout=20.0)
                                                    row = {"URL": key, **d}
                                                    miss = missing_fields(row, is_room_mode=self._is_room_mode)
                                                except Exception as re_ex:
                                                    self.log("WARN", f"Re-extracción tras auto-solve falló: {re_ex}")
                                        except asyncio.TimeoutError:
                                            self.log("WARN", f"({property_idx}/{self.total_properties_expected}) Auto-solver timeout (180s) en primera propiedad")
                                        except BlockedException:
                                            raise
                                        except Exception as solve_err:
                                            self.log("WARN", f"({property_idx}/{self.total_properties_expected}) Auto-solver error: {solve_err}")

                                        # 2) Si auto-solve no funcionó, espera pasiva 30s como fallback
                                        if miss:
                                            self.log("WARN", f"({property_idx}/{self.total_properties_expected}) Auto-solve insuficiente. Esperando 30s por resolución pasiva/manual...")
                                            for i in range(3): # 3 * 10s = 30s
                                                if self._should_stop: break
                                                self.log("INFO", f"Espera CAPTCHA pasiva ({i+1}/3)...")
                                                await asyncio.sleep(10.0)
                                                try:
                                                    d = await asyncio.wait_for(extract_detail_fields(page, debug_items=False, is_room_mode=self._is_room_mode), timeout=20.0)
                                                    if d.get("isBlocked"):
                                                         self.log("ERR", "🚫 Espera pasiva: página sigue BLOQUEADA (Uso indebido).")
                                                         mark_current_profile_blocked()
                                                         raise BlockedException("Acceso bloqueado persistente")

                                                    row = {"URL": key, **d}
                                                    if not missing_fields(row, is_room_mode=self._is_room_mode):
                                                         self.log("OK", "✅ CAPTCHA resuelto durante espera pasiva!")
                                                         miss = False
                                                         break
                                                except BlockedException:
                                                    raise
                                                except Exception:
                                                    pass

                                        if miss:
                                            self.log("ERR", "CAPTCHA_BLOCK_DETECTED tras auto-solve + espera pasiva")
                                            mark_profile_blocked(self.browser_engine)
                                            self.log("WARN", f"⏳ Profile '{self.browser_engine}' entering {PROFILE_COOLDOWN_MINUTES}-min cooldown.")
                                            try:
                                                 if len(additions) > self._last_checkpoint_idx and target_file:
                                                      t_start_save = time.time()
                                                      await self._save_checkpoint(additions, target_file, existing_df, set())
                                                      self.log("INFO", f"Saved captcha checkpoint in {time.time() - t_start_save:.2f}s")
                                            except Exception:
                                                pass
                                            raise Exception("CAPTCHA_BLOCK_DETECTED")
                                        
                                        # CAPTCHA cleared - resume normal operation
                                        if self.on_status: self.on_status("running")
                                    
                                        if self._should_stop:
                                            self.log("WARN", f"First property CAPTCHA - stopped by user: {key}")
                                            continue
                            
                                    # First property scraped successfully (or CAPTCHA cleared)
                                    # Add scraping date
                                    from datetime import datetime
                                    row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                    
                                    # Merge with existing data if available (Smart Enrichment)
                                    if key in self._all_existing_urls:
                                        existing_data = self._all_existing_urls[key].get('full_row', {})
                                        if existing_data:
                                            # Merge: new scraped data takes precedence for non-empty values, 
                                            # but existing data fills holes and preserves additional columns.
                                            for k, v in existing_data.items():
                                                if k not in row or row[k] in [None, "", "NaN", "nan"]:
                                                    row[k] = v

                                    # Marcar siempre como enriquecido tras scrape exitoso
                                    row = mark_as_enriched(row)

                                    additions.append(row)
                                    self.scraped_properties.append(row)
                                    self.consecutive_skips = 0
                                    new_scraped += 1
                                    
                                    # Update profile efficacy stats
                                    self._profile_stats[self._active_profile_name] = self._profile_stats.get(self._active_profile_name, 0) + 1
                                    
                                    self.log("OK", f"({property_idx}/{self.total_properties_expected}) Scraped: {key}")
                                    if self.on_property:
                                        self.on_property(row)
                            
                                    self._processed.add(key)
                                    self.current_property_count = property_idx
                                    self.emit_progress()
                                    continue
                        
                                # === URL nueva — enviar a la cola para scraping paralelo ===
                                self.consecutive_skips = 0
                                if _worker_tasks:
                                    # Workers activos: delegar scraping de detalle a la cola
                                    await _scrape_queue.put(href)
                                    self.log("INFO", f"({property_idx}/{self.total_properties_expected}) → Cola de scraping: {key}")
                                    self.current_property_count = property_idx
                                    self.emit_progress()
                                else:
                                    # Sin workers: browser visible scrapea directamente
                                    await self._interruptible_sleep(random.uniform(*card_delay))
                                    row = await self._scrape_property_detail(page, href)
                                    if row is not None:
                                        additions.append(row)
                                        self.scraped_properties.append(row)
                                        self._processed.add(key)
                                        new_scraped += 1
                                        self._profile_stats[self._active_profile_name] = self._profile_stats.get(self._active_profile_name, 0) + 1
                                        self._real_changes_for_checkpoint += 1
                                        if self._real_changes_for_checkpoint >= self._checkpoint_interval:
                                            self._real_changes_for_checkpoint = 0
                                            t_start_save = time.time()
                                            await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                                            self.log("INFO", f"Saved periodic checkpoint in {time.time() - t_start_save:.2f}s")
                                        await self.simulate_reading_time(row.get("Descripción"))
                                        await self.simulate_mouse_movement(page)
                                        self.log("OK", f"({property_idx}/{self.total_properties_expected}) Scraped: {key}")
                                        if self.on_property:
                                            self.on_property(row)
                                    else:
                                        self._processed.add(key)
                                    self.current_property_count = property_idx
                                    self.emit_progress()
                        
                            except BrowserClosedException:
                                # Save state for resume before exiting
                                self.save_state(page_num, target_file)
                                break
                            except (StopException, BlockedException):
                                # Propagate to top-level handler
                                raise
                            except Exception as e:
                                # CRITICAL: Check stop FIRST — if stop was requested, raise immediately
                                # This prevents the loop from continuing after _force_close_browser()
                                if self._should_stop:
                                    self.log("INFO", f"🛑 Stop detected during property processing: {key}")
                                    self.save_state(page_num, target_file)
                                    raise StopException("Stop requested during property processing")
                                if str(e) == "CAPTCHA_BLOCK_DETECTED":
                                    # Save state for resume before failing
                                    self.log("WARN", "Saving resume state due to CAPTCHA block")
                                    self.save_state(page_num, target_file)
                                    raise e
                                self.log("ERR", f"({property_idx}/{self.total_properties_expected}) {key} -> {e}")
                                self._processed.add(key)
                
                
                        # Check if we should continue to next page
                        if self._should_stop:
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
                        if self.total_pages_expected > 0 and page_num >= self.total_pages_expected and page_num < 60:
                            self.log("INFO", f"Reached expected page limit ({self.total_pages_expected} pages). Finishing scrape.")
                            self.clear_state()
                            scraping_finished = True
                            break

                        # Case 4: Max pages reached (hard limit / Deep Scrape Transition)
                        if page_num >= 60:
                            sort_options = [
                                "ordenado-por=precios-asc",
                                "ordenado-por=fecha-publicacion-desc",
                                "ordenado-por=fecha-publicacion-asc",
                                "ordenado-por=rebajas-desc",
                                "ordenado-por=precio-metro-cuadrado-asc",
                                "ordenado-por=precio-metro-cuadrado-desc",
                                "ordenado-por=planta-desc",
                                "ordenado-por=planta-asc"
                            ]
                            if not hasattr(self, "_current_sort_idx"):
                                self._current_sort_idx = -1
                                
                            self._current_sort_idx += 1
                            if self._current_sort_idx < len(sort_options):
                                next_sort = sort_options[self._current_sort_idx]
                                self.log("INFO", f"Max pages reached (60). Deep Scrape Mode: Switching to '{next_sort}'...")
                                
                                import urllib.parse
                                parsed = urllib.parse.urlsplit(self.seed_url)
                                # Replace query to avoid duplicate sort keys
                                current_base = urllib.parse.urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", parsed.fragment))
                                self.seed_url = f"{current_base}?{next_sort}"

                                page_num = 1
                                self._pages_scraped = 0
                                property_idx = 0  # Reset counter for new sort order
                                self.current_property_count = 0
                                self.total_pages_expected = 60 # Allow another 60 (Deep Scrape)
                                self._force_navigate = True  # Force navigation to page 1 of new sort
                                continue
                            else:
                                self.log("INFO", f"All Deep Scrape sorts exhausted at max pages. Finishing scrape.")
                                self.clear_state()
                                scraping_finished = True
                                break

                        # Default: Next page
                        page_num += 1
                        self._pages_scraped += 1
                
                    # After phase 1 loop completes
                    # === Cerrar cola y detener workers si se paró manualmente ===
                    if self._should_stop and _worker_tasks:
                        _scrape_queue.close()
                        self.log("INFO", f"⏹ Stop detectado. Cerrando cola y esperando {len(_worker_tasks)} workers...")
                        await asyncio.gather(*_worker_tasks, return_exceptions=True)
                        for _ctx, _name in [(_worker_webkit_ctx, "WebKit"), (_worker_opera_ctx, "Opera")]:
                            if _ctx:
                                try:
                                    await _ctx.close()
                                except Exception:
                                    pass

                    # === DRAIN SCRAPING QUEUE + WAIT FOR WORKERS ===
                    if scraping_finished and not self._should_stop:
                        # Cerrar la cola: no se añadirán más URLs
                        _scrape_queue.close()

                        # Browser visible ayuda a drenar URLs pendientes en la cola
                        _main_drain_count = 0
                        while not self._should_stop:
                            _drain_url = await _scrape_queue.claim()
                            if _drain_url is None:
                                break
                            _drain_key = canonical_listing_url(_drain_url)
                            if _drain_key in self._processed:
                                continue
                            try:
                                _, card_delay, _ = self.get_delays()
                                await self._interruptible_sleep(random.uniform(*card_delay))
                                row = await self._scrape_property_detail(page, _drain_url)
                                if row is not None:
                                    additions.append(row)
                                    self.scraped_properties.append(row)
                                    self._processed.add(_drain_key)
                                    new_scraped += 1
                                    _main_drain_count += 1
                                    self._real_changes_for_checkpoint += 1
                                    if self._real_changes_for_checkpoint >= self._checkpoint_interval:
                                        self._real_changes_for_checkpoint = 0
                                        t_save = time.time()
                                        await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                                        self.log("INFO", f"Saved periodic checkpoint in {time.time() - t_save:.2f}s")
                                    await self.simulate_reading_time(row.get("Descripción"))
                                    await self.simulate_mouse_movement(page)
                                    self.log("OK", f"(drain) Scraped: {_drain_key}")
                                    if self.on_property:
                                        self.on_property(row)
                                else:
                                    self._processed.add(_drain_key)
                            except (BlockedException, BrowserClosedException):
                                await _scrape_queue.release(_drain_url)
                                break
                            except StopException:
                                break
                            except Exception as e:
                                if "CAPTCHA_BLOCK_DETECTED" in str(e):
                                    await _scrape_queue.release(_drain_url)
                                    break
                                self.log("ERR", f"Error draining {_drain_url}: {e}")
                                self._processed.add(_drain_key)

                        # Esperar a que los workers headless terminen la cola de Phase 1
                        if _worker_tasks:
                            self.log("INFO", f"⏳ Esperando que {len(_worker_tasks)} workers headless completen sus URLs de Phase 1...")
                            await asyncio.gather(*_worker_tasks, return_exceptions=True)
                            if _worker_counters["scraped"] > 0 or _worker_counters["real_changes"] > 0:
                                self.log("OK", f"✅ Workers scrapearon {_worker_counters['real_changes']} propiedades en Phase 1.")
                            # Limpiar tasks completados para reutilizar workers en Phase 2
                            _worker_tasks.clear()

                        if _main_drain_count > 0:
                            self.log("INFO", f"Browser visible scrapeó {_main_drain_count} propiedades adicionales de la cola.")

                        self._in_enrichment = True

                        # Compute missing_urls: use saved list if resuming, otherwise compute fresh
                        if self._enrichment_missing_urls:
                            missing_urls = [u for u in self._enrichment_missing_urls
                                            if u not in self._enrichment_done_urls]
                            self.log("INFO", f"🔄 Reanudando enrichment: {len(missing_urls)} URLs restantes "
                                             f"({len(self._enrichment_done_urls)} ya verificadas)")
                        else:
                            # Fresh enrichment run — exclude URLs already handled by early workers
                            missing_urls = [
                                u for u in self._all_existing_urls.keys()
                                if u not in self._seen_in_search
                                and u not in self._enrichment_done_urls
                            ]
                            self._enrichment_missing_urls = list(missing_urls)

                        counters = {"checked": 0, "real_changes": 0, "last_checkpoint_real_changes": 0}
                        if missing_urls:
                            self.log("INFO", f"🔍 Found {len(missing_urls)} properties in Excel missing from search. Verifying deactivations (all)...")
                            self.log("INFO", f"🔄 Nueva cola de enrichment creada con {len(missing_urls)} URLs (Phase 1 encontró {len(self._seen_in_search)}, ya enriquecidas: {len(self._enrichment_done_urls)}).")

                            checkpoint_lock = asyncio.Lock()
                            parallel_stop_evt = asyncio.Event()
                            url_queue = SharedURLQueue(missing_urls)

                            async def _enrich_worker(worker_page, worker_label: str, is_secondary: bool = False, use_proxy: bool = True, proxy_label: str = "", proxy_config=None):
                                nonlocal deactivated_count
                                while not self._should_stop and not parallel_stop_evt.is_set():
                                    m_url = await url_queue.claim()
                                    if m_url is None:
                                        break

                                    # Skip if already confirmed inactive in a previous check
                                    orig_row = self._all_existing_urls.get(m_url, {})
                                    if orig_row.get("Anuncio activo") == "No":
                                        self._enrichment_done_urls.add(m_url)
                                        continue

                                    self.log("INFO", f"[{worker_label}] Checking missing property status ({counters['checked']+1}/{len(missing_urls)}): {m_url}")
                                    try:
                                        await self._interruptible_sleep(random.uniform(5, 10))
                                        await self._goto_with_retry(worker_page, m_url, use_proxy=use_proxy, label=worker_label, proxy_label=proxy_label, proxy_config=proxy_config)

                                        try:
                                            await worker_page.wait_for_selector("body", timeout=5000)
                                        except: pass

                                        body_text = await worker_page.evaluate("() => document.body ? document.body.innerText : ''")
                                        body_lower = body_text.lower()

                                        # CRITICAL: Verify we are NOT looking at a block page
                                        if any(kw in body_lower for kw in ["uso indebido", "bloqueado", "acceso bloqueado", "forbidden"]):
                                            self.log("ERR", f"🚫 [{worker_label}] INTERNAL BLOCK detected during enrichment for {m_url}")
                                            if not is_secondary:
                                                # El worker principal se bloquea: para todo y rota identidad
                                                parallel_stop_evt.set()
                                                mark_current_profile_blocked()
                                                raise BlockedException("Block detected during enrichment check")
                                            else:
                                                # Worker secundario bloqueado: devuelve la URL y sale sin interrumpir al principal
                                                self.log("WARN", f"[{worker_label}] Bloqueado. Devolviendo URL a la cola para que otro worker la procese.")
                                                await url_queue.release(m_url)
                                                break

                                        is_gone = any(msg in body_lower for msg in [
                                            "no encontramos", "anuncio no disponible",
                                            "ya no está disponible", "ya no está publicado",
                                            "lo sentimos", "enlace antiguo"
                                        ])

                                        was_active = orig_row.get("full_row", {}).get("Anuncio activo", "Sí") == "Sí"
                                        if is_gone:
                                            self.log("WARN", f"[{worker_label}] Confirmed: Property deactivated -> {m_url}")
                                            row_to_save = orig_row.get("full_row", {}).copy()
                                            if not row_to_save:
                                                row_to_save = {"URL": m_url}
                                            row_to_save["Anuncio activo"] = "No"
                                            from datetime import datetime
                                            row_to_save["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                            row_to_save["Baja anuncio"] = datetime.now().strftime("%d/%m/%Y")
                                            additions.append(row_to_save)
                                            self._processed.add(m_url)
                                            deactivated_count += 1
                                            counters["real_changes"] += 1  # baja = cambio real
                                        else:
                                            self.log("INFO", f"[{worker_label}] Property still active (not in search): {m_url}")
                                            row_to_save = orig_row.get("full_row", {}).copy()
                                            if not row_to_save:
                                                row_to_save = {"URL": m_url}
                                            row_to_save["Anuncio activo"] = "Sí"
                                            from datetime import datetime
                                            row_to_save["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                            row_to_save = mark_as_enriched(row_to_save)
                                            additions.append(row_to_save)
                                            if not was_active:
                                                counters["real_changes"] += 1  # reactivación = cambio real
                                            # Si ya estaba activa: solo confirmación, no cuenta como cambio

                                        # Mark this URL as done so restarts can skip it
                                        self._enrichment_done_urls.add(m_url)
                                        counters["checked"] += 1

                                        # Checkpoint: solo cuando hay 20 cambios reales (bajas o reactivaciones)
                                        real_changes_since_last = counters["real_changes"] - counters["last_checkpoint_real_changes"]
                                        if real_changes_since_last >= self._checkpoint_interval:
                                            async with checkpoint_lock:
                                                # Re-verificar bajo el lock por si otro worker ya guardó
                                                real_changes_since_last = counters["real_changes"] - counters["last_checkpoint_real_changes"]
                                                if real_changes_since_last >= self._checkpoint_interval:
                                                    t_start_save = time.time()
                                                    await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                                                    counters["last_checkpoint_real_changes"] = counters["real_changes"]
                                                    self.log("INFO", f"Saved periodic checkpoint in {time.time() - t_start_save:.2f}s")

                                    except (BlockedException, BrowserClosedException):
                                        if not is_secondary:
                                            # El worker principal se bloquea: para a los secundarios y rota identidad
                                            parallel_stop_evt.set()
                                            raise
                                        else:
                                            # Worker secundario bloqueado: devuelve la URL en vuelo y sale sin afectar al principal
                                            self.log("WARN", f"[{worker_label}] Bloqueado/cerrado inesperadamente. Devolviendo URL a la cola y saliendo.")
                                            await url_queue.release(m_url)
                                            break
                                    except StopException:
                                        self.log("INFO", f"[{worker_label}] Stop requested during missing property checks. Saving progress...")
                                        break
                                    except Exception as e:
                                        self.log("WARN", f"[{worker_label}] Could not verify {m_url}: {e}")
                                        continue

                            if not self.parallel_enrichment:
                                # Sequential mode (existing behavior, no secondary browser)
                                await _enrich_worker(page, "main")
                            else:
                                # Parallel mode: reutilizar workers headless de Phase 1
                                self.log("INFO", "🔀 Iniciando Phase 2 paralela: reutilizando workers de Phase 1")

                                try:
                                    # Construir lista de workers: siempre el main, secundarios según disponibilidad
                                    worker_coros = [_enrich_worker(page, "main")]
                                    if _worker_webkit_page is not None:
                                        worker_coros.append(_enrich_worker(_worker_webkit_page, "chromium-w2", is_secondary=True, use_proxy=True, proxy_label=f"[{_w2_label}]", proxy_config=_proxy_pool[1] if len(_proxy_pool) > 1 else None))
                                        self.log("INFO", f"✅ [{_w2_label}] Chromium-w2 worker reutilizado para Phase 2")
                                    if _worker_opera_page is not None:
                                        worker_coros.append(_enrich_worker(_worker_opera_page, "opera", is_secondary=True, use_proxy=True, proxy_label=f"[{_w3_label}]", proxy_config=_proxy_pool[2] if len(_proxy_pool) > 2 else None))
                                        self.log("INFO", f"✅ [{_w3_label}] Opera worker reutilizado para Phase 2")

                                    if len(worker_coros) > 1:
                                        results = await asyncio.gather(*worker_coros, return_exceptions=True)
                                        # Propagar excepciones criticas del worker principal (primer resultado)
                                        main_exc = results[0]
                                        if isinstance(main_exc, (BlockedException, BrowserClosedException, StopException)):
                                            raise main_exc
                                    else:
                                        # Fallback secuencial si ningún worker secundario arrancó
                                        await _enrich_worker(page, "main")
                                finally:
                                    pass  # Workers se cierran al final, fuera de este bloque

                            if counters["checked"] > 0:
                                self.log("OK", f"Finished checking {counters['checked']} missing properties.")

                        # Enrichment phase complete — clear resume state
                        self._in_enrichment = False
                        self._enrichment_done_urls.clear()
                        self._enrichment_missing_urls.clear()

                    # === Cerrar workers headless (después de Phase 1 + Phase 2) ===
                    for _ctx, _name in [(_worker_webkit_ctx, "WebKit"), (_worker_opera_ctx, "Opera")]:
                        if _ctx:
                            try:
                                await _ctx.close()
                                self.log("INFO", f"✅ {_name} worker cerrado")
                            except Exception as e:
                                self.log("WARN", f"Error cerrando {_name} worker: {e}")

                    self.log("INFO", f"Summary: {new_scraped} new, {deactivated_count} deactivated, {smart_skipped} smart-skipped, {skipped} regular-skipped")
                    self._export_to_excel(additions, target_file, expired_urls)

                    # CRITICAL FIX: If we finished cleanly (last page or max page), STOP the outer recovery loop
                    if scraping_finished and not self._should_stop:
                        self.log("INFO", "Scraping completed successfully. Exiting.")
                        break
                
                    
                    # === DUAL MODE: Run second phase in same browser ===
                    if self.dual_mode_url and not self._should_stop:
                        self.log("INFO", "=== DUAL MODE: Starting second phase in same browser ===")
                        self.log("INFO", f"Switching to: {self.dual_mode_url}")
                    
                        # Cooldown period to appear more human-like
                        cooldown = random.randint(30, 60)
                        self.log("INFO", f"Cooldown pause: {cooldown} seconds before continuing...")
                    
                        for _ in range(cooldown):
                            if self._should_stop:
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
                                url_meta = load_urls_with_dates(target_path)
                                self._processed.update(url_meta.keys())
                                self.log("INFO", f"Pre-loaded {len(url_dates)} existing URLs for phase 2")
                    
                        # Re-run the main scraping loop for phase 2
                        page_num = 1
                        new_scraped = 0
                        updated = 0
                        skipped = 0
                    
                        while not self._should_stop:
                            await self._wait_for_pause()
                            if self._should_stop:
                                break
                        
                            list_url = build_paginated_url(self.seed_url, page_num)
                            self.log("INFO", f"{PROXY_LABEL} Opening listing page {page_num}/{self.total_pages_expected}: {list_url}")
                        
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
                                if self._should_stop:
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
                                        mark_current_profile_blocked()
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
                                    
                                    # Update profile efficacy stats
                                    self._profile_stats[self._active_profile_name] = self._profile_stats.get(self._active_profile_name, 0) + 1
                                
                                    t_start_read = time.time()
                                    await self.simulate_reading_time(row.get("Descripción"))
                                    self.log("INFO", f"Simulated reading time: {time.time() - t_start_read:.2f}s")
                                    await self.simulate_mouse_movement(page)
                                
                                except BrowserClosedException:
                                    break
                                except (StopException, BlockedException):
                                    raise
                                except Exception as e:
                                    self.log("ERR", f"({self.current_property_count}/{self.total_properties_expected}) {key} -> {e}")
                                    self._processed.add(key)
                        
                            if self._should_stop:
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
                self.log("ERR", "🛑 HARD BLOCK: Scraper blocked by Idealista (Uso Indebido).")
                self.save_state(self.current_page or 1, target_file)
                self.log("WARN", "Rotating identity to continue...")
                self.handle_blocked_profile()
                
                # ROTATION LOGIC (2026): Strict Sequential with Cooldown
                mark_current_profile_blocked()

                # This function calculates next profile
                next_config, wait_time = rotate_identity()

                # Circuit breaker: si hay muchos t=bv acumulados, pausar aquí
                try:
                    from idealista_scraper.config import TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN
                except ImportError:
                    TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN = 8, 30
                _cb_count = get_tbv_count()
                if _cb_count >= TBV_CIRCUIT_BREAKER_THRESHOLD:
                    reset_tbv_counter()
                    self.log("WARN", f"🚨 CIRCUIT BREAKER [{_cb_count} t=bv]: pausa de {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min para enfriar IPs...")
                    if self.on_status:
                        self.on_status("blocked", error=f"Circuit breaker: pausa {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min...")
                    try:
                        await self._interruptible_sleep(TBV_CIRCUIT_BREAKER_PAUSE_MIN * 60)
                    except StopException:
                        break
                    self.log("INFO", "Circuit breaker expirado. Reanudando con nueva identidad...")
                    wait_time = 0

                self.log("WARN", f"🔄 ROLLING OVER to Profile {next_config['index']} ({next_config['name']})...")
                if wait_time > 0:
                    self.log("INFO", f"⏳ Profile is in cooldown ({int(wait_time)}s). Waiting...")
                self.log("INFO", f"Restarting in {int(wait_time) + 5} seconds with fresh identity...")
                wait_time += 5.0

                if self.on_status:
                    self.on_status("blocked", error=f"Bloqueado. Rotando a Perfil {next_config['index']}...")

                # Close browser and delete profile dir immediately
                try:
                    if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                        mouse_jitter_task.cancel()
                    if browser:
                        await browser.close()
                    elif ctx:
                        await ctx.close()
                except:
                    pass

                try:
                    self.log("INFO", f"🧹 Eliminando directorio de perfil {profile_index}...")
                    cleanup_stealth_profiles(index=profile_index)
                except Exception:
                    pass

                # Wait cooldown
                try:
                    await self._interruptible_sleep(wait_time)
                except StopException:
                    self.log("INFO", "Rollover wait cancelled by stop event.")
                    break

                if self._should_stop:
                    break

                self.log("INFO", "🔄 Restarting browser now...")
                continue # Loop back to start (and reuse persistent profile handling which will be fresh)

            except (StopException, BrowserClosedException):
                self.log("INFO", "🛑 Scraper stopped manually via user request or browser closed.")
                # State already saved in stop() method, but redundant call here ensures it's fresh
                if target_file:
                    self.save_state(self.current_page or 1, target_file)
                break

            except Exception as e:

                # Legacy CAPTCHA catch
                # NOTE: Most are now caught by BlockedException above
                err_str = str(e).upper()
                if "CAPTCHA" in err_str:
                    self.log("WARN", "⚠️ Se ha detectado un bloqueo por CAPTCHA durante el scraping.")
                    
                    # ROTATION LOGIC (2026): Strict Sequential with Cooldown
                    # Capture profile index BEFORE rotate_identity() changes the current profile
                    current_profile_idx = profile_index
                    mark_current_profile_blocked()
                    next_config, wait_time = rotate_identity()

                    # Circuit breaker: si hay muchos t=bv acumulados, pausar aquí
                    try:
                        from idealista_scraper.config import TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN
                    except ImportError:
                        TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN = 8, 30
                    _cb_count = get_tbv_count()
                    if _cb_count >= TBV_CIRCUIT_BREAKER_THRESHOLD:
                        reset_tbv_counter()
                        self.log("WARN", f"🚨 CIRCUIT BREAKER [{_cb_count} t=bv]: pausa de {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min para enfriar IPs...")
                        if self.on_status:
                            self.on_status("blocked", error=f"Circuit breaker: pausa {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min...")
                        try:
                            await self._interruptible_sleep(TBV_CIRCUIT_BREAKER_PAUSE_MIN * 60)
                        except StopException:
                            break
                        self.log("INFO", "Circuit breaker expirado. Reanudando con nueva identidad...")
                        wait_time = 0

                    self.log("WARN", f"🔄 ROLLING OVER to Profile {next_config['index']} ({next_config['name']})...")
                    if wait_time > 0:
                        self.log("INFO", f"⏳ Profile is in cooldown ({int(wait_time)}s). Waiting...")
                    self.log("INFO", f"Restarting in {int(wait_time) + 5} seconds with fresh identity...")
                    wait_time += 5.0

                    if self.on_status:
                        self.on_status("blocked", error=f"CAPTCHA. Rotando a Perfil {next_config['index']}...")

                    # Close browser and delete profile dir immediately
                    try:
                        if 'mouse_jitter_task' in dir() and mouse_jitter_task:
                            mouse_jitter_task.cancel()
                        if browser:
                            await browser.close()
                        elif ctx:
                            await ctx.close()

                        # GRANULAR CLEANUP: Erase the blocked profile's data immediately
                        self.log("INFO", f"🧼 Cleaning up blocked Profile {current_profile_idx}...")
                        cleanup_stealth_profiles(index=current_profile_idx)
                    except:
                        pass
                    
                    if target_file and self.current_page:
                        self.save_state(self.current_page, target_file)



                    if self.on_status:
                        self.on_status("blocked", message="Esperando 15 minutos para reintentar...")

                    self.log("OK", f"✅ Browser closed. Waiting {wait_time} seconds before restart...")
                    try:
                        # Wait for calculated duration (short for switch, long for cooldown)
                        cycles = max(1, int(wait_time / 5))
                        for _ in range(cycles): 
                            if self._should_stop: break
                            await asyncio.sleep(5)
                    except StopException:
                        self.log("INFO", "Retry wait interrupted.")
                        break
                    
                    if self._should_stop:
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

            # Delete profile directory immediately after browser closes
            try:
                cleanup_stealth_profiles(index=profile_index)
            except Exception:
                pass
        
        # Log profile efficacy report
        total_scraped = len(self.scraped_properties)
        if total_scraped > 0:
            self.log("INFO", "📊 RESUMEN DE EFICACIA POR PERFIL:")
            for profile, count in self._profile_stats.items():
                percentage = (count / total_scraped) * 100
                self.log("INFO", f"🔹 {profile} = {count} propiedades ({percentage:.1f}% del total)")

        # Log captcha resolution report
        captcha_stats = get_captcha_stats()
        # Determine methods that were actually encountered
        all_methods = [
            ("Recarga rápida",       None),
            ("DataDome 2Captcha",    ["ip_bloqueada", "interstitial", "timeout", "error_api", "irresoluble", "sin_cookie", "error_inyeccion", "cookie_rechazada"]),
            ("DataDome CapSolver",   ["ip_bloqueada", "interstitial", "timeout", "error_api", "sin_cookie", "error_inyeccion", "cookie_rechazada"]),
            ("Slider local",         None),
            ("2Captcha GeeTest",     None),
            ("2Captcha Coordenadas", None),
        ]
        active_methods = [m for m, _ in all_methods if captcha_stats.get(f"{m}|intentos", 0) > 0]
        if active_methods:
            total_resueltos = sum(captcha_stats.get(f"{m}|resueltos", 0) for m, _ in all_methods)
            total_intentos  = sum(captcha_stats.get(f"{m}|intentos",  0) for m, _ in all_methods)
            self.log("INFO", "─" * 60)
            self.log("INFO", f"🧩 RESUMEN DE RESOLUCIÓN DE CAPTCHAS  |  {total_resueltos}/{total_intentos} resueltos")
            self.log("INFO", "─" * 60)
            for method, fail_keys in all_methods:
                intentos  = captcha_stats.get(f"{method}|intentos",  0)
                if intentos == 0:
                    continue
                resueltos = captcha_stats.get(f"{method}|resueltos", 0)
                pct       = (resueltos / intentos * 100) if intentos else 0
                label     = "sin coste" if method == "Recarga rápida" else "de pago"
                line      = f"🔹 {method:<22} {intentos:>3} intentos | {resueltos:>3} resueltos ({pct:5.1f}%) [{label}]"
                if fail_keys:
                    fallos = []
                    for k in fail_keys:
                        v = captcha_stats.get(f"{method}|{k}", 0)
                        if v:
                            fallos.append(f"{k}={v}")
                    if fallos:
                        line += "  →  " + "  ".join(fallos)
                self.log("INFO", line)
            self.log("INFO", "─" * 60)
        else:
            self.log("INFO", "🧩 No se encontraron captchas durante el scraping.")
        
        # Clear resume state file ONLY on successful completion (not manual stop)
        if not self._should_stop:
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
        
        # Free up space by removing cached browser profiles
        try:
            cleanup_stealth_profiles()
        except:
            pass
        
        if self.on_status:
            self.on_status(self.status, file=self.output_file, count=len(self.scraped_properties))

