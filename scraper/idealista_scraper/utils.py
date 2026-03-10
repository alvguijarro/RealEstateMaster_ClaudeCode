"""Utility functions for the Idealista scraper.

This module provides helper functions for text processing, URL canonicalization,
logging, and data normalization used throughout the scraper.
"""
from __future__ import annotations

import re
import time
import unicodedata
from functools import lru_cache
from typing import Optional, Tuple
from urllib.parse import urlparse

import asyncio
import os
import sys
import tempfile
from pathlib import Path
# Add project root to sys.path to import shared config
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
try:
    from shared.config import TWOCAPTCHA_API_KEY, CAPSOLVER_API_KEY
except ImportError:
    TWOCAPTCHA_API_KEY = None
    CAPSOLVER_API_KEY = ""

try:
    from shared.proxy_config import get_2captcha_proxy_dict
except ImportError:
    get_2captcha_proxy_dict = lambda: {}

# Fallback: ensure key is always available even if config import failed
if not TWOCAPTCHA_API_KEY:
    TWOCAPTCHA_API_KEY = os.environ.get("TWOCAPTCHA_API_KEY", "f49b4e9ed2e2b36add9c6ef3af3e6e4c")

# Sync solver (TwoCaptcha) — used for screenshots/coordinates
try:
    from twocaptcha import TwoCaptcha
    # Timeout corto (60s) para fallar rápido si el worker tarda demasiado en coordenadas
    SOLVER = TwoCaptcha(TWOCAPTCHA_API_KEY, defaultTimeout=60, pollingInterval=5) if TWOCAPTCHA_API_KEY else None
except Exception as _e:
    TwoCaptcha = None
    SOLVER = None

# Async solver (AsyncTwoCaptcha) — used for DataDome / GeeTest
try:
    from twocaptcha.async_solver import AsyncTwoCaptcha
    # Timeout de 90s para DataDome: si tarda más, el challenge de Idealista habrá caducado ("REINTENTAR")
    ASYNC_SOLVER = AsyncTwoCaptcha(TWOCAPTCHA_API_KEY, defaultTimeout=90, pollingInterval=5) if TWOCAPTCHA_API_KEY else None
except Exception as _e:
    AsyncTwoCaptcha = None
    ASYNC_SOLVER = None

# --- Startup diagnostics ---
if SOLVER:
    _key_tail = TWOCAPTCHA_API_KEY[-4:] if TWOCAPTCHA_API_KEY else '????'
    print(f"[2Captcha] ✅ SOLVER sync listo (key: ...{_key_tail})")
else:
    print("[2Captcha] ⚠️  SOLVER sync NO disponible (import fallida o key inválida)")

if ASYNC_SOLVER:
    _key_tail = TWOCAPTCHA_API_KEY[-4:] if TWOCAPTCHA_API_KEY else '????'
    print(f"[2Captcha] ✅ ASYNC_SOLVER listo (key: ...{_key_tail})")
else:
    print("[2Captcha] ⚠️  ASYNC_SOLVER NO disponible (import fallida o key inválida)")



# =============================================================================
# Captcha Stats
# =============================================================================

_captcha_stats: dict = {}

def reset_captcha_stats() -> None:
    """Reinicia el contador de captchas resueltos (llamar al inicio de cada sesión)."""
    global _captcha_stats
    _captcha_stats = {}

def get_captcha_stats() -> dict:
    """Devuelve una copia del contador actual de captchas resueltos por método."""
    return dict(_captcha_stats)

def _captcha_inc(key: str) -> None:
    """Incrementa el contador para la clave dada (formato 'Método|evento')."""
    _captcha_stats[key] = _captcha_stats.get(key, 0) + 1

# =============================================================================
# t=bv (IP bloqueada) Counter — detecta cuándo el pool de IPs está agotado
# =============================================================================

_TBV_STATE_FILE = Path(__file__).parent.parent / 'app' / 'tbv_state.json'
_last_solver_fail_reason: str = ''  # 'tbv' o '' — escrito por solver, leído por el bucle


def _increment_tbv_counter(ip: str = 'unknown') -> int:
    """Incrementa el contador global de t=bv consecutivos y persiste en JSON. Retorna el nuevo valor."""
    import json
    state: dict = {'consecutive': 0, 'last_ts': 0.0, 'last_ip': ''}
    try:
        if _TBV_STATE_FILE.exists():
            state = json.loads(_TBV_STATE_FILE.read_text())
    except Exception:
        pass
    state['consecutive'] = state.get('consecutive', 0) + 1
    state['last_ts'] = time.time()
    state['last_ip'] = ip
    try:
        _TBV_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TBV_STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass
    return state['consecutive']


def _get_tbv_count() -> int:
    """Lee el contador de t=bv consecutivos del archivo de estado."""
    import json
    try:
        if _TBV_STATE_FILE.exists():
            return json.loads(_TBV_STATE_FILE.read_text()).get('consecutive', 0)
    except Exception:
        pass
    return 0


def _reset_tbv_counter() -> None:
    """Resetea el contador de t=bv consecutivos (llamar cuando un solve tiene éxito)."""
    import json
    try:
        state = {'consecutive': 0, 'last_ts': time.time(), 'last_ip': ''}
        _TBV_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        _TBV_STATE_FILE.write_text(json.dumps(state))
    except Exception:
        pass

# =============================================================================
# Logging Functions
# =============================================================================

def log(kind: str, msg: str) -> None:
    """Simple timestamped logger with colored output for different log levels.
    
    Args:
        kind: Log level/kind - one of: DEBUG, INFO, OK, WARN, ERR
        msg: The message to log
    """
    import sys
    import os
    
    ts = time.strftime("%H:%M:%S")
    
    # Detect if we should use colors (terminal only)
    use_colors = sys.stdout.isatty() and os.environ.get("NO_COLOR") != "1"
    
    if use_colors:
        colors = {
            "DEBUG": "\033[36m",    # Cyan
            "INFO": "\033[37m",     # White
            "OK": "\033[32m",       # Green
            "WARN": "\033[33m",     # Yellow
            "ERR": "\033[31m",      # Red
        }
        color = colors.get(kind.upper(), "")
        reset = "\033[0m"
    else:
        color = ""
        reset = ""
    
    full_msg = f"[{ts}] [{kind.upper()}] {msg}"
    
    try:
        print(f"{color}{full_msg}{reset}")
    except UnicodeEncodeError:
        # Fallback for Windows consoles (cp1252)
        # Replace non-ascii chars like ≤, → with safe alternatives
        safe_msg = msg.replace("≤", "<=").replace("→", "->").replace("€", "E")
        # Final safety net: replace any other unicode with ?
        try:
            print(f"{color}[{ts}] [{kind.upper()}] {safe_msg}{reset}")
        except UnicodeEncodeError:
            final_msg = full_msg.encode('ascii', 'replace').decode('ascii')
            print(f"{final_msg}")


def play_captcha_alert():
    """Play a soft melodic bell to alert the user of a CAPTCHA."""
    try:
        import winsound
        # Play a gentle ascending chime (softer frequencies, longer tones)
        winsound.Beep(523, 300)   # C5 - 300ms
        winsound.Beep(659, 300)   # E5 - 300ms
        winsound.Beep(784, 400)   # G5 - 400ms (held longer)
    except ImportError:
        # Fallback for non-Windows or if winsound fails
        print("\a")  # ASCII Bell

def cleanup_stealth_profiles(index: Optional[int] = None):
    """Remove stealth_profile* directories to free up space.
    
    Args:
        index: If provided, only delete 'stealth_profile_{index}'.
               If None, delete all 'stealth_profile*' directories.
    """
    import shutil
    base_dir = Path(__file__).parent.parent.parent
    
    # Check root and scraper directory
    dirs_to_check = [base_dir, base_dir / 'scraper']
    
    target_pattern = f"stealth_profile_{index}" if index is not None else "stealth_profile"
    
    for d in dirs_to_check:
        if not d.exists(): continue
        for item in d.iterdir():
            if item.is_dir() and (item.name == target_pattern if index is not None else item.name.startswith('stealth_profile')):
                # Retry logic for Windows file locking issues
                for attempt in range(3):
                    try:
                        # log("INFO", f"Limpiando perfil residual: {item.name} (intento {attempt+1})")
                        shutil.rmtree(item, ignore_errors=True)
                        if not item.exists():
                            break
                        time.sleep(0.5)
                    except Exception as e:
                        if attempt == 2:
                            log("WARN", f"No se pudo borrar {item.name} tras 3 intentos: {e}")


def play_blocked_alert():
    """Play an alarming descending tone to indicate scraper has been blocked."""
    try:
        import winsound
        # Play an urgent descending alert (indicates failure/stop)
        winsound.Beep(880, 400)   # A5 - 400ms
        winsound.Beep(659, 400)   # E5 - 400ms
        winsound.Beep(440, 600)   # A4 - 600ms (held longer, lower)
    except ImportError:
        print("\a\a\a")  # Triple ASCII Bell


# =============================================================================

def fold_text(s: Optional[str]) -> str:
    """Normalize text by removing accents and converting to lowercase.
    
    This is used for case-insensitive and accent-insensitive text matching,
    particularly useful for Spanish text with accents.
    
    Args:
        s: Input string to normalize, can be None
        
    Returns:
        Normalized lowercase string without accents, empty string if input is None
        
    Example:
        >>> fold_text("Habitación")
        'habitacion'
        >>> fold_text("ÁTICO")
        'atico'
    """
    if s is None:
        return ""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)).lower()


def sanitize_units(s: Optional[str]) -> Optional[str]:
    """Normalize whitespace and special characters in measurement units.
    
    Converts various Unicode spaces to regular spaces and normalizes the string
    to ensure consistent formatting of measurements like "100 m²".
    
    Args:
        s: Input string with potential special whitespace characters
        
    Returns:
        Normalized string with regular spaces, or None if input is None/empty
        
    Example:
        >>> sanitize_units("100\u00a0m²")
        '100 m²'
    """
    if not s:
        return None
    t = unicodedata.normalize("NFKC", s)
    t = t.replace("\u00a0", " ").replace("\u202f", " ")
    # Remove emojis and other non-ASCII symbols (keep letters, numbers, spaces, basic punctuation)
    t = re.sub(r'[^\w\s.,;:°²³/\-+()€%]', '', t, flags=re.UNICODE)
    t = re.sub(r"\s+", " ", t).strip()
    return t if t else None


# =============================================================================
# URL Processing Functions
# =============================================================================

@lru_cache(maxsize=4096)
def same_domain(url: str) -> bool:
    """Check if a URL belongs to the idealista.com domain.
    
    This function is cached for performance since it's called frequently
    during link filtering.
    
    Args:
        url: The URL to check
        
    Returns:
        True if the URL is from idealista.com, False otherwise
        
    Example:
        >>> same_domain("https://www.idealista.com/inmueble/12345")
        True
        >>> same_domain("https://example.com")
        False
    """
    try:
        return urlparse(url).netloc.endswith("idealista.com")
    except Exception:
        return False


LISTING_URL_RE = re.compile(r"/inmueble[s]?/\d+", re.I)
"""Regex pattern to identify property listing URLs by their path structure."""


@lru_cache(maxsize=4096)
def canonical_listing_url(u: str) -> str:
    """Convert a listing URL to its canonical form.
    
    Removes language prefixes and normalizes the URL structure to ensure
    consistent deduplication of the same property across different URL variants.
    
    Args:
        u: The raw listing URL
        
    Returns:
        Canonical URL in the format: https://domain.com/inmueble/12345
        
    Example:
        >>> canonical_listing_url("https://www.idealista.com/en/inmueble/12345/")
        'https://www.idealista.com/inmueble/12345/'
    """
    m = re.search(r"(https?://[^/]+)/(?:[a-z]{2}/)?(inmueble[s]?/\d+/?)", u, flags=re.I)
    return f"{m.group(1)}/{m.group(2)}" if m else u


def is_listing_url(url: str) -> bool:
    """Check if a URL points to a property listing detail page.
    
    Args:
        url: The URL to check
        
    Returns:
        True if the URL is a property listing on idealista.com
        
    Example:
        >>> is_listing_url("https://www.idealista.com/inmueble/12345")
        True
        >>> is_listing_url("https://www.idealista.com/alquiler-viviendas/madrid/")
        False
    """
    if not same_domain(url):
        return False
    return LISTING_URL_RE.search(url) is not None


# =============================================================================
# Data Parsing & Extraction Functions
# =============================================================================

def split_location(text: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    """Split a location string into two parts at the first comma.
    
    Property locations on Idealista are often formatted as "Street, District"
    or "Building, Street". This function splits them for separate column storage.
    
    Args:
        text: Location text to split
        
    Returns:
        Tuple of (first_part, second_part), both can be None
    """
    if not text:
        return None, None
    parts = [p.strip() for p in str(text).split(",")]
    if len(parts) < 2:
        return text, None
    ubic = ", ".join(parts[:-1])
    prov = parts[-1]
    return ubic, prov



def normalize_price(v) -> Optional[int]:
    """Extract numeric value from a price string.
    
    Removes currency symbols, spaces, and other non-digit characters
    to extract the raw integer price.
    
    Args:
        v: Price value (can be string, int, or None)
        
    Returns:
        Integer price value or None if no digits found
        
    Example:
        >>> normalize_price("1.500 €")
        1500
        >>> normalize_price("€ 250,000")
        250000
    """
    if v is None:
        return None
    digits = re.sub(r"[^\d]", "", str(v))
    return int(digits) if digits else None


def parse_relative_date(text: Optional[str]) -> Optional[str]:
    """Convert relative date strings like 'hoy', 'ayer', 'anteayer' into DD/MM/YYYY.
    
    Args:
        text: Date string (relative or absolute)
        
    Returns:
        Formatted date string as DD/MM/YYYY, or original text if not relative.
        
    Example:
        >>> parse_relative_date("ayer")  # If today is 26/02/2026
        '25/02/2026'
    """
    if not text:
        return None
        
    from datetime import datetime, timedelta
    t = text.lower().strip()
    now = datetime.now()
    
    if "hoy" in t:
        return now.strftime("%d/%m/%Y")
    elif "ayer" in t:
        return (now - timedelta(days=1)).strftime("%d/%m/%Y")
    elif "anteayer" in t:
        return (now - timedelta(days=2)).strftime("%d/%m/%Y")
    
    # If it's already a date like 05/01/2026, we might want to ensure DD/MM/YYYY
    # But for now, we leave it as is if it matches a date pattern
    if re.search(r"\d{1,2}/\d{1,2}/\d{4}", t):
        return text.strip()
        
    return text.strip()


def digits_only(s: Optional[str]) -> Optional[int]:
    """Extract only digits from a string and convert to integer.
    
    Args:
        s: Input string containing digits
        
    Returns:
        Integer value of extracted digits, or None if no digits found
        
    Example:
        >>> digits_only("3 bedrooms")
        3
        >>> digits_only("Built in 2015")
        2015
    """
    if not s:
        return None
    d = re.sub(r"[^\d]", "", str(s))
    return int(d) if d else None


def infer_tipo_from_title(title: Optional[str]) -> Optional[str]:
    """Infer the property type from the title text.
    
    Searches for common Spanish property type keywords in the title
    and returns the first match found.
    
    Args:
        title: Property title text
        
    Returns:
        Property type keyword if found, None otherwise
        
    Example:
        >>> infer_tipo_from_title("Piso en venta en Madrid")
        'piso'
        >>> infer_tipo_from_title("Chalet adosado con jardín")
        'chalet'
    """
    if not title:
        return None
    t = fold_text(title)
    for k in [
        "piso", "chalet", "casa", "atico", "ático", "duplex", "dúplex",
        "apartamento", "estudio", "adosado", "villa", "loft", "finca", "bungalow",
        "atico duplex", "ático dúplex"
    ]:
        if k in t:
            return k
    return None


def sanitize_filename_part(s: str) -> str:
    """Sanitize a string for safe use in filenames.
    
    Removes special characters and replaces spaces with underscores
    to create Windows-safe filenames.
    
    Args:
        s: String to sanitize (e.g., province name)
        
    Returns:
        Sanitized string safe for use in filenames
        
    Example:
        >>> sanitize_filename_part("Madrid Centro")
        'Madrid_Centro'
        >>> sanitize_filename_part("Toledo (Province)")
        'Toledo_Province'
    """
    s = s.strip()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s)
    return s or "idealista"

# =============================================================================
# Geography / Location Helpers
# =============================================================================

PROVINCE_TO_COMMUNITY = {
    "Araba": "País Vasco", "Alava": "País Vasco", "Álava": "País Vasco",
    "Albacete": "Castilla-La Mancha",
    "Alicante": "Comunidad Valenciana", "Alacant": "Comunidad Valenciana",
    "Almería": "Andalucía", "Almeria": "Andalucía",
    "Asturias": "Asturias", "Oviedo": "Asturias",
    "Ávila": "Castilla y León", "Avila": "Castilla y León",
    "Badajoz": "Extremadura",
    "Baleares": "Islas Baleares", "Illes Balears": "Islas Baleares", "Menorca": "Islas Baleares", "Mallorca": "Islas Baleares", "Ibiza": "Islas Baleares",
    "Barcelona": "Cataluña",
    "Burgos": "Castilla y León",
    "Cáceres": "Extremadura", "Caceres": "Extremadura",
    "Cádiz": "Andalucía", "Cadiz": "Andalucía",
    "Cantabria": "Cantabria", "Santander": "Cantabria",
    "Castellón": "Comunidad Valenciana", "Castello": "Comunidad Valenciana",
    "Ciudad Real": "Castilla-La Mancha",
    "Córdoba": "Andalucía", "Cordoba": "Andalucía",
    "Coruña": "Galicia", "A Coruña": "Galicia", "La Coruña": "Galicia",
    "Cuenca": "Castilla-La Mancha",
    "Girona": "Cataluña", "Gerona": "Cataluña",
    "Granada": "Andalucía",
    "Guadalajara": "Castilla-La Mancha",
    "Guipúzcoa": "País Vasco", "Gipuzkoa": "País Vasco",
    "Huelva": "Andalucía",
    "Huesca": "Aragón",
    "Jaén": "Andalucía", "Jaen": "Andalucía",
    "León": "Castilla y León", "Leon": "Castilla y León",
    "Lleida": "Cataluña", "Lérida": "Cataluña",
    "Lugo": "Galicia",
    "Madrid": "Comunidad de Madrid",
    "Málaga": "Andalucía", "Malaga": "Andalucía",
    "Murcia": "Región de Murcia",
    "Navarra": "Navarra",
    "Ourense": "Galicia", "Orense": "Galicia",
    "Palencia": "Castilla y León",
    "Las Palmas": "Canarias", "Palmas, Las": "Canarias",
    "Pontevedra": "Galicia",
    "La Rioja": "La Rioja", "Rioja": "La Rioja",
    "Salamanca": "Castilla y León",
    "Segovia": "Castilla y León",
    "Sevilla": "Andalucía",
    "Soria": "Castilla y León",
    "Tarragona": "Cataluña",
    "Santa Cruz de Tenerife": "Canarias", "Tenerife": "Canarias",
    "Teruel": "Aragón",
    "Toledo": "Castilla-La Mancha",
    "Valencia": "Comunidad Valenciana", "València": "Comunidad Valenciana",
    "Valladolid": "Castilla y León",
    "Vizcaya": "País Vasco", "Bizkaia": "País Vasco",
    "Zamora": "Castilla y León",
    "Zaragoza": "Aragón",
    "Ceuta": "Ceuta",
    "Melilla": "Melilla"
}

def get_comunidad(provincia: Optional[str]) -> Optional[str]:
    """Look up Comunidad Autónoma from Province name.
    
    Args:
        provincia: Name of the province (e.g. "Madrid", "Segovia")
        
    Returns:
        Name of the Autonomous Community or None if not found.
    """
    if not provincia:
        return None
        
    # 1. Direct lookup
    if provincia in PROVINCE_TO_COMMUNITY:
        return PROVINCE_TO_COMMUNITY[provincia]
        
    # 2. Case-insensitive lookup
    # Normalize input slightly for better matching
    p_clean = provincia.strip()
    for k, v in PROVINCE_TO_COMMUNITY.items():
        if k.lower() == p_clean.lower():
            return v
            
    # 3. Partial match (risky but useful for "Provincia de Madrid")
    p_lower = p_clean.lower()
    for k, v in PROVINCE_TO_COMMUNITY.items():
        if k.lower() in p_lower:
            return v
            
    return None


# =============================================================================
# Human Emulation / Stealth Functions
# =============================================================================

import random
import math

def _bezier_curve(p0, p1, p2, p3, t):
    """Cubic Bezier curve."""
    return (
        (1-t)**3 * p0 +
        3 * (1-t)**2 * t * p1 +
        3 * (1-t) * t**2 * p2 +
        t**3 * p3
    )

async def simulate_human_interaction(page):
    """Simulate human-like mouse movements and random scrolling."""
    try:
        # Get viewport size
        viewport = page.viewport_size or {"width": 1280, "height": 800}
        width, height = viewport["width"], viewport["height"]

        # 1. Random Mouse Move (Bezier Curve)
        # Start from current position or random
        start_x = random.randint(0, width)
        start_y = random.randint(0, height)
        
        # Target position (random element or point)
        end_x = random.randint(0, width)
        end_y = random.randint(0, height)
        
    # Control points for curved path
        cp1_x = random.randint(0, width)
        cp1_y = random.randint(0, height)
        cp2_x = random.randint(0, width)
        cp2_y = random.randint(0, height)
        
        steps = random.randint(20, 50)
        for i in range(steps):
             # 1. Abort if stop requested (needs to be passed or checked)
             # But this function doesn't take check_stop yet. We'll rely on timeout in wrapper for now.
             
            t = i / steps
            x = _bezier_curve(start_x, cp1_x, cp2_x, end_x, t)
            y = _bezier_curve(start_y, cp1_y, cp2_y, end_y, t)
            
            # Guard against closed page
            if page.is_closed():
                return
                
            await page.mouse.move(x, y)
            await asyncio.sleep(random.uniform(0.01, 0.03)) # Fast, realistic movement

        # 2. Random short pause
        await asyncio.sleep(random.uniform(0.5, 1.5))
        
    except Exception as e:
        # log("DEBUG", f"Interaction failed: {e}") # helpful debug
        pass # Fail silently to not interrupt scraper flow


async def solve_slider_captcha(page):
    """Automatically solve 'Slide to the Right' CAPTCHA with human-like dragging."""
    try:
        # Determine the target frame (DataDome puts everything inside an iframe)
        target_frame = page
        
        # Check for DataDome iframe
        iframe_element = await page.query_selector('iframe[src*="captcha-delivery.com"]')
        if iframe_element:
            frame = await iframe_element.content_frame()
            if frame:
                target_frame = frame
                
        # 1. Identify the slider handle
        selectors = [
            ".geetest_slider_button", ".nc_iconfont.btn_slide", "#nc_1_n1z", 
            ".slid_btn", ".captcha_slider", "div[role='button'][aria-label*='slider']",
            ".px-captcha-container .px-captcha-slider-button",
            "div[class*='slider-handle']", "div[class*='captcha-slider-handle']",
            ".arrow-right", "button[aria-label*='Slide']", "button[aria-label*='Desliza']"
        ]
        
        handle = None
        for sel in selectors:
            try:
                h = await target_frame.query_selector(sel)
                if h and await h.is_visible():
                    handle = h
                    break
            except: continue
        
        if not handle:
            # Try finding by icon or style if specific selector fails
            try:
                h = await target_frame.query_selector("span:has-text('→'), div[aria-label*='Slide to right'], div[aria-label*='Desliza hacia la derecha']")
                if h and await h.is_visible():
                    handle = h
            except: pass
            
        if not handle:
            return False

        # 2. Get bounding boxes
        box = await handle.bounding_box()
        if not box:
            return False
            
        # If the handle is inside an iframe, we must adjust coordinates relative to the page
        if target_frame != page and iframe_element:
            iframe_box = await iframe_element.bounding_box()
            if iframe_box:
                box['x'] += iframe_box['x']
                box['y'] += iframe_box['y']
        start_x = box['x'] + box['width'] / 2
        start_y = box['y'] + box['height'] / 2
        
        # Track length - usually around 250-300px, or we try to find the container
        container = await target_frame.query_selector(".geetest_slider, .nc-container, .captcha_track, [class*='track'], [class*='slider-track']")
        if container:
            cbox = await container.bounding_box()
            distance = cbox['width'] - box['width'] if cbox else 260
        else:
            if iframe_element:
                container = await target_frame.query_selector(".px-captcha-container, body")
                if container:
                    cbox = await container.bounding_box()
                    distance = cbox['width'] - box['width'] - 15 if cbox else 250
                else:
                    distance = 260 + random.randint(-5, 5)
            else:
                distance = 260 + random.randint(-10, 10)

        # 3. Perform human-like drag
        await page.mouse.move(start_x, start_y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await page.mouse.down()
        await asyncio.sleep(random.uniform(0.1, 0.3))
        
        current_x = start_x
        steps = random.randint(15, 25)
        
        for i in range(steps):
            # Non-linear speed (acceleration then deceleration)
            t = i / steps
            # Smooth step function
            move_x = distance * (math.sin((t * math.pi / 2))) 
            
            # Add slight vertical jitter (+/- 1-2px)
            jitter_y = start_y + random.uniform(-1, 1)
            
            await page.mouse.move(start_x + move_x, jitter_y)
            await asyncio.sleep(random.uniform(0.01, 0.04))
            
        # Small overshoot and correction (very human)
        overshoot = random.randint(2, 5)
        await page.mouse.move(start_x + distance + overshoot, start_y + random.uniform(-1, 1))
        await asyncio.sleep(random.uniform(0.1, 0.2))
        await page.mouse.move(start_x + distance, start_y)
        
        await asyncio.sleep(random.uniform(0.2, 0.5))
        await page.mouse.up()
        
        # 4. Verification
        await asyncio.sleep(2)
        return True
        
    except Exception as e:
        log("WARN", f"Slider solver attempt failed: {e}")
        return False

async def solve_geetest_2captcha(page, logger=None):
    """Solve GeeTest CAPTCHA using 2Captcha service."""
    l = logger or log
    if not ASYNC_SOLVER:
        l("WARN", "2Captcha ASYNC_SOLVER not initialized (check API Key).")
        return False
        
    try:
        _captcha_inc("geetest_attempts")
        l("INFO", "🌀 Detecting GeeTest parameters...")
        # Idealista usually puts GeeTest params in a specific script or object
        # We try to extract gt and challenge
        params = await page.evaluate("""() => {
            const scripts = Array.from(document.querySelectorAll('script'));
            for (const s of scripts) {
                if (s.textContent.includes('gt') && s.textContent.includes('challenge')) {
                    const gtMatch = s.textContent.match(/gt\\s*:\\s*['"]([^'"]+)['"]/);
                    const challengeMatch = s.textContent.match(/challenge\\s*:\\s*['"]([^'"]+)['"]/);
                    if (gtMatch && challengeMatch) {
                        return { gt: gtMatch[1], challenge: challengeMatch[1] };
                    }
                }
            }
            // Fallback: search in window object if possible
            if (window.initGeetest) return { type: 'dynamic' };
            return null;
        }""")

        if not params:
            l("WARN", "Could not find GeeTest parameters automatically.")
            return False

        l("INFO", f"📦 GeeTest params found. Sending to 2Captcha... (gt: {params.get('gt', 'detected')})")

        proxy_dict = get_2captcha_proxy_dict()

        result = await ASYNC_SOLVER.geetest(
            gt=params['gt'],
            challenge=params['challenge'],
            url=page.url,
            proxy=proxy_dict
        )

        # Normalize response: 2captcha-python can return str or dict
        code = None
        if isinstance(result, str) and result:
            code = result
        elif isinstance(result, dict) and result.get('code'):
            code = result['code']

        if code:
            l("OK", "✅ 2Captcha returned solution. Injecting into page...")
            
            # Inject the solution
            await page.evaluate(f"""(code) => {{
                // Standard GeeTest callback
                if (window.geetest_callback) {{
                    window.geetest_callback(code);
                }} else {{
                    // Try to find the hidden inputs and fill them
                    const validate = document.querySelector('input[name="geetest_validate"]');
                    const challenge = document.querySelector('input[name="geetest_challenge"]');
                    const seccode = document.querySelector('input[name="geetest_seccode"]');
                    
                    if (validate) validate.value = code;
                    if (seccode) seccode.value = code + '|jordan';
                    
                    // Submit form if present
                    const form = validate ? validate.form : null;
                    if (form) form.submit();
                }}
            }}""", code)
            
            await asyncio.sleep(3)
            _captcha_inc("geetest_solved")
            return True

    except Exception as e:
        _captcha_inc("geetest_errors")
        l("ERR", f"2Captcha GeeTest solver failed: {e}")

    return False

async def solve_datadome_2captcha(page, captcha_url=None, logger=None):
    """Solve DataDome CAPTCHA using 2Captcha DataDomeSliderTask."""
    l = logger or log
    if not TWOCAPTCHA_API_KEY:
        l("WARN", "2Captcha API key no configurada.")
        return False

    try:
        l("INFO", "Desplegando solución DataDome dedicada (DataDomeSliderTask)...")

        # 1. Prepare parameters
        page_url = page.url
        user_agent = await page.evaluate("navigator.userAgent")

        if not captcha_url:
            l("INFO", "Buscando captchaUrl de DataDome...")
            captcha_url = await page.evaluate("""() => {
                const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                return iframe ? iframe.src : null;
            }""")

        if not captcha_url:
            l("WARN", "No se pudo encontrar captchaUrl para DataDome.")
            return False

        # ── Validate t= parameter in captchaUrl ─────────────────────────
        # t=fe → IP in good standing, challenge can be solved
        # t=bv → IP permanently blocked by DataDome, any cookie will be rejected
        from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
        _parsed_captcha = _urlparse(captcha_url)
        _qs_params = _parse_qs(_parsed_captcha.query)
        _t_param = _qs_params.get('t', [''])[0]
        # Log completo (sin truncar) para diagnóstico
        l("INFO", f"DataDome captchaUrl completa: {captcha_url}")
        l("INFO", f"DataDome params: t={_t_param!r} | {_qs_params}")

        if '/interstitial/' in _parsed_captcha.path:
            l("WARN", "URL tipo /interstitial/ detectada (no es slider). DataDomeSliderTask no puede resolver este tipo. Rotando...")
            _captcha_inc("DataDome 2Captcha|interstitial")
            return False

        # 2. Prepare proxy config
        try:
            from shared.proxy_config import PROXY_CONFIG
        except ImportError:
            PROXY_CONFIG = None

        if not PROXY_CONFIG:
            l("ERR", "No se ha encontrado PROXY_CONFIG. DataDomeSliderTask exige proxy residencial.")
            return False

        import httpx
        # Build proxy login with sticky session ID so 2Captcha uses the same IP as the browser
        proxy_login = PROXY_CONFIG.get('login', '')
        sticky_sid = PROXY_CONFIG.get('sticky_session_id')
        if sticky_sid:
            proxy_login = f"{proxy_login}-session-{sticky_sid}"
            l("INFO", f"Proxy session ID: {sticky_sid}")

        # 3. Pre-flight proxy IP check — ANTES del check t=bv para loguear IP real en cada llamada
        # (sin flag one-shot: así detectamos si las sesiones realmente rotan IPs distintas)
        proxy_exit_ip = "unknown"
        try:
            proxy_url_check = f"http://{proxy_login}:{PROXY_CONFIG.get('password', '')}@{PROXY_CONFIG.get('host')}:{PROXY_CONFIG.get('port')}"
            async with httpx.AsyncClient(proxy=proxy_url_check, timeout=10, verify=False) as _ipc:
                _ip_resp = await _ipc.get("https://api.ipify.org?format=json")
                proxy_exit_ip = _ip_resp.json().get("ip", "unknown")
                l("INFO", f"🌐 Proxy exit IP (session {sticky_sid}): {proxy_exit_ip}")
        except Exception as ip_err:
            l("WARN", f"No se pudo verificar IP del proxy: {ip_err}")

        # 4. Ahora check t=bv — con IP ya logueada para diagnóstico
        if _t_param == 'bv':
            tbv_count = _increment_tbv_counter(proxy_exit_ip)
            l("WARN", f"t=bv #{tbv_count}: IP {proxy_exit_ip} bloqueada permanentemente por DataDome. Rotando identidad...")
            _captcha_inc("DataDome 2Captcha|ip_bloqueada")
            global _last_solver_fail_reason
            _last_solver_fail_reason = 'tbv'
            return False

        # 4. Build and send task payload
        task_payload = {
            "clientKey": TWOCAPTCHA_API_KEY,
            "task": {
                "type": "DataDomeSliderTask",
                "websiteURL": page_url,
                "captchaUrl": captcha_url,
                "userAgent": user_agent,
                "proxyType": PROXY_CONFIG.get('type', 'http').lower(),
                "proxyAddress": PROXY_CONFIG.get('host'),
                "proxyPort": PROXY_CONFIG.get('port'),
                "proxyLogin": proxy_login,
                "proxyPassword": PROXY_CONFIG.get('password')
            }
        }

        # Log sanitized payload (hide password)
        safe_payload = {**task_payload, "task": {**task_payload["task"], "proxyPassword": "***", "proxyLogin": proxy_login[:10] + "..."}}
        safe_payload["clientKey"] = "..." + TWOCAPTCHA_API_KEY[-4:]
        l("INFO", f"Payload (sanitized): {safe_payload}")

        async with httpx.AsyncClient(timeout=30) as client:
            l("INFO", "Enviando createTask a api.2captcha.com...")
            try:
                resp = await client.post("https://api.2captcha.com/createTask", json=task_payload, timeout=30)
            except httpx.ReadTimeout:
                l("ERR", "Timeout al enviar createTask a 2Captcha.")
                _captcha_inc("DataDome 2Captcha|error_api")
                return False
            resp_data = resp.json()

            if resp_data.get("errorId") != 0:
                l("ERR", f"Error 2Captcha createTask: {resp_data}")
                _captcha_inc("DataDome 2Captcha|error_api")
                return False

            task_id = resp_data.get("taskId")
            l("INFO", f"Tarea {task_id} creada. Polling cada 5s (hasta 65s, tokens DataDome expiran ~60s)...")

            max_wait = 65
            start_time = time.time()
            code = None
            poll_count = 0

            while time.time() - start_time < max_wait:
                await asyncio.sleep(5)
                poll_count += 1
                elapsed = int(time.time() - start_time)
                try:
                    res = await client.post("https://api.2captcha.com/getTaskResult", json={
                        "clientKey": TWOCAPTCHA_API_KEY,
                        "taskId": task_id
                    }, timeout=30)
                except httpx.ReadTimeout:
                    l("WARN", f"ReadTimeout en getTaskResult (poll #{poll_count}, {elapsed}s), reintentando...")
                    continue

                res_data = res.json()
                if res_data.get("errorId") != 0:
                    error_code = res_data.get("errorCode", "unknown")
                    if error_code == "ERROR_CAPTCHA_UNSOLVABLE":
                        l("WARN", f"2Captcha: CAPTCHA marcado como IRRESOLUBLE tras {elapsed}s")
                        _captcha_inc("DataDome 2Captcha|irresoluble")
                    else:
                        l("ERR", f"Error 2Captcha getTaskResult: {error_code} | {res_data}")
                        _captcha_inc("DataDome 2Captcha|error_api")
                    break

                status = res_data.get("status")
                if status == "ready":
                    solution = res_data.get("solution", {})
                    l("INFO", f"Solución recibida en {elapsed}s. Keys: {list(solution.keys())}")
                    # Robust cookie extraction: try multiple known keys
                    code = solution.get("cookie") or solution.get("datadome") or solution.get("token")
                    if not code:
                        # Fallback: find any string value >20 chars (likely the cookie)
                        for v in solution.values():
                            if isinstance(v, str) and len(v) > 20:
                                code = v
                                l("INFO", f"Cookie extracted from fallback key (len={len(v)})")
                                break
                    break
                elif status == "processing":
                    # Progress log every 30s
                    if elapsed % 30 < 6:
                        l("INFO", f"Polling... {elapsed}s/{max_wait}s (poll #{poll_count})")
                    continue
                else:
                    l("WARN", f"Estado de tarea 2Captcha desconocido: {status}")
                    break

            if not code:
                elapsed = int(time.time() - start_time)
                if elapsed >= max_wait:
                    l("WARN", f"2Captcha TIMEOUT: tarea sigue 'processing' tras {elapsed}s (URL captcha probablemente expirada)")
                    _captcha_inc("DataDome 2Captcha|timeout")
                else:
                    l("WARN", f"2Captcha: tarea finalizada sin cookie tras {elapsed}s")
                    _captcha_inc("DataDome 2Captcha|sin_cookie")
                return False

            l("INFO", f"Cookie datadome obtenida (len={len(code)}): ...{code[-20:]}")

            # 5. Inject the cookie via Playwright's native API
            try:
                domain = ".idealista.com"
                # Strip 'datadome=' prefix if present
                if 'datadome=' in code:
                    m = re.search(r'datadome=([^;]+)', code)
                    code = m.group(1).strip() if m else code

                await page.context.add_cookies([{
                    'name': 'datadome',
                    'value': code.strip(),
                    'domain': domain,
                    'path': '/',
                    'secure': True,
                    'sameSite': 'Lax'
                }])
                l("OK", f"Cookie 'datadome' inyectada en {domain}")
            except Exception as cookie_err:
                l("ERR", f"Error inyectando cookie DataDome: {cookie_err}")
                _captcha_inc("DataDome 2Captcha|error_inyeccion")
                return False

            # 6. Reload and verify (two-pass: handles slow page loads)
            l("INFO", f"Recargando página: {page_url[:60]}...")
            try:
                await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as nav_err:
                l("WARN", f"Error al recargar tras inyección: {nav_err}")
                return False

            for pass_num in (1, 2):
                await asyncio.sleep(5)
                is_still_blocked = await page.evaluate(r"""() => {
                    const hasDataDomeIframe = !!document.querySelector('iframe[src*="captcha-delivery.com"]');
                    const hasWarningText = document.body && document.body.innerText.toLowerCase().includes('estamos recibiendo muchas peticiones');
                    return !!(hasDataDomeIframe || hasWarningText);
                }""")

                if not is_still_blocked:
                    l("OK", f"DataDome resuelto (pass {pass_num}): CAPTCHA desapareció.")
                    return True

                if pass_num == 1:
                    l("INFO", "CAPTCHA aún presente tras 5s, esperando 5s más (pass 2)...")

            l("WARN", "Cookie inyectada pero el CAPTCHA persiste tras dos verificaciones.")
            _captcha_inc("DataDome 2Captcha|cookie_rechazada")
            return False

    except Exception as e:
        l("ERR", f"Error en solver DataDome 2Captcha: {type(e).__name__}: {e}")

    l("WARN", "DataDome no resuelto via 2Captcha.")
    return False


async def solve_datadome_capsolver(page, captcha_url=None, logger=None):
    """Fallback DataDome solver using CapSolver's DatadomeSliderTask API."""
    l = logger or log
    if not CAPSOLVER_API_KEY:
        l("WARN", "CapSolver API key no configurada. Fallback deshabilitado.")
        return False

    try:
        l("INFO", "Intentando CapSolver como fallback para DataDome...")

        page_url = page.url
        user_agent = await page.evaluate("navigator.userAgent")

        # CapSolver solo soporta UAs Chrome/Edge estándar (hasta Chrome/144)
        # — eliminar tokens Opera/Brave/Vivaldi y cap Chrome version a 144
        import re as _re_ua
        if _re_ua.search(r'\b(OPR|Brave|Vivaldi)/[\d.]+', user_agent):
            user_agent = _re_ua.sub(r'\s*(OPR|Brave|Vivaldi)/[\d.]+', '', user_agent).strip()
            l("INFO", f"UA sanitizado para CapSolver (tokens no estándar eliminados)")
        # Cap Chrome version to max supported (144)
        _chrome_match = _re_ua.search(r'Chrome/(\d+)', user_agent)
        if _chrome_match and int(_chrome_match.group(1)) > 144:
            user_agent = _re_ua.sub(r'Chrome/\d+', 'Chrome/144', user_agent)
            l("INFO", f"UA Chrome version capped a 144 para CapSolver (era {_chrome_match.group(1)})")
        # Eliminar Edg/ completamente: CapSolver rechaza cualquier UA con token Edg/ → ERROR_INVALID_TASK_DATA
        _edge_match = _re_ua.search(r'Edg/([\d.]+)', user_agent)
        if _edge_match:
            user_agent = _re_ua.sub(r'\s*Edg/[\d.]+', '', user_agent).strip()
            l("INFO", f"UA Edge token eliminado para CapSolver (era Edg/{_edge_match.group(1)})")

        if not captcha_url:
            captcha_url = await page.evaluate("""() => {
                const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                return iframe ? iframe.src : null;
            }""")

        if not captcha_url:
            l("WARN", "No se encontró captchaUrl para CapSolver.")
            return False

        # Validate t= parameter
        from urllib.parse import urlparse as _urlparse, parse_qs as _parse_qs
        _parsed_captcha = _urlparse(captcha_url)
        _t_param = _parse_qs(_parsed_captcha.query).get('t', [''])[0]
        if _t_param == 'bv':
            l("WARN", "t=bv: IP bloqueada. CapSolver tampoco puede resolver.")
            _captcha_inc("DataDome CapSolver|ip_bloqueada")
            global _last_solver_fail_reason
            _last_solver_fail_reason = 'tbv'
            return False

        if '/interstitial/' in _parsed_captcha.path:
            l("WARN", "URL tipo /interstitial/ detectada (no es slider). CapSolver no puede resolver este tipo. Rotando...")
            _captcha_inc("DataDome CapSolver|interstitial")
            return False

        try:
            from shared.proxy_config import PROXY_CONFIG
        except ImportError:
            PROXY_CONFIG = None

        if not PROXY_CONFIG:
            l("ERR", "No se ha encontrado PROXY_CONFIG para CapSolver.")
            return False

        import httpx
        proxy_login = PROXY_CONFIG.get('login', '')
        sticky_sid = PROXY_CONFIG.get('sticky_session_id')
        if sticky_sid:
            proxy_login = f"{proxy_login}-session-{sticky_sid}"

        task_payload = {
            "clientKey": CAPSOLVER_API_KEY,
            "task": {
                "type": "DatadomeSliderTask",
                "websiteURL": page_url,
                "captchaUrl": captcha_url,
                "userAgent": user_agent,
                "proxy": f"http://{proxy_login}:{PROXY_CONFIG.get('password', '')}@{PROXY_CONFIG.get('host')}:{PROXY_CONFIG.get('port')}"
            }
        }

        async with httpx.AsyncClient(timeout=30) as client:
            l("INFO", "Enviando createTask a api.capsolver.com...")
            try:
                resp = await client.post("https://api.capsolver.com/createTask", json=task_payload, timeout=30)
            except httpx.ReadTimeout:
                l("ERR", "Timeout al enviar createTask a CapSolver.")
                _captcha_inc("DataDome CapSolver|error_api")
                return False
            resp_data = resp.json()

            if resp_data.get("errorId", 1) != 0:
                l("ERR", f"Error CapSolver createTask: {resp_data}")
                _captcha_inc("DataDome CapSolver|error_api")
                return False

            task_id = resp_data.get("taskId")
            l("INFO", f"CapSolver tarea {task_id} creada. Polling (hasta 65s)...")

            max_wait = 65
            start_time = time.time()
            code = None

            while time.time() - start_time < max_wait:
                await asyncio.sleep(5)
                elapsed = int(time.time() - start_time)
                try:
                    res = await client.post("https://api.capsolver.com/getTaskResult", json={
                        "clientKey": CAPSOLVER_API_KEY,
                        "taskId": task_id
                    }, timeout=30)
                except httpx.ReadTimeout:
                    l("WARN", f"ReadTimeout CapSolver ({elapsed}s), reintentando...")
                    continue

                res_data = res.json()
                if res_data.get("errorId", 1) != 0:
                    l("ERR", f"Error CapSolver getTaskResult: {res_data}")
                    _captcha_inc("DataDome CapSolver|error_api")
                    break

                status = res_data.get("status")
                if status == "ready":
                    solution = res_data.get("solution", {})
                    code = solution.get("cookie") or solution.get("datadome") or solution.get("token")
                    if not code:
                        for v in solution.values():
                            if isinstance(v, str) and len(v) > 20:
                                code = v
                                break
                    break
                elif status == "processing":
                    if elapsed % 30 < 6:
                        l("INFO", f"CapSolver polling... {elapsed}s/{max_wait}s")
                    continue
                else:
                    l("WARN", f"CapSolver estado desconocido: {status}")
                    break

            if not code:
                elapsed_final = int(time.time() - start_time)
                if elapsed_final >= max_wait:
                    l("WARN", f"CapSolver TIMEOUT: tarea sigue procesando tras {elapsed_final}s.")
                    _captcha_inc("DataDome CapSolver|timeout")
                else:
                    l("WARN", "CapSolver: no se recibió cookie.")
                    _captcha_inc("DataDome CapSolver|sin_cookie")
                return False

            l("INFO", f"CapSolver cookie obtenida (len={len(code)})")

            # Inject cookie
            try:
                domain = ".idealista.com"
                if 'datadome=' in code:
                    m = re.search(r'datadome=([^;]+)', code)
                    code = m.group(1).strip() if m else code

                await page.context.add_cookies([{
                    'name': 'datadome',
                    'value': code.strip(),
                    'domain': domain,
                    'path': '/',
                    'secure': True,
                    'sameSite': 'Lax'
                }])
                l("OK", f"CapSolver cookie inyectada en {domain}")
            except Exception as cookie_err:
                l("ERR", f"Error inyectando cookie CapSolver: {cookie_err}")
                _captcha_inc("DataDome CapSolver|error_inyeccion")
                return False

            # Reload and verify (two-pass)
            try:
                await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
            except Exception as nav_err:
                l("WARN", f"Error al recargar tras CapSolver: {nav_err}")
                return False

            for pass_num in (1, 2):
                await asyncio.sleep(5)
                is_still_blocked = await page.evaluate(r"""() => {
                    const hasDataDomeIframe = !!document.querySelector('iframe[src*="captcha-delivery.com"]');
                    const hasWarningText = document.body && document.body.innerText.toLowerCase().includes('estamos recibiendo muchas peticiones');
                    return !!(hasDataDomeIframe || hasWarningText);
                }""")
                if not is_still_blocked:
                    l("OK", f"CapSolver resolvió DataDome (pass {pass_num}).")
                    return True
                if pass_num == 1:
                    l("INFO", "CapSolver: CAPTCHA aún presente, esperando 5s más...")

            l("WARN", "CapSolver: cookie inyectada pero CAPTCHA persiste.")
            _captcha_inc("DataDome CapSolver|cookie_rechazada")
            return False

    except Exception as e:
        l("ERR", f"Error en CapSolver DataDome: {type(e).__name__}: {e}")

    return False


async def solve_slider_2captcha(page, logger=None):
    """Solve simple slider captchas using 2Captcha Coordinates method (Refined version)."""
    l = logger or log
    if not SOLVER:
        return False
        
    try:
        # Get Device Pixel Ratio for coordinate scaling
        pixel_ratio = await page.evaluate("window.devicePixelRatio || 1.0")
        l("INFO", f"📐 Detection Scale (DPI): {pixel_ratio}")

        # 1. Detect any slider-like containers with expanded selectors
        selectors = [
            ".px-captcha-container", 
            "#captcha-container",
            "#challenge-container",
            ".geetest_holder",
            ".nc-container",
            "div[class*='captcha']",
            "div[id*='captcha']",
            "iframe[title*='captcha']",
            "iframe[src*='captcha']",
            ".captcha-box"
        ]
        
        container = None
        for sel in selectors:
            try:
                elem = await page.query_selector(sel)
                if elem and await elem.is_visible():
                    container = elem
                    # Ensure it's in view for a good screenshot
                    await elem.scroll_into_view_if_needed()
                    break
            except: continue
            
        if not container:
            l("INFO", "No explicit captcha container found. Using body fallback.")
            container = await page.query_selector("body")
            
        if not container: return False

        l("INFO", "📸 Capturando screenshot del captcha para 2Captcha...")
        # Precise screenshot
        fd, img_path = tempfile.mkstemp(suffix=".png", prefix="captcha_")
        os.close(fd)
        
        await container.screenshot(path=img_path)
        
        # 3. Request Coordinates from 2Captcha
        l("INFO", "📤 Enviando coordenadas a 2Captcha (Slider)...")
        # Updated instructions to be more precise
        instructions = "Haz clic en el PUNTO DESTINO (extremo derecho) donde debe llegar el botón deslizante. / Click on the DESTINATION point (far right) where the slider button should end."
        
        try:
            result = await asyncio.to_thread(
                SOLVER.coordinates,
                file=img_path,
                textinstructions=instructions
            )
        except Exception as e:
            l("ERR", f"2Captcha API call failed: {type(e).__name__}: {e}")
            result = None
        
        # Cleanup image
        if os.path.exists(img_path):
            try: os.remove(img_path)
            except: pass
            
        # Type-safe parsing of 2Captcha coordinates result to prevent KeyError
        # result for coordinates can be a list or a dict {'captchaId': '...', 'code': 'coordinates:x=614,y=390'}
        # or {'0': {'x': '20', 'y': '30'}}
        if result:
            tx, ty = 0.0, 0.0
            parsed_successfully = False
            
            if isinstance(result, list) and len(result) > 0:
                target = result[0]
                tx = float(target.get('x', 0))
                ty = float(target.get('y', 0))
                parsed_successfully = True
            elif isinstance(result, dict):
                code = result.get('code')
                if code and isinstance(code, str) and 'coordinates:' in code:
                    # Example: 'coordinates:x=614,y=390' OR 'coordinates:x=244,y=80;x=474,y=558'
                    # We only care about the first point
                    first_coord = code.split(';')[0].replace('coordinates:', '') # 'x=614,y=390'
                    parts = dict(kv.split('=') for kv in first_coord.split(','))
                    tx = float(parts.get('x', 0))
                    ty = float(parts.get('y', 0))
                    parsed_successfully = True
                elif '0' in result:
                    target = result['0']
                    tx = float(target.get('x', 0))
                    ty = float(target.get('y', 0))
                    parsed_successfully = True
                    
            if not parsed_successfully:
                l("ERR", f"Unexpected result format from 2Captcha coordinates: {result}")
                return False
            
            box = await container.bounding_box()
            if not box: return False
            
            # Map image-relative coords to page-relative
            # IMPORTANT: Screenshot is in physical pixels, but Playwright mouse moves in CSS pixels.
            # We MUST divide by pixel_ratio for correct alignment.
            dest_x = box['x'] + (tx / pixel_ratio)
            dest_y = box['y'] + (ty / pixel_ratio)
            
            l("INFO", f"🎯 Target mapped: {int(dest_x)},{int(dest_y)} (Original: {tx},{ty} @ {pixel_ratio}x)")
            
            # 4. Find the slider handle with expanded selectors
            handle_selectors = [
                 ".px-captcha-slider-button", 
                 ".geetest_slider_button", 
                 ".nc_iconfont.btn_slide", 
                 "#nc_1_n1z", 
                 ".slid_btn",
                 "div[role='button'][aria-label*='Desliza']",
                 "div[role='button'][aria-label*='Slide']",
                 "div[class*='slider-handle']",
                 "div[class*='captcha-slider-handle']",
                 "span:has-text('→')",
                 "div:has-text('→')",
                 ".arrow-right",
                 "div[aria-label*='Slide to right']",
                 "div[aria-label*='Desliza hacia la derecha']",
                 "button[aria-label*='Slide']",
                 "button[aria-label*='Desliza']"
            ]
            
            handle = None
            is_iframe = await container.evaluate("el => el.tagName.toLowerCase() === 'iframe'")
            query_root = page
            if is_iframe:
                frame = await container.content_frame()
                if frame:
                    query_root = frame
                    l("INFO", "🔍 Buscando slider handle dentro del iframe...")
            
            for hs in handle_selectors:
                try:
                    h = await query_root.query_selector(hs)
                    if h and await h.is_visible():
                        handle = h
                        break
                except: continue
                
            if not handle:
                # If no handle, try to find the FIRST visible role='button' inside the container
                try:
                    handle = await query_root.query_selector("div[role='button'], button")
                except: pass

            if not handle:
                l("WARN", "Slider handle not found. Attempting a simple click at target...")
                await page.mouse.click(dest_x, dest_y)
                return True
                
            h_box = await handle.bounding_box()
            if not h_box: return False
            
            start_x = h_box['x'] + h_box['width'] / 2
            start_y = h_box['y'] + h_box['height'] / 2
            
            if is_iframe:
                start_x += box['x']
                start_y += box['y']
            
            l("INFO", f"🖱️ Dragging handle from {start_x:.0f} to target {dest_x:.0f} (Organic)...")
            
            # Move to handle
            await page.mouse.move(start_x, start_y, steps=5)
            await asyncio.sleep(random.uniform(0.1, 0.3))
            
            # Simple wiggle to simulate human touch
            await page.mouse.move(start_x + random.randint(-2, 2), start_y + random.randint(-2, 2))
            await asyncio.sleep(random.uniform(0.1, 0.2))
            
            await page.mouse.down()
            await asyncio.sleep(random.uniform(0.3, 0.6))
            
            # Organic drag movement (faster in middle, slower at ends)
            steps = random.randint(40, 60)
            for i in range(1, steps + 1):
                # Ease-in-out curve
                t = i / steps
                # Quadratic ease in out: t < 0.5 ? 2*t*t : -1 + (4-2*t)*t
                ease_t = 2*t*t if t < 0.5 else -1 + (4-2*t)*t
                
                curr_x = start_x + (dest_x - start_x) * ease_t
                # Small vertical jitter
                curr_y = start_y + (dest_y - start_y) * ease_t + random.uniform(-1, 1)
                
                await page.mouse.move(curr_x, curr_y)
                # Small variable sleep to simulate human inconsistency
                await asyncio.sleep(random.uniform(0.01, 0.04))
            
            # Hold at destination for a moment
            await asyncio.sleep(random.uniform(0.3, 0.7))
            await page.mouse.up()
            
            # 5. Verification: Check if captcha container is still present/visible
            await asyncio.sleep(3)
            still_there = False
            # Update trailing log logic to verify the DOM more robustly
            is_still_blocked = await page.evaluate(r"""() => {
                const hasCaptchaContainer = document.querySelector(".px-captcha-container, #captcha-container, #challenge-container, .geetest_holder, .nc-container, div[class*='captcha'], div[id*='captcha'], iframe[title*='captcha'], iframe[src*='captcha'], .captcha-box");
                const hasWarningText = document.body && document.body.innerText.toLowerCase().includes('estamos recibiendo muchas peticiones');
                return !!(hasCaptchaContainer && hasCaptchaContainer.offsetParent !== null || hasWarningText);
            }""")
            
            if is_still_blocked:
                l("WARN", "⚠️ El slider se movió pero el captcha sigue visible o la página sigue bloqueada.")
                return False
                
            l("OK", "✅ 2Captcha Coordinates solved!")
            return True
            
    except Exception as e:
        l("ERR", f"2Captcha Slider solver error: {type(e).__name__}: {e}")
        
    l("WARN", "❌ Falló la resolución del CAPTCHA después de todos los intentos.")
    return False



async def solve_captcha_advanced(page, logger=None, use_proxy: bool = True):
    """Orchestrator: DataDome (alternating 2Captcha/CapSolver) -> Local Slider -> 2Captcha (GeeTest/Coords).

    use_proxy: False cuando el browser no usa proxy (ej. WebKit sin proxy). En ese caso los solvers
    de pago se omiten porque la cookie de DataDome estaría vinculada a la IP del solver (BrightData)
    y no a la IP directa del browser → IP mismatch garantizado.
    """
    l = logger or log

    # ── Guard: skip SSL error pages (not a captcha) ──────────────────────────
    try:
        page_title = await page.title()
        if page_title and any(kw in page_title.lower() for kw in [
            "no es privada", "error de privacidad",
            "is not private", "err_cert_authority_invalid",
        ]):
            l("WARN", f"Página de error SSL detectada (título: '{page_title}'). No es un captcha.")
            return False
    except Exception:
        pass

    # ── 1. Check for DataDome FIRST (most common blocker) ──────────────────
    datadome_data = await page.evaluate("""() => {
        const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
        return iframe ? { is_datadome: true, captcha_url: iframe.src } : { is_datadome: false };
    }""")

    if datadome_data.get('is_datadome'):
        page_url = page.url

        # ── Circuit breaker: demasiados t=bv consecutivos → pausa larga para enfriar IPs ──
        try:
            from scraper.idealista_scraper.config import TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN
        except ImportError:
            try:
                from idealista_scraper.config import TBV_CIRCUIT_BREAKER_THRESHOLD, TBV_CIRCUIT_BREAKER_PAUSE_MIN
            except ImportError:
                TBV_CIRCUIT_BREAKER_THRESHOLD = 8
                TBV_CIRCUIT_BREAKER_PAUSE_MIN = 30
        _tbv_now = _get_tbv_count()
        if _tbv_now >= TBV_CIRCUIT_BREAKER_THRESHOLD:
            l("WARN", f"🚨 CIRCUIT BREAKER: {_tbv_now} t=bv consecutivos. Pausa de {TBV_CIRCUIT_BREAKER_PAUSE_MIN} min para enfriar IPs del pool español...")
            await asyncio.sleep(TBV_CIRCUIT_BREAKER_PAUSE_MIN * 60)
            _reset_tbv_counter()
            l("INFO", "Circuit breaker expirado. Reanudando...")

        # Sin proxy: los solvers de pago producirían IP mismatch (cookie vinculada a IP de BrightData,
        # pero el browser navega con IP directa). Solo intentar recarga rápida, sin coste.
        if not use_proxy:
            l("INFO", "🚫 Worker sin proxy: solvers de pago omitidos (IP mismatch garantizado). Solo recarga rápida...")
            _captcha_inc("Recarga rápida sin proxy|intentos")
            try:
                await page.goto("https://www.idealista.com", wait_until='domcontentloaded', timeout=15000)
                await asyncio.sleep(2)
                await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)
                try:
                    await page.wait_for_load_state('load', timeout=10000)
                except Exception:
                    pass
                quick_check = await page.evaluate("""() => {
                    const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                    return iframe ? iframe.src : null;
                }""")
                if not quick_check:
                    l("OK", "✅ DataDome desapareció tras recarga (worker sin proxy). Página libre.")
                    _captcha_inc("Recarga rápida sin proxy|resueltos")
                    return True
            except Exception as e:
                l("WARN", f"Error en recarga rápida (worker sin proxy): {e}")
            l("WARN", "DataDome persiste en worker sin proxy. No se puede resolver automáticamente.")
            return False

        # Check if current browser UA is CapSolver-compatible (standard Chrome/Edge only)
        import re as _re_ua
        try:
            current_ua = await page.evaluate("navigator.userAgent")
        except Exception:
            current_ua = ""
        # CapSolver solo soporta Chrome puro: excluir OPR/Brave/Vivaldi y también Safari/WebKit (sin Chrome en UA)
        capsolver_compatible = (
            not _re_ua.search(r'\b(OPR|Brave|Vivaldi)/[\d.]+', current_ua)
            and bool(_re_ua.search(r'Chrome/\d+', current_ua))  # Safari/WebKit no tienen Chrome en UA
        )

        # Build solver sequence: CapSolver primero (más rápido, ~5s vs ~65s de 2Captcha)
        # 2Captcha hace timeout sistemáticamente antes de que expire el token DataDome (~60s)
        if CAPSOLVER_API_KEY and capsolver_compatible:
            solvers = [
                ("CapSolver", solve_datadome_capsolver),
                ("2Captcha", solve_datadome_2captcha),
                ("CapSolver", solve_datadome_capsolver),
            ]
        else:
            if CAPSOLVER_API_KEY and not capsolver_compatible:
                l("INFO", f"CapSolver excluido: UA no compatible ({current_ua.split()[-1] if current_ua else 'unknown'})")
            solvers = [
                ("2Captcha", solve_datadome_2captcha),
                ("2Captcha", solve_datadome_2captcha),
                ("2Captcha", solve_datadome_2captcha),
            ]
        total = len(solvers)

        l("INFO", f"DataDome CAPTCHA detectado. Secuencia de resolución: {' → '.join(s[0] for s in solvers)} ({total} intentos)...")

        # ── Quick reload attempt BEFORE paid solvers ─────────────────────────
        # DataDome often clears itself after a homepage→target reload cycle.
        # This saves ~65s per 2Captcha call when it works (frequent case).
        _captcha_inc("Recarga rápida|intentos")
        l("INFO", "🔄 Intentando recarga rápida (homepage → URL) antes de solvers de pago...")
        try:
            await page.goto("https://www.idealista.com", wait_until='domcontentloaded', timeout=15000)
            await asyncio.sleep(2)
            await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
            await asyncio.sleep(2)
            try:
                await page.wait_for_load_state('load', timeout=10000)
            except Exception:
                pass
            await asyncio.sleep(1)

            # Check if DataDome is still present. Wrapped with retry: after domcontentloaded
            # the page may still be executing JS redirects (DataDome iframe injection), which
            # destroys the execution context. A 3s retry avoids falling through to paid solvers.
            try:
                quick_check = await page.evaluate("""() => {
                    const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                    return iframe ? iframe.src : null;
                }""")
            except Exception as eval_err:
                l("WARN", f"Error evaluando captcha (contexto destruido): {eval_err}. Reintentando en 3s...")
                await asyncio.sleep(3)
                try:
                    quick_check = await page.evaluate("""() => {
                        const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                        return iframe ? iframe.src : null;
                    }""")
                except Exception:
                    quick_check = datadome_data.get('captcha_url')  # fallback: asumir captcha persiste

            if not quick_check:
                l("OK", "✅ DataDome desapareció tras recarga rápida. Página libre (sin coste 2Captcha).")
                _captcha_inc("Recarga rápida|resueltos")
                return True
            if '/interstitial/' in quick_check:
                l("WARN", "⛔ URL /interstitial/ detectada tras recarga (IP bloqueada). Abortando.")
                return False
            # DataDome persists — update captcha_url for solver loop
            l("INFO", "DataDome persiste tras recarga rápida. Continuando con solvers de pago...")
            datadome_data['captcha_url'] = quick_check
        except Exception as reload_err:
            l("WARN", f"Error en recarga rápida pre-solver: {reload_err}. Navegando de vuelta a URL objetivo...")
            try:
                await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                await asyncio.sleep(2)
                # Comprobar si DataDome desapareció tras la navegación de recuperación.
                # Es posible que la recarga homepage→URL sí funcionara pero el evaluate()
                # fallara por contexto destruido — en ese caso evitamos llamar a 2Captcha.
                try:
                    recovery_check = await page.evaluate("""() => {
                        const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                        return iframe ? iframe.src : null;
                    }""")
                    if not recovery_check:
                        l("OK", "✅ DataDome desapareció tras navegación de recuperación. Página libre (sin coste 2Captcha).")
                        _captcha_inc("Recarga rápida|resueltos")
                        return True
                except Exception:
                    pass  # Si el check falla, continuar al loop de solvers
            except Exception:
                pass

        _last_was_tbv = False
        for attempt, (solver_name, solver_fn) in enumerate(solvers, 1):
            # Fast-fail: si el último intento fue t=bv (IP bloqueada), la misma sesión de proxy
            # dará el mismo resultado — no gastar 10-15s en navegación innecesaria
            if attempt > 1 and _last_was_tbv:
                l("WARN", f"⚡ Saltando intento {attempt}/{total}: último fallo fue t=bv (IP bloqueada, misma sesión de proxy)")
                break

            # On retries, navigate AWAY first to reset DataDome context and avoid escalation
            # (reloading the same URL causes DataDome to escalate /captcha/ → /interstitial/)
            if attempt > 1:
                l("INFO", f"Intento {attempt}/{total}: Navegando a homepage para evitar escalación DataDome...")
                try:
                    await page.goto("https://www.idealista.com", wait_until='domcontentloaded', timeout=15000)
                    await asyncio.sleep(2)
                    l("INFO", f"Intento {attempt}/{total}: Volviendo a URL objetivo...")
                    await page.goto(page_url, wait_until='domcontentloaded', timeout=30000)
                    await asyncio.sleep(2)
                    try:
                        await page.wait_for_load_state('load', timeout=10000)
                    except Exception:
                        pass
                    await asyncio.sleep(1)
                except Exception as reload_err:
                    l("WARN", f"Error en navegación de retry: {reload_err}")
                    continue

                # Re-detect captcha URL after reload (wrapped to prevent crash on context destruction)
                try:
                    fresh_data = await page.evaluate("""() => {
                        const iframe = document.querySelector('iframe[src*="captcha-delivery.com"]');
                        return iframe ? iframe.src : null;
                    }""")
                except Exception as eval_err:
                    l("WARN", f"Intento {attempt}/{total}: Error detectando captcha tras recarga: {eval_err}")
                    continue
                if not fresh_data:
                    # Page might have loaded clean after reload
                    l("OK", "DataDome desapareció tras recarga. Página libre.")
                    _captcha_inc("Recarga rápida|resueltos")
                    _reset_tbv_counter()
                    return True
                if '/interstitial/' in fresh_data:
                    l("WARN", f"Intento {attempt}/{total}: URL /interstitial/ detectada (IP bloqueada). Abortando reintentos...")
                    break
                captcha_url = fresh_data
            else:
                captcha_url = datadome_data.get('captcha_url')

            global _last_solver_fail_reason
            _last_solver_fail_reason = ''  # Reset antes de llamar al solver
            _captcha_inc(f"DataDome {solver_name}|intentos")
            l("INFO", f"{solver_name} intento {attempt}/{total}...")
            if await solver_fn(page, captcha_url=captcha_url, logger=l):
                l("OK", f"DataDome resuelto via {solver_name} en intento {attempt}/{total}.")
                _captcha_inc(f"DataDome {solver_name}|resueltos")
                _reset_tbv_counter()
                return True

            _last_was_tbv = (_last_solver_fail_reason == 'tbv')
            l("WARN", f"{solver_name} intento {attempt}/{total} falló." + (" (t=bv)" if _last_was_tbv else ""))

        # DataDome: coordinates are useless (server-side validation)
        # Rotar sticky session para que la próxima rotación de identidad use IP fresca
        try:
            from shared.proxy_config import regenerate_session
            new_sid = regenerate_session()
            l("INFO", f"🔑 Proxy session rotada tras fallo total de solvers DataDome (nueva: {new_sid})")
        except Exception as proxy_err:
            l("WARN", f"Error rotando proxy session: {proxy_err}")
        l("WARN", "DataDome no resuelto tras todos los intentos. Rotando identidad.")
        return False

    # ── 2. Non-DataDome: try local slider first (fast, free) ───────────────
    _captcha_inc("Slider local|intentos")
    l("INFO", "Intentando resolución local (Slider)...")
    if await solve_slider_captcha(page):
        await asyncio.sleep(3)
        title = (await page.title()).lower()
        if "idealista" in title and not any(kw in title for kw in ["captcha", "attention", "robot", "challenge", "verification"]):
            l("OK", "Local slider solved the CAPTCHA!")
            _captcha_inc("Slider local|resueltos")
            return True
        l("WARN", "Slider local falló o el bloqueo persiste.")

    # ── 3. Non-DataDome: 2Captcha paid solvers ─────────────────────────────
    if SOLVER:
        # A. GeeTest
        is_geetest = await page.evaluate("() => !!(window.initGeetest || document.querySelector('.geetest_holder'))")
        if is_geetest:
            _captcha_inc("2Captcha GeeTest|intentos")
            l("INFO", "Iniciando solver 2Captcha para GeeTest...")
            if await solve_geetest_2captcha(page, logger=l):
                await asyncio.sleep(3)
                title = (await page.title()).lower()
                if "idealista" in title and not any(kw in title for kw in ["captcha", "attention", "robot", "challenge", "verification"]):
                    l("OK", "2Captcha GeeTest solved!")
                    _captcha_inc("2Captcha GeeTest|resueltos")
                    return True

        # B. Coordinate-based slider
        _captcha_inc("2Captcha Coordenadas|intentos")
        l("INFO", "Iniciando solver 2Captcha por Coordenadas (Screenshot)...")
        if await solve_slider_2captcha(page, logger=l):
            await asyncio.sleep(4)
            title = (await page.title()).lower()
            if "idealista" in title and not any(kw in title for kw in ["captcha", "attention", "robot", "challenge", "verification"]):
                l("OK", "2Captcha Coordinates solved!")
                _captcha_inc("2Captcha Coordenadas|resueltos")
                return True
    else:
        l("WARN", "2Captcha no disponible (import fallida o API Key inválida).")

    l("WARN", "Falló la resolución del CAPTCHA después de todos los intentos.")
    return False
