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



# =============================================================================
# Logging Functions
# =============================================================================

def log(kind: str, msg: str) -> None:
    """Simple timestamped logger with colored output for different log levels.
    Filters out verbose intermediate logs as requested by user.
    """
    import sys
    import os
    
    # Filter verbose logs
    kind_upper = kind.upper()
    if kind_upper in ["DEBUG_TIMING", "STEALTH", "DEBUG"]:
        return
        
    if kind_upper == "INFO":
        silence_patterns = ["Navigating to:", "Sleeping for", "took", "Target Excel file", "Loaded"]
        if any(p in msg for p in silence_patterns):
            return

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
        color = colors.get(kind_upper, "")
        reset = "\033[0m"
    else:
        color = ""
        reset = ""
    
    full_msg = f"[{ts}] [{kind_upper}] {msg}"
    
    try:
        print(f"{color}{full_msg}{reset}")
    except UnicodeEncodeError:
        # Fallback for Windows consoles (cp1252)
        safe_msg = msg.replace("≤", "<=").replace("→", "->").replace("€", "E")
        try:
            print(f"{color}[{ts}] [{kind_upper}] {safe_msg}{reset}")
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
        # 1. Identify the slider handle
        # Common selectors for slider captchas (Idealista uses specific ones, but we check common ones)
        selectors = [
            ".geetest_slider_button", ".nc_iconfont.btn_slide", "#nc_1_n1z", 
            ".slid_btn", ".captcha_slider", "div[role='button'][aria-label*='slider']",
            ".px-captcha-container .px-captcha-slider-button" # PerimeterX/DataDome common
        ]
        
        handle = None
        for sel in selectors:
            handle = await page.query_selector(sel)
            if handle and await handle.is_visible():
                break
        
        if not handle:
            # Try finding by icon or style if specific selector fails
            handle = await page.query_selector("span:has-text('→'), .arrow-right, [class*='slider']")
            if not handle or not await handle.is_visible():
                return False

        # 2. Get bounding boxes
        box = await handle.bounding_box()
        if not box:
            return False
            
        start_x = box['x'] + box['width'] / 2
        start_y = box['y'] + box['height'] / 2
        
        # Track length - usually around 250-300px, or we try to find the container
        container = await page.query_selector(".geetest_slider, .nc-container, .captcha_track, [class*='track']")
        if container:
            cbox = await container.bounding_box()
            distance = cbox['width'] - box['width'] if cbox else 260
        else:
            distance = 260 + random.randint(-10, 10)

        # 3. Perform human-like drag
        await page.mouse.move(start_x, start_y)
        await page.mouse.down()
        
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
