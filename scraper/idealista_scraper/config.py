from __future__ import annotations
from typing import Tuple, List

DEFAULT_CDP_PORT: int = 9222
HARVEST_DEBOUNCE_SECONDS: float = 1.5
PAGE_WAIT_MS: int = 250
RETRY_MAX_ATTEMPTS: int = 3
RETRY_BASE_DELAY: float = 0.75
GOTO_WAIT_UNTIL: str = "domcontentloaded"
SCROLL_STEPS: int = 3
LISTING_LINKS_PER_PAGE_MAX: int = 30

# Mode-specific delay presets
# Stealth mode (existing values - human-like delays)
STEALTH_SCROLL_PAUSE_RANGE: Tuple[float, float] = (0.5, 1.2)
STEALTH_CARD_DELAY_RANGE: Tuple[float, float] = (0.8, 2.0)
STEALTH_POST_CARD_DELAY_RANGE: Tuple[float, float] = (1.2, 3.0)

# Fast mode (minimal delays for speed)
FAST_SCROLL_PAUSE_RANGE: Tuple[float, float] = (0.1, 0.3)
FAST_CARD_DELAY_RANGE: Tuple[float, float] = (0.1, 0.3)
FAST_POST_CARD_DELAY_RANGE: Tuple[float, float] = (0.2, 0.5)

# Extra Stealth mode (maximum anti-detection)
EXTRA_STEALTH_SCROLL_PAUSE_RANGE: Tuple[float, float] = (1.5, 4.0)
EXTRA_STEALTH_CARD_DELAY_RANGE: Tuple[float, float] = (4.0, 10.0)
EXTRA_STEALTH_POST_CARD_DELAY_RANGE: Tuple[float, float] = (6.0, 18.0)

# Extra Stealth: Session limits and rest periods
EXTRA_STEALTH_SESSION_LIMIT: int = 50  # Properties before mandatory rest
EXTRA_STEALTH_REST_DURATION_RANGE: Tuple[float, float] = (600, 900)  # 10-15 minutes in seconds
EXTRA_STEALTH_COFFEE_BREAK_RANGE: Tuple[float, float] = (30, 90)  # Random pause every N properties
EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY: Tuple[int, int] = (10, 18)  # Every 10-18 properties

# Extra Stealth: Reading time simulation (seconds per 100 characters of description)
EXTRA_STEALTH_READING_TIME_PER_100_CHARS: float = 1.5

# Extra Stealth: User-agent rotation list (Updated 2026)
USER_AGENTS: List[str] = [
    # Windows 10/11 Chrome 132+
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    # Windows Firefox 134+
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
    # MacOS Chrome 132+
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # MacOS Firefox 134+
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14.7; rv:134.0) Gecko/20100101 Firefox/134.0",
    # Edge 132+
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36 Edg/132.0.0.0"
]

# Extra Stealth: Viewport sizes (width, height) for rotation
# Using common resolutions to blend in
VIEWPORT_SIZES: List[Tuple[int, int]] = [
    (1920, 1080), # FHD (Most common)
    (1366, 768),  # Laptop HD
    (1536, 864),  # Windows Scaling 125%
    (1440, 900),  # Mac Retina / Widescreen
    (1280, 720),  # HD
]

# Default to stealth mode
SCROLL_PAUSE_RANGE: Tuple[float, float] = STEALTH_SCROLL_PAUSE_RANGE

# Profile Rotation Settings (Advanced Evasion)
MAX_PROFILE_POOL_SIZE: int = 5
PROFILE_COOLDOWN_MINUTES: int = 10

# Browser Rotation Sequence (Strict Sequential)
# 1 -> 2 -> 3 -> 4 -> 5 -> 6 -> 7 -> 1
BROWSER_ROTATION_POOL: List[dict] = [
    {"index": 1, "engine": "chromium", "channel": None, "name": "Chromium (Default)"},
    {"index": 2, "engine": "chromium", "channel": "chrome", "name": "Google Chrome"},
    {"index": 3, "engine": "chromium", "channel": "msedge", "name": "Microsoft Edge"},
    {"index": 4, "engine": "firefox", "channel": None, "name": "Firefox"},
    {"index": 5, "engine": "webkit", "channel": None, "name": "Webkit (Safari)"},
    {"index": 6, "engine": "chromium", "channel": "opera", "name": "Opera Browser"}
]
