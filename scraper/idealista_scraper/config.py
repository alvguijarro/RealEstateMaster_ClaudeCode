from __future__ import annotations
from typing import Tuple, List

DEFAULT_CDP_PORT: int = 9222

# t=bv circuit breaker: pausa preventiva cuando el pool de IPs españolas está agotado
TBV_CIRCUIT_BREAKER_THRESHOLD: int = 8    # t=bv consecutivos antes de pausar
TBV_CIRCUIT_BREAKER_PAUSE_MIN: int = 30   # minutos de pausa para enfriar IPs

# CapSolver: versión máxima de Chrome soportada por la API de CapSolver DatadomeSliderTask.
# Actualizar cuando CapSolver anuncie soporte para versiones superiores.
CAPSOLVER_MAX_CHROME_VERSION: int = 144
HARVEST_DEBOUNCE_SECONDS: float = 1.5
PAGE_WAIT_MS: int = 250
RETRY_MAX_ATTEMPTS: int = 3
RETRY_BASE_DELAY: float = 0.75
GOTO_WAIT_UNTIL: str = "domcontentloaded"

# Estabilización post-carga: tiempo mínimo para que la página se asiente
# antes de evaluar si hay captcha, bloqueo, o contenido
PAGE_LOAD_STABILIZATION_SECONDS: float = 15.0
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
EXTRA_STEALTH_SESSION_LIMIT: int = 150  # Increased from 50
EXTRA_STEALTH_REST_DURATION_RANGE: Tuple[float, float] = (300, 600)  # 5-10 minutes instead of 10-15
EXTRA_STEALTH_COFFEE_BREAK_RANGE: Tuple[float, float] = (30, 90)  # Random pause every N properties
EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY: Tuple[int, int] = (10, 18)  # Every 10-18 properties

# Extra Stealth: Reading time simulation (seconds per 100 characters of description)
EXTRA_STEALTH_READING_TIME_PER_100_CHARS: float = 1.5

# Extra Stealth: User-agent rotation list (Updated March 2026)
# Only Chromium-based UAs to match the browser pool (no Firefox/Safari UAs)
USER_AGENTS: List[str] = [
    # Chrome 137
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36",
    # Chrome 136
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    # Chrome 135
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
    # Chrome 134
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    # Edge 137
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 Edg/137.0.0.0",
    # Edge 136
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36 Edg/136.0.0.0",
    # Edge 135
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36 Edg/135.0.0.0",
    # Opera (Chromium 137)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Safari/537.36 OPR/123.0.0.0",
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

# Pool de 5 workers paralelos — reemplaza BROWSER_ROTATION_POOL + PROXY_FREE_PARALLEL_BROWSERS.
# Worker 1 es visible (productor de listado + consumidor), Workers 2-5 son headless (solo consumidores).
# Todos usan proxy residencial (cada uno con sticky session diferente).
WORKER_POOL: List[dict] = [
    {"id": 1, "engine": "chromium", "channel": None,     "name": "Chromium",  "headless": False, "slot": 1},
    {"id": 2, "engine": "chromium", "channel": "chrome",  "name": "Chrome",    "headless": True,  "slot": 2},
    {"id": 3, "engine": "chromium", "channel": "msedge",  "name": "Edge",      "headless": True,  "slot": 3},
    {"id": 4, "engine": "chromium", "channel": "opera",   "name": "Opera",     "headless": True,  "slot": 4},
    {"id": 5, "engine": "chromium", "channel": "iron",    "name": "Iron",      "headless": True,  "slot": 5},
]

# Mantener BROWSER_ROTATION_POOL como alias para compatibilidad con código legacy
# (update_urls.py, trends_tracker.py, rotate_identity, etc.)
BROWSER_ROTATION_POOL: List[dict] = [
    {"index": w["id"], "engine": w["engine"], "channel": w["channel"], "name": w["name"]}
    for w in WORKER_POOL[:3]  # Solo los 3 primeros para rotación de identidad del browser principal
]
