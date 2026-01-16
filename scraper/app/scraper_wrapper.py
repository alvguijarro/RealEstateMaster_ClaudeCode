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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

from playwright.async_api import async_playwright

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
from idealista_scraper.utils import same_domain, canonical_listing_url, is_listing_url, sanitize_filename_part, play_captcha_alert
from idealista_scraper.extractors import extract_detail_fields, missing_fields
from idealista_scraper.excel_writer import (
    load_existing_single_sheet, load_existing_specific_sheet, export_single_sheet,
    load_urls_with_dates, export_split_by_distrito
)


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


@dataclass
class ScraperController:
    """Controller for the Idealista scraper with pause/stop and callbacks."""
    
    seed_url: str
    mode: str = "stealth"  # "stealth" or "fast"
    out_xlsx: str = "idealista.xlsx"
    sheet_name: str = "idealista"
    output_dir: str = DEFAULT_OUTPUT_DIR  # Configurable output directory
    
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
    _browser_closed: bool = False
    
    # Extra Stealth state
    _session_property_count: int = 0  # Properties scraped this session (for rest breaks)
    _next_coffee_break: int = 0  # Property count for next coffee break
    _total_session_count: int = 0  # Total across rest breaks
    
    # Checkpoint saving state
    _last_checkpoint_idx: int = 0  # Index of last saved property
    _checkpoint_interval: int = 50  # Save every N properties
    _target_file: Optional[str] = None  # Cached target filename for checkpoints
    
    def __post_init__(self):
        self._stop_evt = asyncio.Event()
        self._pause_evt = asyncio.Event()
        self._pause_evt.set()  # Not paused initially
        self.scraped_properties = []
        self._processed = set()
        self._inflight = set()
        self._recent = {}
        self._index_map = {}
    
    def log(self, level: str, message: str):
        """Log a message and send to callback if set."""
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
            self.log("STEALTH", f"Simulating reading time: {reading_time:.1f}s ({char_count} chars)")
            await asyncio.sleep(reading_time)
    
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
            self.log("STEALTH", f"Mouse movement: {num_moves} random positions")
            for i in range(num_moves):
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
                self.log("STEALTH", "Variable scroll: scrolling up first")
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
                    self.log("STEALTH", f"Variable scroll: mid-scroll pause {pause_time:.1f}s")
                    await asyncio.sleep(pause_time)
            
            # Sometimes scroll back up slightly
            if random.random() < 0.2:
                self.log("STEALTH", "Variable scroll: scrolling back up")
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
            self.log("STEALTH", f"Coffee break starting: {break_duration:.0f}s pause")
            
            if self.on_status:
                self.on_status("resting", duration=int(break_duration))
            
            # Log countdown every 10 seconds
            remaining = break_duration
            while remaining > 0:
                sleep_chunk = min(10, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
                if remaining > 0:
                    self.log("STEALTH", f"Coffee break: {remaining:.0f}s remaining...")
            
            if self.on_status:
                self.on_status("running")
            
            # Schedule next coffee break
            self._next_coffee_break = self._session_property_count + random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
            self.log("STEALTH", f"Coffee break ended. Next break in ~{self._next_coffee_break - self._session_property_count} properties")
    
    async def maybe_session_rest(self):
        """Take a long rest after session limit (Extra Stealth only)."""
        if self.mode != "stealth":
            return
        
        if self._session_property_count >= EXTRA_STEALTH_SESSION_LIMIT:
            rest_duration = random.uniform(*EXTRA_STEALTH_REST_DURATION_RANGE)
            # Round to nearest minute for cleaner display
            rest_duration = round(rest_duration / 60) * 60
            rest_mins = int(rest_duration // 60)
            self.log("STEALTH", f"Session limit reached ({EXTRA_STEALTH_SESSION_LIMIT} properties). Resting for {rest_mins} minutes...")
            
            if self.on_status:
                self.on_status("resting", duration=int(rest_duration))
            
            # Log countdown every minute
            remaining = rest_duration
            while remaining > 0:
                sleep_chunk = min(60, remaining)
                await asyncio.sleep(sleep_chunk)
                remaining -= sleep_chunk
                if remaining > 0:
                    remaining_mins = int(remaining // 60)
                    self.log("STEALTH", f"Session rest: {remaining_mins} minutes remaining...")
            
            if self.on_status:
                self.on_status("running")
            
            # Reset session counter
            self._total_session_count += self._session_property_count
            self._session_property_count = 0
            self._next_coffee_break = random.randint(*EXTRA_STEALTH_COFFEE_BREAK_FREQUENCY)
            self.log("STEALTH", f"Session rest complete. Total scraped: {self._total_session_count}. Starting new session...")
    
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
        self._stop_evt.set()
        self._pause_evt.set()  # Unpause to allow graceful stop
        self.status = "stopping"
        self.log("INFO", "Stopping scraper...")
        if self.on_status:
            self.on_status("stopping")
    
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
    
    async def _save_checkpoint(self, additions: List[dict], target_file: Optional[str], existing_df, carry_cols: Set[str]):
        """Save checkpoint - incremental save of new properties since last checkpoint.
        
        This saves only the new properties (from _last_checkpoint_idx to current)
        to the Excel file, preserving the worksheet structure by Distrito.
        """
        if not additions or len(additions) <= self._last_checkpoint_idx:
            return
        
        # Calculate how many new properties to save
        batch_start = self._last_checkpoint_idx
        batch_end = len(additions)
        batch_count = batch_end - batch_start
        
        self.log("INFO", f"💾 Checkpoint: Saving batch of {batch_count} properties ({batch_start+1} to {batch_end})...")
        
        try:
            # Determine output path
            if target_file:
                out_effective = os.path.join(self.output_dir, target_file)
            elif additions:
                ciudad = additions[0].get("Ciudad")
                category = self._detected_sheet or "unknown"
                if ciudad:
                    ciudad_clean = sanitize_filename_part(ciudad)
                    out_effective = f"idealista_{ciudad_clean}_{category}.xlsx"
                else:
                    out_effective = f"idealista_{category}.xlsx"
                out_effective = os.path.join(self.output_dir, out_effective)
            else:
                return
            
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)
            
            # Get only the new additions since last checkpoint
            new_batch = additions[batch_start:batch_end]
            
            # Load current state from file for merging
            checkpoint_existing_df, _, _ = load_existing_single_sheet(out_effective, self._detected_sheet or self.sheet_name)
            
            # Export with split by Distrito (this handles merging internally)
            export_split_by_distrito(checkpoint_existing_df, new_batch, out_effective, carry_cols=carry_cols)
            
            # Update checkpoint index
            self._last_checkpoint_idx = batch_end
            self.log("OK", f"💾 Checkpoint saved! Total: {self.current_property_count} properties in {os.path.basename(out_effective)} (of which {batch_count} are new)")
            
        except Exception as e:
            self.log("WARN", f"Checkpoint save failed: {e} - will retry at next checkpoint")
    
    async def _goto_with_retry(self, page, url: str) -> None:
        """Navigate to URL with retry logic. Detects browser close."""
        delay = RETRY_BASE_DELAY
        last_err: Optional[Exception] = None
        for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
            try:
                await page.goto(url, wait_until=GOTO_WAIT_UNTIL, timeout=60000)
                
                # Check for CAPTCHA/Bot protection
                try:
                    title = await page.title()
                    t_lower = title.lower()
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
            "verification" in t_lower
        )

                    if is_captcha:
                        self.log("WARN", f"CAPTCHA DETECTED on {url} (Title: '{title}')")
                        self.log("WARN", ">>> PLEASE SOLVE THE CAPTCHA MANUALLY IN THE BROWSER <<<")
                        if self.on_status:
                            self.on_status("captcha")
                        
                        # Loop until resolved
                        while True:
                            play_captcha_alert()
                            await asyncio.sleep(10.0)
                            
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

                await asyncio.sleep(3.0)
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
                await asyncio.sleep(delay)
                delay *= 2
        if last_err:
            raise last_err
    
    async def _wait_for_pause(self):
        """Wait if paused."""
        while not self._pause_evt.is_set() and not self._stop_evt.is_set():
            await asyncio.sleep(0.1)
    
    async def run(self):
        """Main scraping loop."""
        self.is_running = True
        self.status = "running"
        self._stop_evt.clear()
        self._pause_evt.set()
        
        if self.on_status:
            self.on_status("running")
        
        self.log("INFO", f"Starting scraper in {self.mode.upper()} mode")
        self.log("INFO", f"Seed URL: {self.seed_url}")
        
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
        else:
            self.log("INFO", "New seed URL - no previous scrape history found")
        
        additions: List[dict] = []
        expired_urls: List[str] = []  # URLs to delete from Excel (expired listings)
        
        async with async_playwright() as pw:
            self.log("INFO", "Launching browser...")
            try:
                browser = await pw.chromium.launch(
                    headless=False,
                    args=["--start-minimized", "--window-size=1280,900", "--disable-blink-features=AutomationControlled"]
                )
                self.log("OK", "Browser launched successfully!")
            except Exception as e:
                self.log("ERR", f"Could not launch browser: {e}")
                self.log("ERR", "Run: python -m playwright install chromium")
                self.is_running = False
                self.status = "error"
                if self.on_status:
                    self.on_status("error", error=str(e))
                return
            
            # Use random user-agent and viewport for Extra Stealth mode
            if self.mode == "stealth":
                user_agent = self.get_random_user_agent()
                viewport = self.get_random_viewport()
                # Extract browser name from user agent for cleaner logging
                ua_short = "Chrome" if "Chrome" in user_agent else "Firefox" if "Firefox" in user_agent else "Safari" if "Safari" in user_agent else "Unknown"
                self.log("STEALTH", f"Browser fingerprint rotation: {viewport['width']}x{viewport['height']} viewport, {ua_short} user-agent")
            else:
                user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                viewport = {"width": 1920, "height": 1080}
            
            ctx = await browser.new_context(
                no_viewport=True,  # Use window size instead of fixed viewport
                user_agent=user_agent
            )
            page = await ctx.new_page()
            
            # Navigate to seed URL
            try:
                self.log("INFO", f"Navigating to seed URL...")
                await page.goto(self.seed_url, wait_until="domcontentloaded", timeout=60000)
                await asyncio.sleep(3.0)
                self.log("OK", "Opened seed URL")
            except Exception as e:
                self.log("ERR", f"Could not open seed URL: {e}")
            
            # Detect sheet name and total properties from page
            self.log("INFO", "Waiting for page to load property count...")
            h1txt = ""
            total_count = 0
            
            # Retry h1 extraction up to 3 times (page may navigate/redirect)
            for attempt in range(3):
                try:
                    # Wait for page to stabilize
                    await asyncio.sleep(2.0)
                    
                    # Wait for network to be idle
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                    
                    # Try to get h1 text
                    h1txt = await page.evaluate(r"""() => {
                        const el = document.querySelector('#h1-container__text') || 
                                   document.querySelector('#h1-container') || 
                                   document.querySelector('h1');
                        return el ? el.textContent.trim() : '';
                    }""") or ""
                    
                    if h1txt:
                        self.log("INFO", f"H1 text: '{h1txt[:100]}'")
                        break  # Success, exit retry loop
                    
                except Exception as e:
                    if attempt < 2:
                        self.log("WARN", f"H1 extraction attempt {attempt+1} failed: {e}. Retrying...")
                        await asyncio.sleep(1.0)
                    else:
                        self.log("WARN", f"H1 extraction failed after 3 attempts: {e}")
            
            # Extract count from h1 text
            if h1txt:
                # Look for patterns like "2.055 viviendas"
                match = re.search(r'(\d{1,3}(?:\.\d{3})*)\s*(?:vivienda|pisos?|casas?|inmuebles?|anuncios?)', h1txt, re.IGNORECASE)
                if match:
                    total_count = int(match.group(1).replace('.', ''))
                    self.log("INFO", f"Extracted count from h1: {total_count}")
            
            # If still no count, log warning
            if total_count == 0:
                self.log("WARN", "Could not extract count from h1, using 0")
            
            # Detect alquiler/venta from h1 text
            h1_lower = h1txt.lower()
            if "alquiler" in h1_lower:
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
                if "alquiler" in url_lower:
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
            
            # target_file and url_dates already set from registry lookup above
            # Only load url_dates if not already preloaded and target_file exists
            
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
            
            while not self._stop_evt.is_set():
                await self._wait_for_pause()
                if self._stop_evt.is_set():
                    break
                
                # Navigate to listing page
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
                        await asyncio.sleep(3.0)
                        
                except BrowserClosedException:
                    break
                except Exception as e:
                    self.log("ERR", f"Failed to open listing page: {e}")
                    break
                
                # Wait for content and scroll
                try:
                    await page.wait_for_selector("article, .item, [data-element-id]", timeout=10000, state="visible")
                    await asyncio.sleep(2.0)
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
                            page_title = await page.title()
                            page_url = page.url
                            self.log("INFO", f"Current URL: {page_url}")
                            self.log("INFO", f"Page title: {page_title}")
                            
                            # Check for CAPTCHA indicators
                            captcha_check = await page.evaluate(r"""() => {
                                const body = document.body.innerText.toLowerCase();
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
                                await asyncio.sleep(60)  # Give user time to solve CAPTCHA
                                # Try collecting links again after waiting
                                hrefs = await page.evaluate(js_collect)
                                if hrefs:
                                    self.log("OK", f"After waiting, found {len(hrefs)} links!")
                            else:
                                self.log("WARN", "No CAPTCHA, but no links either. Keeping browser open 30s for inspection...")
                                await asyncio.sleep(30)  # Keep open for inspection
                                
                        except Exception as debug_e:
                            self.log("ERR", f"Debug check failed: {debug_e}")
                    
                    # Still no links after debug - exit
                    if not hrefs:
                        self.log("INFO", f"End of listings at page {page_num}.")
                        break
                
                self.log("INFO", f"Page {page_num}: Found {len(hrefs)} properties to scrape")
                
                skipped_on_page = 0
                
                # Update page progress
                self.current_page = page_num
                self.emit_progress()
                
                # Scrape each property on this page
                for href in hrefs:
                    await self._wait_for_pause()
                    if self._stop_evt.is_set():
                        break
                    
                    property_idx += 1
                    key = canonical_listing_url(href)
                    
                    if key in self._processed:
                        # Update counter for skipped properties too, so UI shows correct count
                        skipped_on_page += 1
                        self.current_property_count = property_idx
                        self.emit_progress()
                        continue
                    
                    try:
                        await asyncio.sleep(random.uniform(*card_delay))
                        await self._goto_with_retry(page, href)
                        await asyncio.sleep(random.uniform(*post_card_delay))
                        
                        # If this is the first property, determine target file
                        if target_file is None:
                            await page.wait_for_timeout(PAGE_WAIT_MS)
                            d = await extract_detail_fields(page, debug_items=False)
                            row = {"URL": key, **d}
                            
                            # Build target filename: idealista_[Ciudad]_[venta/alquiler].xlsx
                            # Prefer city from listing h1 header, fall back to property's Ciudad
                            ciudad = self._detected_city or row.get("Ciudad")
                            category = self._detected_sheet or "unknown"
                            
                            if ciudad:
                                ciudad_clean = sanitize_filename_part(ciudad)
                                target_file = f"idealista_{ciudad_clean}_{category}.xlsx"
                            else:
                                target_file = f"idealista_{category}.xlsx"
                            
                            target_path = os.path.join(self.output_dir, target_file)
                            self.log("INFO", f"Target Excel file: {target_path}")
                            
                            # Load existing URLs from this file
                            url_dates = load_urls_with_dates(target_path)
                            self.log("INFO", f"Loaded {len(url_dates)} existing URLs from file")
                            
                            # Process first property - check for missing fields (CAPTCHA)
                            miss = missing_fields(row)
                            if miss:
                                self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA detectado. Resuelve el CAPTCHA y pulsa Resume.")
                                
                                if self.on_status:
                                    self.on_status("captcha")
                                
                                # Pause and wait indefinitely for user to solve CAPTCHA and resume
                                self._pause_evt.clear()  # Pause the scraper
                                wait_start = asyncio.get_running_loop().time()
                                
                                while not self._stop_evt.is_set():
                                    # Wait for resume signal
                                    await self._wait_for_pause()
                                    
                                    if self._stop_evt.is_set():
                                        self.save_state(page_num, target_file)
                                        break
                                    
                                    # User resumed - retry extraction
                                    d = await extract_detail_fields(page, debug_items=False)
                                    row = {"URL": key, **d}
                                    miss = missing_fields(row)
                                    
                                    if not miss:
                                        elapsed = int(asyncio.get_running_loop().time() - wait_start)
                                        self.log("OK", f"CAPTCHA resuelto! (esperado {elapsed}s)")
                                        if self.on_status:
                                            self.on_status("running")
                                        break
                                    else:
                                        # Still CAPTCHA - pause again
                                        self.log("WARN", "CAPTCHA aun presente. Resuelve y pulsa Resume de nuevo.")
                                        if self.on_status:
                                            self.on_status("captcha")
                                        self._pause_evt.clear()
                                
                                if miss and self._stop_evt.is_set():
                                    self.log("WARN", f"First property CAPTCHA - stopped by user: {key}")
                            
                            if not miss:
                                # Add scraping date
                                from datetime import datetime
                                row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                                
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
                        
                        # Check if URL already exists in Excel
                        if key in url_dates:
                            # Still need to visit to check if listing is expired
                            await page.wait_for_timeout(PAGE_WAIT_MS)
                            d = await extract_detail_fields(page, debug_items=False)
                            
                            # Check if listing is expired
                            if d.get("_isExpired"):
                                self.log("WARN", f"({property_idx}/{self.total_properties_expected}) [EXPIRED] Anuncio eliminado: {key}")
                                expired_urls.append(key)
                                self._processed.add(key)
                                self.current_property_count = property_idx
                                self.emit_progress()
                                continue
                            
                            # Normal skip - listing still exists
                            self.log("INFO", f"({property_idx}/{self.total_properties_expected}) [SKIP] Ya existe: {key}")
                            skipped += 1
                            self._processed.add(key)
                            self.current_property_count = property_idx
                            self.emit_progress()
                            continue
                        
                        new_scraped += 1
                        
                        # Scrape the property
                        await page.wait_for_timeout(PAGE_WAIT_MS)
                        d = await extract_detail_fields(page, debug_items=False)
                        
                        row = {"URL": key, **d}
                        miss = missing_fields(row)
                        
                        # Check if this is a "listing not found" page (not a CAPTCHA)
                        if miss:
                            page_text = await page.evaluate("() => document.body.innerText || ''")
                            is_not_found = (
                                "no encontramos" in page_text.lower() or
                                "anuncio no disponible" in page_text.lower() or
                                "este anuncio ya no está disponible" in page_text.lower() or
                                "enlace antiguo" in page_text.lower()
                            )
                            
                            if is_not_found:
                                # Listing is unavailable - skip without pausing for CAPTCHA
                                self.log("WARN", f"({property_idx}/{self.total_properties_expected}) Anuncio no disponible: {key}")
                                self._processed.add(key)
                                self.current_property_count = property_idx
                                self.emit_progress()
                                continue
                        
                        # If missing fields and not a "not found" page, might be CAPTCHA
                        if miss:
                            self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA detectado. Resuelve el CAPTCHA y pulsa Resume.")
                            
                            if self.on_status:
                                self.on_status("captcha")
                            
                            # Pause and wait indefinitely for user to solve CAPTCHA and resume
                            self._pause_evt.clear()  # Pause the scraper
                            wait_start = asyncio.get_running_loop().time()
                            
                            while not self._stop_evt.is_set():
                                # Wait for resume signal
                                await self._wait_for_pause()
                                
                                if self._stop_evt.is_set():
                                    self.save_state(page_num, target_file)
                                    break
                                
                                # User resumed - retry extraction
                                d = await extract_detail_fields(page, debug_items=False)
                                row = {"URL": key, **d}
                                miss = missing_fields(row)
                                
                                if not miss:
                                    elapsed = int(asyncio.get_running_loop().time() - wait_start)
                                    self.log("OK", f"({property_idx}/{self.total_properties_expected}) CAPTCHA resuelto! (esperado {elapsed}s)")
                                    if self.on_status:
                                        self.on_status("running")
                                    break
                                else:
                                    # Still CAPTCHA - pause again
                                    self.log("WARN", "CAPTCHA aun presente. Resuelve y pulsa Resume de nuevo.")
                                    if self.on_status:
                                        self.on_status("captcha")
                                    self._pause_evt.clear()
                            
                            if self._stop_evt.is_set():
                                break
                            
                            if miss:
                                self.log("WARN", f"({property_idx}/{self.total_properties_expected}) CAPTCHA - stopped: {key}")
                                self._processed.add(key)
                                continue
                        
                        # Add scraping date in dd/mm/yyyy format
                        from datetime import datetime
                        row["Fecha Scraping"] = datetime.now().strftime("%d/%m/%Y")
                        
                        additions.append(row)
                        self.scraped_properties.append(row)
                        self._processed.add(key)
                        
                        # Checkpoint saving: save every 100 properties
                        if len(additions) > 0 and len(additions) % self._checkpoint_interval == 0:
                            await self._save_checkpoint(additions, target_file, existing_df, carry_cols=set())
                        
                        # Extra Stealth: Simulate reading time
                        await self.simulate_reading_time(row.get("Descripción"))
                        
                        # Extra Stealth: Mouse movement simulation
                        await self.simulate_mouse_movement(page)
                        
                        # Extra Stealth: Increment session counter and check for breaks
                        if self.mode == "stealth":
                            self._session_property_count += 1
                            await self.maybe_coffee_break()
                            await self.maybe_session_rest()
                        
                        if key not in url_dates:
                            self.log("OK", f"({property_idx}/{self.total_properties_expected}) Scraped: {key}")
                        
                        if self.on_property:
                            self.on_property(row)
                        
                        self.current_property_count = len(self.scraped_properties)
                        self.emit_progress()
                        
                    except BrowserClosedException:
                        # Save state for resume before exiting
                        self.save_state(page_num, target_file)
                        break
                    except Exception as e:
                        self.log("ERR", f"({property_idx}/{self.total_properties_expected}) {key} -> {e}")
                        self._processed.add(key)
                
                # Check if we should continue to next page
                if self._stop_evt.is_set():
                    # Save state for resume before stopping
                    self.save_state(page_num, target_file)
                    break
                    
                if len(hrefs) < LISTING_LINKS_PER_PAGE_MAX:
                    self.log("INFO", f"Last page reached (only {len(hrefs)} links).")
                    # Clear state on successful completion
                    self.clear_state()
                    break
                # Check if we skipped everything on this page
                if len(hrefs) > 0 and skipped_on_page == len(hrefs):
                    self.log("WARN", f"Página {page_num}: todas las propiedades ya existen en el fichero")
                    # Wait a bit longer to let things settle
                    await asyncio.sleep(5.0)

                # Check if we've reached the maximum page limit (60 pages)
                if page_num >= 60:
                    self.log("INFO", f"Reached page {page_num} (maximum listing pages). Finishing scrape.")
                    self.clear_state()
                    break

                # Next page
                page_num += 1
            
            self.log("INFO", f"Summary: {new_scraped} new, {updated} updated, {skipped} skipped, {len(expired_urls)} expired")
            self.log("INFO", "Scraping finished. Closing browser...")
            await browser.close()
        
        # Export data
        self.log("INFO", "Exporting data to Excel...")
        
        # Use filename from Phase 2 if available, otherwise build it
        if target_file:
            out_effective = os.path.join(self.output_dir, target_file)
        elif additions:
            # Build filename: idealista_[Ciudad]_[venta/alquiler].xlsx
            ciudad = additions[0].get("Ciudad")
            category = self._detected_sheet or "unknown"
            
            if ciudad:
                ciudad_clean = sanitize_filename_part(ciudad)
                out_effective = f"idealista_{ciudad_clean}_{category}.xlsx"
            else:
                out_effective = f"idealista_{category}.xlsx"
            out_effective = os.path.join(self.output_dir, out_effective)
        else:
            out_effective = os.path.join(self.output_dir, f"idealista_{self._detected_sheet or 'unknown'}.xlsx")
        
        self.log("INFO", f"Output path: {out_effective}")
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Load ALL existing data from all sheets and export with split by Distrito
        existing_df, _, _ = load_existing_single_sheet(out_effective, self._detected_sheet or self.sheet_name)
        self.log("INFO", f"Loaded {len(existing_df)} existing rows from file")
        
        # Delete expired URLs from existing data
        if expired_urls and not existing_df.empty and "URL" in existing_df.columns:
            initial_count = len(existing_df)
            existing_df = existing_df[~existing_df["URL"].isin(expired_urls)]
            deleted_count = initial_count - len(existing_df)
            if deleted_count > 0:
                self.log("OK", f"Deleted {deleted_count} expired listings from Excel")
        
        export_split_by_distrito(existing_df, additions, out_effective, carry_cols=set())
        
        self.output_file = os.path.abspath(out_effective)
        self.log("OK", f"Saved {len(additions)} new/updated rows to {self.output_file}")
        
        # Register this scrape in the history registry
        total_properties = len(existing_df) + len(additions) if existing_df is not None else len(additions)
        register_scrape(
            self.seed_url,
            os.path.basename(out_effective),
            total_properties,
            page_num
        )
        self.log("INFO", f"Registered scrape: {os.path.basename(out_effective)} ({total_properties} properties)")
        
        # Clear resume state file on successful completion
        self.clear_state()
        self.log("INFO", "Resume state cleared (scraping completed successfully)")
        
        self.is_running = False
        self.status = "completed"
        if self.on_status:
            self.on_status("completed", file=self.output_file, count=len(self.scraped_properties))
