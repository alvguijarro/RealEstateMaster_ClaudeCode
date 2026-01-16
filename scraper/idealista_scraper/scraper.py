"""Main scraping orchestration and session state."""
from __future__ import annotations

import asyncio
import random
import re
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlsplit, urlunsplit

from playwright.async_api import async_playwright

from .config import (
    HARVEST_DEBOUNCE_SECONDS, PAGE_WAIT_MS, RETRY_MAX_ATTEMPTS, RETRY_BASE_DELAY,
    GOTO_WAIT_UNTIL, SCROLL_STEPS, SCROLL_PAUSE_RANGE, LISTING_LINKS_PER_PAGE_MAX
)
from .utils import log, same_domain, canonical_listing_url, is_listing_url, sanitize_filename_part, play_captcha_alert
from .extractors import extract_detail_fields, missing_fields
from .excel_writer import (
    load_existing_single_sheet, load_existing_specific_sheet, export_single_sheet,
    load_urls_with_dates
)

def build_paginated_url(seed_url: str, page_number: int) -> str:
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

async def _goto_with_retry(page, url: str) -> None:
    """Navigate to URL with retry logic and proper content loading."""
    delay = RETRY_BASE_DELAY
    last_err: Optional[Exception] = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            # Use domcontentloaded for faster, more reliable navigation
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
                    log("WARN", f"CAPTCHA DETECTED on {url} (Title: '{title}')")
                    log("WARN", ">>> PLEASE SOLVE THE CAPTCHA MANUALLY IN THE BROWSER <<<")
                    
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
                                log("OK", "CAPTCHA solved! Resuming...")
                                break
                        except Exception:
                            pass
            except Exception as e:
                # If checking title fails, just ignore
                pass

            # Wait a fixed time for JavaScript to render content
            await asyncio.sleep(3.0)
            return
        except Exception as e:
            last_err = e
            log("WARN", f"goto attempt {attempt}/{RETRY_MAX_ATTEMPTS} failed for {url}: {e}")
            await asyncio.sleep(delay)
            delay *= 2
    if last_err:
        raise last_err

@dataclass
class ScraperSession:
    cdp_endpoint: str
    out_xlsx: str
    sheet_name: str
    debug_items: bool = False
    seed_url: str = ""
    additions: List[dict] = field(default_factory=list)
    processed: Set[str] = field(default_factory=set)
    inflight: Set[str] = field(default_factory=set)
    recent: Dict[str, float] = field(default_factory=dict)
    index_map: Dict[str, Tuple[int, int]] = field(default_factory=dict)
    detected_sheet: Optional[str] = None

    async def auto_browse_seed(self, page, stop_evt: asyncio.Event) -> None:
        visited_cards: Set[str] = set()
        page_num = 1
        while not stop_evt.is_set():
            list_url = build_paginated_url(self.seed_url, page_num)
            log("INFO", f"Opening listing page {page_num}: {list_url}")
            try:
                await _goto_with_retry(page, list_url)
            except Exception as e:
                log("ERR", f"Failed to open {list_url}: {e}")
                break

            # Wait for page content to load - critical for JavaScript-rendered pages
            try:
                log("DEBUG", "Waiting for page content to load...")
                # Wait for property cards to appear (adjust selector based on actual structure)
                await page.wait_for_selector("article, .item, [data-element-id]", timeout=10000, state="visible")
                # Additional wait for JavaScript to finish rendering
                await asyncio.sleep(2.0)
            except Exception as e:
                log("WARN", f"Timeout waiting for content: {e}")
                # Continue anyway, maybe content is there but selector didn't match

            # Scroll to trigger lazy-loaded content
            try:
                for _ in range(SCROLL_STEPS):
                    await page.evaluate('window.scrollBy(0, document.body.scrollHeight / 3)')
                    await asyncio.sleep(random.uniform(*SCROLL_PAUSE_RANGE))
                # Final wait after scrolling
                await asyncio.sleep(1.5)
            except Exception:
                pass

            # More aggressive link collection with debug info
            js_collect = r'''(()=>{const A=[...document.querySelectorAll("a[href*='/inmueble']")];const U=A.map(a=>new URL(a.getAttribute("href")||a.href,location.origin).href).filter(u=>/\/inmueble[s]?\/\d+/.test(u));return [...new Set(U)].slice(0,%d)})()''' % LISTING_LINKS_PER_PAGE_MAX
            try:
                hrefs: List[str] = await page.evaluate(js_collect)
            except Exception as e:
                log("WARN", f"Error collecting links: {e}")
                hrefs = []
            
            # Enhanced logging
            if len(hrefs) == 0:
                log("WARN", f"No property links found on page {page_num}. This might indicate:")
                log("WARN", "  1. The page structure has changed")
                log("WARN", "  2. The page requires JavaScript to load (already scrolled)")
                log("WARN", "  3. This is not a listing page")
                # Try to get page title for debugging
                try:
                    title = await page.title()
                    log("DEBUG", f"Page title: {title}")
                except Exception:
                    pass
            
            log("INFO", f"Page {page_num}: {len(hrefs)} property links.")

            self.index_map.clear()
            total = len(hrefs)
            for idx, u in enumerate(hrefs, start=1):
                self.index_map[canonical_listing_url(u)] = (idx, total)

            for card_url in hrefs:
                if stop_evt.is_set():
                    break
                if card_url in visited_cards:
                    continue
                visited_cards.add(card_url)
                try:
                    await asyncio.sleep(random.uniform(0.8, 2.0))
                    await _goto_with_retry(page, card_url)
                    await asyncio.sleep(random.uniform(1.2, 3.0))
                    try:
                        await page.evaluate('window.scrollBy(0, 600)')
                    except Exception:
                        pass
                    await _goto_with_retry(page, list_url)
                except Exception as e:
                    log("ERR", f"Error visiting {card_url}: {e}")
                    try:
                        await _goto_with_retry(page, list_url)
                    except Exception:
                        pass

            if total < LISTING_LINKS_PER_PAGE_MAX:
                log("INFO", f"Reached last page with {total} links (<{LISTING_LINKS_PER_PAGE_MAX}). Finishing crawl.")
                stop_evt.set()
                break

            page_num += 1
        log("INFO", "Auto-crawl finished.")

    async def listen_and_collect(self):
        # Check if Excel file can be read (prompt user to close it if open)
        import os
        if os.path.exists(self.out_xlsx):
            while True:
                try:
                    # Try to open file for reading to check if it's accessible
                    with open(self.out_xlsx, 'r+b'):
                        pass
                    break
                except PermissionError:
                    log("WARN", f"Cannot read '{self.out_xlsx}'. The file appears to be open in Excel.")
                    resp = input("Please close the Excel file and press ENTER to continue (or 'q' to abort): ").strip().lower()
                    if resp == 'q':
                        log("INFO", "Aborted by user.")
                        return
                except Exception:
                    break  # File doesn't exist or other error, continue anyway
        
        existing_df, seen_urls, carry_cols = load_existing_single_sheet(self.out_xlsx, self.sheet_name)
        
        # Load URLs with their last-updated dates for smart deduplication
        url_dates = load_urls_with_dates(self.out_xlsx)
        log("INFO", f"Loaded {len(url_dates)} existing URLs from Excel for deduplication check.")
        updated_rows = []  # Track rows that need updating (different dates)

        async def maybe_harvest(page):
            url = page.url or ""
            if not is_listing_url(url):
                return
            key = canonical_listing_url(url)
            now = asyncio.get_running_loop().time()
            if now - self.recent.get(key, 0) < HARVEST_DEBOUNCE_SECONDS:
                return
            self.recent[key] = now
            if key in self.inflight:
                return
            self.inflight.add(key)
            try:
                if key in self.processed:
                    return
                
                # Quick check: extract just the date first
                try:
                    page_date = await page.evaluate("""() => {
                        const el = document.querySelector('.date-update-text');
                        return el ? el.textContent.trim() : null;
                    }""")
                except Exception:
                    page_date = None
                
                # Smart deduplication: check if URL exists with same date
                if key in url_dates:
                    existing_date = url_dates.get(key, "")
                    if page_date and existing_date and page_date.strip() == existing_date.strip():
                        log("INFO", f"[already exists - skipping] {key}")
                        self.processed.add(key)
                        return
                    else:
                        # Date is different - need to update
                        log("INFO", f"[date changed - updating] {key}")

                await page.wait_for_timeout(PAGE_WAIT_MS)
                d = await extract_detail_fields(page, debug_items=self.debug_items)

                row = {"URL": key, **d}
                miss = missing_fields(row)
                if miss:
                    self.processed.add(key)
                    return

                self.additions.append(row)
                self.processed.add(key)
                seen_urls.add(key)

                pos = self.index_map.get(key)
                if pos:
                    i, N = pos
                    log("OK", f"scrape OK: ({i}/{N}) {key}")
                    if N < LISTING_LINKS_PER_PAGE_MAX and i == N:
                        log("INFO", f"Finished last page with {N} links (<{LISTING_LINKS_PER_PAGE_MAX}). Stopping crawl.")
                        stop_evt.set()
                else:
                    log("OK", f"scrape OK: {key}")

            except Exception as e:
                log("ERR", f"{key} -> exception: {e}")
                self.processed.add(key)
            finally:
                self.inflight.discard(key)

        async with async_playwright() as pw:
            # Launch a fresh browser instance (not connected to existing Chrome)
            log("INFO", "Launching new browser window...")
            
            # Check if we should run headless (cloud/Docker environment)
            import os
            is_headless = os.environ.get('HEADLESS', '').lower() in ('1', 'true', 'yes')
            if is_headless:
                log("INFO", "Running in headless mode (cloud environment)")
            
            try:
                browser = await pw.chromium.launch(
                    headless=is_headless,  # Headless in cloud, visible locally
                    args=[
                        "--start-maximized",
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",  # Required for Docker
                        "--disable-dev-shm-usage",  # Required for Docker
                    ]
                )
                log("INFO", "Browser launched successfully!")
            except Exception as e:
                log("ERR", f"Could not launch browser: {e}")
                log("ERR", "Run: python -m playwright install chromium")
                return
            
            # Create a new context and page
            ctx = await browser.new_context(
                viewport={"width": 1920, "height": 1080},
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            )
            page = await ctx.new_page()
            
            # Navigate to the seed URL
            try:
                if self.seed_url and same_domain(self.seed_url):
                    log("INFO", f"Navigating to: {self.seed_url}")
                    await page.goto(self.seed_url, wait_until="domcontentloaded", timeout=60000)
                    await asyncio.sleep(3.0)  # Wait for JavaScript to render
                    log("INFO", f"Opened seed URL: {self.seed_url}")
            except Exception as e:
                log("ERR", f"Could not open seed URL: {e}")

            try:
                h1txt = await ctx.pages[-1].evaluate("""() => {
                    const h = document.querySelector('#h1-container');
                    return h ? h.textContent.trim() : null;
                }""")
                f = (h1txt or "").lower()
                if "alquiler" in f:
                    self.detected_sheet = "alquiler"
                elif "venta" in f:
                    self.detected_sheet = "venta"
            except Exception:
                self.detected_sheet = None
            if self.detected_sheet:
                log("INFO", f"Detected worksheet: '{self.detected_sheet}' from h1-container.")
            else:
                self.detected_sheet = self.sheet_name

            stop_evt = asyncio.Event()

            def hook_page(p):
                p.on("framenavigated", lambda fr: asyncio.create_task(maybe_harvest(p)) if fr == p.main_frame else None)

            for p in ctx.pages:
                hook_page(p)
            ctx.on("page", lambda p: hook_page(p))

            if ctx.pages:
                await maybe_harvest(ctx.pages[-1])

            async def stdin_loop():
                log("INFO", "Type 'q' then ENTER to stop and export.")
                try:
                    import msvcrt  # Windows
                    while not stop_evt.is_set():
                        if msvcrt.kbhit():
                            ch = msvcrt.getwch()
                            if ch and ch.lower() == "q":
                                stop_evt.set()
                                break
                        await asyncio.sleep(0.1)
                except Exception:
                    loop = asyncio.get_running_loop()
                    fut = loop.create_future()
                    def _on_stdin():
                        try:
                            line = sys.stdin.readline()
                            if not fut.done():
                                fut.set_result(line)
                        except Exception:
                            if not fut.done():
                                fut.set_result("")
                    try:
                        loop.add_reader(sys.stdin, _on_stdin)
                    except NotImplementedError:
                        while not stop_evt.is_set():
                            line = await asyncio.to_thread(sys.stdin.readline)
                            if line and line.strip().lower() == "q":
                                stop_evt.set()
                                break
                            await asyncio.sleep(0.1)
                        return
                    try:
                        while not stop_evt.is_set():
                            try:
                                line = await asyncio.wait_for(fut, timeout=0.2)
                            except asyncio.TimeoutError:
                                continue
                            if line and line.strip().lower() == "q":
                                stop_evt.set()
                                break
                            fut = loop.create_future()
                    finally:
                        try:
                            loop.remove_reader(sys.stdin)
                        except Exception:
                            pass

            stdin_task = asyncio.create_task(stdin_loop())
            crawl_task = asyncio.create_task(self.auto_browse_seed(ctx.pages[-1], stop_evt))

            done, pending = await asyncio.wait({stdin_task, crawl_task}, return_when=asyncio.FIRST_COMPLETED)
            stop_evt.set()
            for t in pending:
                t.cancel()
            await asyncio.sleep(0.5)

        out_effective = self.out_xlsx
        if self.additions:
            prov = self.additions[0].get("Provincia")
            if prov:
                out_effective = f"{sanitize_filename_part(prov)}_idealista.xlsx"
                log("INFO", f"Output filename set to '{out_effective}' based on province '{prov}'.")

        existing_df_target = load_existing_specific_sheet(out_effective, self.detected_sheet or self.sheet_name)
        export_single_sheet(existing_df_target, self.additions, out_effective, self.detected_sheet or self.sheet_name, carry_cols=set())
        log("INFO", f"Added {len(self.additions)} new rows this session.")
