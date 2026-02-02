"""Background Enrichment Worker for API-downloaded properties.

This script runs in the background and enriches properties downloaded via API
by visiting each URL and extracting the missing fields using the scraper's
extraction logic.

Features:
- Filters properties by price (≤300,000€ by default)
- Resumes from where it left off if interrupted
- Rate-limited to avoid detection
- Updates Excel files in-place with enriched data

Usage:
    python scripts/enrich_worker.py --input scraper/salidas/API_BATCH_*.xlsx
    python scripts/enrich_worker.py --input scraper/salidas/API_BATCH_Madrid_sale_*.xlsx --max-price 250000
"""
import sys
import os
import time
import random
import json
import argparse
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Optional, Set, List, Dict

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from playwright.async_api import async_playwright

# Import scraper components
from scraper.idealista_scraper.extractors import extract_detail_fields
from scraper.idealista_scraper.utils import log, simulate_human_interaction, play_captcha_alert
from scraper.idealista_scraper.excel_writer import export_split_by_distrito
from shared.config import API_MAX_PRICE

# Try to import stealth
try:
    from playwright_stealth import stealth_async
    HAS_STEALTH = True
except ImportError:
    HAS_STEALTH = False
    stealth_async = None

# =============================================================================
# CONFIGURATION
# =============================================================================
DEFAULT_MAX_PRICE = API_MAX_PRICE or 300000
ENRICH_STATE_FILE = PROJECT_ROOT / "scraper" / "salidas" / ".enrich_state.json"

# Rate limiting (conservative to avoid detection)
DELAY_BETWEEN_PAGES = (8, 20)  # seconds, randomized
DELAY_BETWEEN_BATCHES = (120, 300)  # 2-5 minutes between batches of 20
BATCH_SIZE = 20
SESSION_LIMIT = 100  # Properties per session before long break
SESSION_BREAK = (600, 1200)  # 10-20 minutes

# Fields that the API provides (we skip these during enrichment)
API_PROVIDED_FIELDS = {
    "Titulo", "price", "old price", "price change %", "Ubicacion",
    "actualizado hace", "m2 construidos", "habs", "banos", "Num plantas",
    "Terraza", "Garaje", "Trastero", "aire acond", "piscina", "jardin",
    "ascensor", "tipo", "altura", "exterior",
    "Calle", "Barrio", "Distrito", "Ciudad", "Provincia",
    "estado", "Descripcion", "URL", "Fecha Scraping", "Anuncio activo",
    "nombre anunciante"
}

# Fields that ONLY the scraper can provide
ENRICH_FIELDS = {
    "m2 utiles", "precio por m2", "orientacion", "construido en",
    "Consumo 1", "Consumo 2", "Emisiones 1", "Emisiones 2",
    "gastos comunidad", "Armarios", "Calefaccion", "parcela",
    "okupado", "Copropiedad", "con inquilino", "nuda propiedad", "ces. remate",
    "tipo anunciante", "Baja anuncio", "Comunidad Autonoma", "Zona"
}


def load_enrich_state() -> dict:
    """Load enrichment progress state."""
    if ENRICH_STATE_FILE.exists():
        try:
            with open(ENRICH_STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            pass
    return {"enriched_urls": [], "last_file": None, "last_index": 0}


def save_enrich_state(state: dict):
    """Save enrichment progress state."""
    ENRICH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(ENRICH_STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def find_api_files(pattern: str) -> List[Path]:
    """Find API batch files matching pattern."""
    import glob
    files = glob.glob(pattern)
    return [Path(f) for f in sorted(files) if f.endswith(".xlsx")]


def load_properties_to_enrich(file_path: Path, max_price: int, enriched_urls: Set[str]) -> pd.DataFrame:
    """Load properties from Excel that need enrichment."""
    log("INFO", f"Loading {file_path.name}...")
    
    # Read all sheets and combine
    sheets = pd.read_excel(file_path, sheet_name=None)
    if not sheets:
        return pd.DataFrame()
    
    df = pd.concat(sheets.values(), ignore_index=True)
    original_count = len(df)
    
    # Filter by price
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df[df["price"] <= max_price]
    
    final_count = len(df)
    
    # Filter out already enriched
    if "URL" in df.columns:
        df = df[~df["URL"].isin(enriched_urls)]
    
    to_process = len(df)
    log("INFO", f"Procesando {to_process} anuncios (Filtrados por precio <= {max_price}E y no enriquecidos)")
    
    return df


async def enrich_single_property(page, url: str) -> Optional[dict]:
    """Visit a URL and extract missing fields."""
    try:
        # Use domcontentloaded for faster, more reliable navigation
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # 1. IMMEDIATE BLOCK CHECK (Title)
        title = await page.title()
        t_lower = title.lower()
        if "uso indebido" in t_lower or "access denied" in t_lower:
            log("ERR", f"⛔ BLOQUEO DETECTADO (Título): {title}")
            return {"__blocked__": True}

        await simulate_human_interaction(page)
        
        # Check for CAPTCHA
        if any(kw in t_lower for kw in ["captcha", "robot", "verification", "challenge"]):
            log("WARN", f"CAPTCHA detected on {url}")
            play_captcha_alert()
            # Wait for manual resolution
            for _ in range(60):  # Wait up to 60 seconds
                await asyncio.sleep(1)
                new_title = await page.title()
                if "idealista" in new_title.lower() and "captcha" not in new_title.lower():
                    log("OK", "CAPTCHA resolved!")
                    break
            else:
                log("ERR", "CAPTCHA not resolved, skipping...")
                return None
        
        # 2. CONTENT BLOCK CHECK
        # Sometimes title is normal but body says "uso indebido"
        page_text = await page.evaluate("() => document.body ? document.body.innerText : ''")
        pt_lower = page_text.lower()
        if "uso indebido" in pt_lower or "access denied" in pt_lower or "se ha bloqueado" in pt_lower:
            log("ERR", "⛔ BLOQUEO DETECTADO (Contenido). Deteniendo.")
            return {"__blocked__": True}
        
        # Extract fields
        data = await extract_detail_fields(page)
        
        # Only return enrich fields
        enriched = {k: v for k, v in data.items() if k in ENRICH_FIELDS and v is not None}
        enriched["__enriched__"] = True
        enriched["Fecha Enriquecimiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
        
    except Exception as e:
        log("WARN", f"Error enriching {url}: {e}")
        return None


async def run_enrichment(files: List[Path], max_price: int, dry_run: bool = False):
    """Main enrichment loop."""
    state = load_enrich_state()
    enriched_urls = set(state.get("enriched_urls", []))
    
    log("INFO", f"Iniciando Enriquecedor (Precio max: {max_price}E)")
    
    # Collect all properties to enrich
    all_properties = []
    for file_path in files:
        df = load_properties_to_enrich(file_path, max_price, enriched_urls)
        if not df.empty:
            for _, row in df.iterrows():
                all_properties.append({
                    "file": file_path,
                    "url": row.get("URL"),
                    "row_data": row.to_dict()
                })
    
    if not all_properties:
        log("OK", "No hay inmuebles nuevos para enriquecer.")
        return
    
    log("INFO", f"Total a procesar: {len(all_properties)} inmuebles")
    
    # Randomize order to avoid sequential access patterns
    random.shuffle(all_properties)
    
    if dry_run:
        log("INFO", "DRY RUN - would process these URLs:")
        for prop in all_properties[:10]:
            log("INFO", f"  {prop['url']}")
        log("INFO", f"  ... and {len(all_properties) - 10} more")
        return
    
    # Start browser with robust stealth settings
    async with async_playwright() as p:
        # Match main scraper's stealth configuration
        browser = await p.chromium.launch(
            headless=False,
            args=["--start-maximized"],
            ignore_default_args=["--enable-automation", "--no-sandbox"]
        )
        
        # Use random user agent if available, else standard
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=ua
        )
        
        # Critical: Strip webdriver property
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = await context.new_page()
        
        if HAS_STEALTH:
            await stealth_async(page)
        
        session_count = 0
        batch_count = 0
        enriched_data = {}  # file -> list of enriched rows
        
        for i, prop in enumerate(all_properties):
            url = prop["url"]
            file_path = prop["file"]
            
            if not url or pd.isna(url):
                continue
            
            log("INFO", f"[{i+1}/{len(all_properties)}] Enriching: {url[:60]}...")
            
            # Enrich
            enriched = await enrich_single_property(page, url)
            
            if enriched and enriched.get("__blocked__"):
                log("ERR", "⛔ Uso Indebido detectado (Bloqueo IP/UserAgent). Deteniendo enriquecimiento para proteger perfil.")
                break
            
            if enriched:
                # Merge with original row data
                merged = {**prop["row_data"], **enriched}
                
                if file_path not in enriched_data:
                    enriched_data[file_path] = []
                enriched_data[file_path].append(merged)
                
                enriched_urls.add(url)
                log("OK", f"  Enriched with {len(enriched)} new fields")
            
            session_count += 1
            batch_count += 1
            
            # Save state periodically
            if session_count % 10 == 0:
                state["enriched_urls"] = list(enriched_urls)
                save_enrich_state(state)
            
            # Batch break
            if batch_count >= BATCH_SIZE:
                batch_count = 0
                delay = random.uniform(*DELAY_BETWEEN_BATCHES)
                log("INFO", f"Batch complete. Resting {delay/60:.1f} minutes...")
                await asyncio.sleep(delay)
            
            # Session break
            if session_count >= SESSION_LIMIT:
                session_count = 0
                delay = random.uniform(*SESSION_BREAK)
                log("INFO", f"Session limit. Long rest: {delay/60:.1f} minutes...")
                
                # Save enriched data to files
                for fp, rows in enriched_data.items():
                    if rows:
                        log("INFO", f"Saving {len(rows)} enriched rows to {fp.name}")
                        existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                        export_split_by_distrito(existing_df, rows, str(fp), set())
                enriched_data = {}
                
                await asyncio.sleep(delay)
            else:
                # Normal delay between pages
                delay = random.uniform(*DELAY_BETWEEN_PAGES)
                await asyncio.sleep(delay)
        
        # Final save
        for fp, rows in enriched_data.items():
            if rows:
                log("INFO", f"Final save: {len(rows)} enriched rows to {fp.name}")
                existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                export_split_by_distrito(existing_df, rows, str(fp), set())
        
        state["enriched_urls"] = list(enriched_urls)
        save_enrich_state(state)
        
        await browser.close()
    
    log("OK", f"Enrichment complete! Total enriched: {len(enriched_urls)}")


def main():
    parser = argparse.ArgumentParser(description="Background property enrichment worker")
    parser.add_argument("--input", required=True, help="Glob pattern for input Excel files")
    parser.add_argument("--max-price", type=int, default=DEFAULT_MAX_PRICE, 
                        help=f"Max price filter (default: {DEFAULT_MAX_PRICE})")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be processed")
    parser.add_argument("--reset", action="store_true", help="Reset enrichment state")
    
    args = parser.parse_args()
    
    if args.reset:
        if ENRICH_STATE_FILE.exists():
            ENRICH_STATE_FILE.unlink()
            log("OK", "Enrichment state reset.")
        return
    
    files = find_api_files(args.input)
    if not files:
        log("ERR", f"No files found matching: {args.input}")
        return
    
    log("INFO", f"Found {len(files)} files to process")
    
    asyncio.run(run_enrichment(files, args.max_price, args.dry_run))


if __name__ == "__main__":
    main()
