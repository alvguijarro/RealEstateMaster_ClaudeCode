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
from scraper.idealista_scraper.utils import log, simulate_human_interaction, play_captcha_alert, solve_captcha_advanced
from scraper.idealista_scraper.excel_writer import export_split_by_distrito
from scraper.idealista_scraper.config import USER_AGENTS, VIEWPORT_SIZES
from scraper.idealista_scraper.scraper import _goto_with_retry # Import the robust navigator
from shared.config import API_MAX_PRICE

# Try to import stealth from scraper utils or use local
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

# Rate limiting (Sync with scraper logic)
DELAY_BETWEEN_PAGES = (5, 12)  # seconds, randomized
DELAY_BETWEEN_BATCHES = (60, 180)  # minutes between batches
BATCH_SIZE = 15
SESSION_LIMIT = 80  
# Signal flags
STOP_FLAG = PROJECT_ROOT / "scraper" / "ENRICH_STOP.flag"

def check_stop():
    """Check if the user has requested to stop the process."""
    if STOP_FLAG.exists():
        log("WARN", "🛑 Stop signal detected. Exiting gracefully...")
        # Note: server.py will clean this up, but we double ensure
        return True
    return False

# Session break
SESSION_BREAK = (300, 900)  # 5-15 minutes

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
    try:
        sheets = pd.read_excel(file_path, sheet_name=None)
        if not sheets:
            return pd.DataFrame()
        
        df = pd.concat(sheets.values(), ignore_index=True)
    except Exception as e:
        log("ERR", f"Error reading {file_path.name}: {e}")
        return pd.DataFrame()
    
    # Filter by price
    if "price" in df.columns:
        df["price"] = pd.to_numeric(df["price"], errors="coerce")
        df = df[df["price"] <= max_price]
    
    # Filter out already enriched
    if "URL" in df.columns:
        df = df[~df["URL"].isin(enriched_urls)]
    
    to_process = len(df)
    log("INFO", f"Procesando {to_process} anuncios (Filtrados por precio <= {max_price}E y no enriquecidos)")
    
    return df


async def enrich_single_property(page, url: str) -> Optional[dict]:
    """Visit a URL and extract missing fields using robust navigation."""
    try:
        # Use robust navigation from scraper.py
        # It handles: retries, backoff, basic blocks, and automatic slider solving
        await _goto_with_retry(page, url)
        
        # After navigation, check for specific blocks that _goto might have missed 
        # but are critical for enrichment
        title = (await page.title()).lower()
        if "uso indebido" in title or "access denied" in title:
            log("ERR", "⛔ BLOQUEO DETECTADO post-navegación.")
            return {"__blocked__": True}

        # Extra interaction just in case
        await simulate_human_interaction(page)
        
        # Extract fields
        data = await extract_detail_fields(page)
        
        # Only return enrich fields
        enriched = {k: v for k, v in data.items() if k in ENRICH_FIELDS and v is not None}
        if not enriched or len(enriched) < 3:
             # If too many fields are empty, might be a soft block or rendering issue
             log("WARN", f"Pocos datos extraídos para {url}. Posible renderizado incompleto.")
             
        enriched["__enriched__"] = True
        enriched["Fecha Enriquecimiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
        
    except Exception as e:
        err_msg = str(e)
        if "Acceso bloqueado" in err_msg or "uso indebido" in err_msg.lower():
            return {"__blocked__": True}
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
                url = row.get("URL")
                if url and isinstance(url, str):
                    all_properties.append({
                        "file": file_path,
                        "url": url,
                        "row_data": row.to_dict()
                    })
    
    if not all_properties:
        log("OK", "No hay inmuebles nuevos para enriquecer.")
        return
    
    log("INFO", f"Total a procesar: {len(all_properties)} inmuebles")
    
    # Shuffle to hide patterns
    random.shuffle(all_properties)
    
    if dry_run:
        log("INFO", f"DRY RUN - Procesaría {len(all_properties)} inmuebles.")
        return
    
    # Start browser with EXACT same config as scraper.py
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=False,
            args=[
                "--start-maximized",
                "--disable-blink-features=AutomationControlled"
            ],
            ignore_default_args=["--enable-automation", "--no-sandbox"]
        )
        
        # Pick random identity
        ua = random.choice(USER_AGENTS)
        v_size = random.choice(VIEWPORT_SIZES)
        
        context = await browser.new_context(
            viewport={"width": v_size[0], "height": v_size[1]},
            user_agent=ua
        )
        
        # Stealth init
        await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        page = await context.new_page()
        
        session_count = 0
        batch_count = 0
        enriched_data = {}  # file -> list of enriched rows
        
        try:
            for i, prop in enumerate(all_properties):
                if check_stop(): break
                    
                url = prop["url"]
                file_path = prop["file"]
                
                log("INFO", f"[{i+1}/{len(all_properties)}] Enriching: {url[:60]}...")
                
                # Try enrichment with robust logic
                enriched = await enrich_single_property(page, url)
                
                if enriched and enriched.get("__blocked__"):
                    log("ERR", "⛔ Bloqueo detectado. Abortando lote para evitar baneo permanente.")
                    break
                
                if enriched:
                    # Update row
                    merged = {**prop["row_data"], **enriched}
                    if file_path not in enriched_data:
                        enriched_data[file_path] = []
                    enriched_data[file_path].append(merged)
                    
                    enriched_urls.add(url)
                    log("OK", f"  Éxito: {len(enriched)} campos nuevos.")
                else:
                    log("WARN", "  No se pudieron obtener datos.")
                
                session_count += 1
                batch_count += 1
                
                # Periodic save
                if session_count % 5 == 0:
                    state["enriched_urls"] = list(enriched_urls)
                    save_enrich_state(state)
                
                # Save to disk periodically (Enrichment updates are crucial)
                if batch_count >= BATCH_SIZE:
                    batch_count = 0
                    log("INFO", "Guardando progreso intermedio...")
                    for fp, rows in enriched_data.items():
                        if rows:
                            try:
                                existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                                export_split_by_distrito(existing_df, rows, str(fp), set())
                            except Exception as e:
                                log("ERR", f"Error guardando {fp.name}: {e}")
                    enriched_data = {} # Reset list after saving
                    
                    delay = random.uniform(*DELAY_BETWEEN_BATCHES)
                    log("INFO", f"Batch completado. Descansando {delay/60:.1f} min...")
                    for _ in range(int(delay)):
                        if check_stop(): break
                        await asyncio.sleep(1)
                    if check_stop(): break
                
                # Extra long rest
                if session_count >= SESSION_LIMIT:
                    session_count = 0
                    delay = random.uniform(*SESSION_BREAK)
                    log("INFO", f"Límite de sesión alcanzado. Descansa {delay/60:.1f} min...")
                    for _ in range(int(delay)):
                        if check_stop(): break
                        await asyncio.sleep(1)
                    if check_stop(): break
                else:
                    # Generic page delay
                    await asyncio.sleep(random.uniform(*DELAY_BETWEEN_PAGES))

        finally:
            # Final Save always
            for fp, rows in enriched_data.items():
                if rows:
                    log("INFO", f"Guardado final: {fp.name}")
                    try:
                        existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                        export_split_by_distrito(existing_df, rows, str(fp), set())
                    except: pass
            
            state["enriched_urls"] = list(enriched_urls)
            save_enrich_state(state)
            await browser.close()
    
    log("OK", f"Enriquecimiento finalizado. Total enriquecidos: {len(enriched_urls)}")


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
