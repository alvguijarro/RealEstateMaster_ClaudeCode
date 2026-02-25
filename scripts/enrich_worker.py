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

# Force UTF-8 encoding for Windows consoles
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except:
        pass

import pandas as pd
from playwright.async_api import async_playwright

# Import scraper components
from scraper.idealista_scraper.extractors import extract_detail_fields
from scraper.idealista_scraper.utils import log, simulate_human_interaction, play_captcha_alert, solve_captcha_advanced, cleanup_stealth_profiles
from scraper.idealista_scraper.excel_writer import export_split_by_distrito
from scraper.idealista_scraper.config import USER_AGENTS, VIEWPORT_SIZES, BROWSER_ROTATION_POOL
from scraper.idealista_scraper.scraper import _goto_with_retry, ScraperSession # Import the robust navigator and session
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
DELAY_BETWEEN_PAGES = (1, 3)  # Faster timing (1-3 seconds) to match main scraper
BATCH_SIZE = 999999 # Practically disabled
SESSION_LIMIT = 999999 # Practically disabled
# Signal flags
STOP_FLAG = PROJECT_ROOT / "scraper" / "ENRICH_STOP.flag"

def check_stop():
    """Check if the user has requested to stop the process."""
    if STOP_FLAG.exists():
        log("WARN", "🛑 Stop signal detected. Exiting gracefully...")
        # Note: server.py will clean this up, but we double ensure
        return True
    return False

# Session break (Practically disabled)
SESSION_BREAK = (1, 2)  

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
    "tipo anunciante", "Baja anuncio", "Comunidad Autonoma", "Zona", "Anuncio activo"
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
        # 1. Skip if URL is in global history
        mask = df["URL"].isin(enriched_urls)
        
        # 2. Skip if row is already marked as enriched in the file
        if "__enriched__" in df.columns:
            # Handle both boolean and string "True"
            mask = mask | (df["__enriched__"].astype(str).str.lower() == "true")
            
        if "Fecha Enriquecimiento" in df.columns:
            mask = mask | df["Fecha Enriquecimiento"].notna()
            
        df = df[~mask]
    
    to_process = len(df)
    log("INFO", f"Procesando {to_process} anuncios (Filtrados por precio <= {max_price}E y no enriquecidos)")
    
    return df


async def enrich_single_property(page, url: str, session: Optional[ScraperSession] = None) -> Optional[dict]:
    """Visit a URL and extract missing fields using robust navigation."""
    try:
        # Use robust navigation from scraper.py
        # It handles: retries, backoff, basic blocks, and automatic slider solving
        # It also handles the 30s manual wait if a session is provided
        await _goto_with_retry(page, url, session=session)
        
        # After navigation, check for common block strings in the text
        # We check both the full text and the title
        body_text = (await page.inner_text("body")).lower()
        title = (await page.title()).lower()
        
        block_strings = [
            "uso indebido", "access denied", "muchas peticiones tuyas", 
            "verificar tu dispositivo", "acceso se ha bloqueado",
            "un uso indebido", "acceso restringido"
        ]
        
        if any(bs in body_text for bs in block_strings) or any(bs in title for bs in block_strings):
            log("ERR", f"⛔ BLOQUEO DETECTADO en {url[:40]}... (Rotando perfil inmediatamente)")
            return {"__blocked__": True}

        # Extra interaction just in case
        await simulate_human_interaction(page)
        
        # Extract fields
        data = await extract_detail_fields(page)
        
        # Check if the extractor itself detected a block
        if data.get("isBlocked") or data.get("__blocked__"):
             log("ERR", f"⛔ BLOQUEO DETECTADO por el extractor en {url[:40]}...")
             return {"__blocked__": True}
             
        # Only return enrich fields
        enriched = {k: v for k, v in data.items() if k in ENRICH_FIELDS and v is not None}
        # Detection of expired/de-listed ads
        is_expired = data.get("Anuncio activo") == "No" or data.get("Baja anuncio") is not None
        
        if is_expired:
            log("INFO", f"  ✅ Anuncio de baja: {data.get('Baja anuncio') or 'No disponible'}")
        elif not enriched or len(enriched) < 3:
             # If too many fields are empty, might be a soft block or rendering issue
             log("WARN", f"Pocos datos extraídos para {url[:60]}. Posible renderizado incompleto.")
             # Mark as incomplete to allow re-trying after rotation if it's a soft-block
             return {"__incomplete__": True, "__enriched__": False}
             
        enriched["__enriched__"] = True
        enriched["Fecha Enriquecimiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
        
    except Exception as e:
        err_msg = str(e).lower()
        block_strings = ["acceso bloqueado", "uso indebido", "peticiones tuyas", "verificar tu dispositivo", "denied"]
        if any(bs in err_msg for bs in block_strings):
            return {"__blocked__": True}
        log("WARN", f"Error enriqueciendo {url[:60]}: {e}")
        return None


async def run_enrichment(files: List[Path], max_price: int, dry_run: bool = False):
    """Main enrichment loop with automatic session rotation."""
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

    # Enrichment loop with rotation logic
    async with async_playwright() as p:
        i = 0
        consecutive_failures = 0
        enriched_data = {}  # file -> list of enriched rows
        total_session_count = 0
        batch_count = 0
        pool_idx = 0

        while i < len(all_properties):
            if check_stop(): break
            
            # Start/Restart browser session using the rotation pool
            browser_conf = BROWSER_ROTATION_POOL[pool_idx % len(BROWSER_ROTATION_POOL)]
            engine_name = browser_conf["engine"]
            channel = browser_conf.get("channel")
            friendly_name = browser_conf.get("name", engine_name)
            
            ua = random.choice(USER_AGENTS)
            v_size = random.choice(VIEWPORT_SIZES)
            
            log("INFO", f"🚀 {friendly_name.upper()}: Iniciando sesión (UA: {ua[:40]}...)")
            
            try:
                browser_type = getattr(p, engine_name)
                launch_args = []
                
                # Chromium-specific optimizations
                if engine_name == "chromium":
                    launch_args = [
                        "--disable-blink-features=AutomationControlled",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-sync",
                        "--metrics-recording-only",
                        "--disable-extensions",
                        "--disable-component-update",
                        "--disable-domain-reliability"
                    ]
                    if channel in ["chrome", "msedge"]:
                        launch_args.append("--start-maximized")
                
                # For Firefox and Webkit, we keep launch_args empty to avoid misinterpretation
                # as URLs or unrecognized flags
                
                browser = await browser_type.launch(
                    headless=False,
                    channel=channel if engine_name == "chromium" else None,
                    args=launch_args,
                    ignore_default_args=["--enable-automation", "--no-sandbox"]
                )
            except Exception as e:
                log("ERR", f"No se pudo iniciar motor {friendly_name}: {e}. Saltando al siguiente.")
                pool_idx += 1
                continue

            context = await browser.new_context(viewport={"width": v_size[0], "height": v_size[1]}, user_agent=ua)
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            page = await context.new_page()
            
            # Create a session object to track captchas (mirroring scraper.py)
            session = ScraperSession(cdp_endpoint="", out_xlsx="", sheet_name="")
            
            error_in_session = False
            try:
                while i < len(all_properties):
                    if check_stop(): break
                    
                    prop = all_properties[i]
                    url = prop["url"]
                    file_path = prop["file"]
                    
                    log("INFO", f"[{i+1}/{len(all_properties)}] Enriching: {url[:60]}...")
                    
                    # Try enrichment
                    try:
                        enriched = await enrich_single_property(page, url, session=session)
                    except Exception as e:
                        if "CAPTCHA_BLOCK_DETECTED" in str(e):
                            log("ERR", "⛔ CAPTCHA RESISTENTE. Rotando pool...")
                        else:
                            log("ERR", f"Error crítico enriqueciendo: {e}")
                        error_in_session = True
                        break

                    # 1. HARD BLOCK DETECTED (via return flag)
                    if enriched and enriched.get("__blocked__"):
                        log("ERR", "⛔ BLOQUEO CONFIRMADO. Rotando identidad...")
                        error_in_session = True
                        break # Break inner loop to rotate
                    
                    # 2. INCOMPLETE DATA (Soft block suspicion)
                    if enriched and enriched.get("__incomplete__"):
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            log("WARN", "⚠️ 3 fallos consecutivos (datos vacíos). Sospecha de soft-block. Rotando...")
                            consecutive_failures = 0
                            error_in_session = True
                            break # Break inner loop to rotate
                    else:
                        consecutive_failures = 0 # Reset if we get a good result or a hard fail

                    if enriched and not enriched.get("__incomplete__"):
                        # Update row
                        merged = {**prop["row_data"], **enriched}
                        if file_path not in enriched_data:
                            enriched_data[file_path] = []
                        enriched_data[file_path].append(merged)
                        
                        enriched_urls.add(url)
                        real_fields = [k for k in enriched.keys() if k not in ["__enriched__", "Fecha Enriquecimiento"]]
                        if real_fields:
                            log("OK", f"  Éxito: {len(real_fields)} campos nuevos ({', '.join(real_fields[:3])}...)")
                        else:
                            log("OK", "  Éxito: Solo metadatos (Anuncio marcado como enriquecido)")
                        
                        i += 1 # Advance to next property only on success or expired
                    elif not enriched:
                        # General error (not block), maybe skip or retry once? 
                        # For now, skip to avoid infinite loops but don't count as success
                        log("WARN", "  No se pudieron obtener datos. Saltando...")
                        i += 1
                    
                    total_session_count += 1
                    batch_count += 1
                    
                    # Periodic state save
                    if total_session_count % 5 == 0:
                        state["enriched_urls"] = list(enriched_urls)
                        save_enrich_state(state)
                    
                    # Periodic Excel save
                    if batch_count >= 15:
                        batch_count = 0
                        log("INFO", "Guardando progreso intermedio...")
                        for fp, rows in enriched_data.items():
                            if rows:
                                try:
                                    existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                                    export_split_by_distrito(existing_df, rows, str(fp), set())
                                except Exception as e:
                                    log("ERR", f"Error guardando {fp.name}: {e}")
                        enriched_data = {}

                    # Wait between pages
                    await asyncio.sleep(random.uniform(*DELAY_BETWEEN_PAGES))
                    
            finally:
                if session.captchas_found > 0:
                    log("INFO", f"📊 Resumen sesión: {session.captchas_found} captchas encontrados, {session.captchas_solved} resueltos.")
                
                await browser.close()
                pool_idx += 1 # Advance browser engine in pool
                
                if error_in_session:
                    log("INFO", "� Rotando identidad inmediatamente...")
                    # Removing the security pause as requested by user

        # Final cleanup and save
        log("INFO", "Finalizando sesión y guardando datos finales...")
        for fp, rows in enriched_data.items():
            if rows:
                try:
                    existing_df = pd.concat(pd.read_excel(fp, sheet_name=None).values(), ignore_index=True)
                    export_split_by_distrito(existing_df, rows, str(fp), set())
                except: pass
        
        state["enriched_urls"] = list(enriched_urls)
        save_enrich_state(state)
    
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
    
    # Cleanup profiles to free up space
    try:
        cleanup_stealth_profiles()
    except:
        pass


if __name__ == "__main__":
    main()
