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

# Advanced Features from Scraper Wrapper
from scraper.app.scraper_wrapper import (
    rotate_identity, mark_current_profile_blocked, get_profile_dir, 
    get_browser_executable_path, DEEP_STEALTH_SCRIPT, continuous_mouse_jitter,
    BlockedException
)

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

# =============================================================================
# ADVANCED HELPERS (Ported from ScraperController)
# =============================================================================

async def check_for_blocks(page, log_func=None) -> Optional[str]:
    """Granular block detection (WAF IDs, DataDome, Cloudflare, short pages)."""
    try:
        page_data = await page.evaluate("""
            () => ({
                title: document.title,
                text: document.documentElement ? document.documentElement.innerText : (document.body ? document.body.innerText : '')
            })
        """)
        title = (page_data.get("title") or "").lower()
        text_lower = (page_data.get("text") or "").lower()
        text_lower = re.sub(r'\s+', ' ', text_lower).strip()

        # 1. HARD BLOCKS
        has_block_id = bool(re.search(r"id:\s*[0-9a-f]{8,32}-", text_lower))
        hard_block_keywords = [
            "el acceso se ha bloqueado", "se ha detectado un uso indebido",
            "un uso indebido", "uso no autorizado", "acceso bloqueado",
            "forbidden", "access denied"
        ]
        if any(kw in text_lower for kw in hard_block_keywords) or has_block_id:
            if log_func: log_func("ERR", f"🛑 HARD BLOCK detected (Title: {title[:30]})")
            return "block"

        # 2. CAPTCHAS / DATADOME
        is_datadome = await page.evaluate("""() => {
            return !!(document.querySelector('iframe[src*="captcha-delivery.com"]') || 
                      window.dd || 
                      document.querySelector('script[src*="captcha-delivery.com"]'));
        }""")
        captcha_keywords = [
            "muchas peticiones tuyas", "confirma que eres humano",
            "verificación necesaria", "un momento, por favor",
            "cloudflare", "checking your browser"
        ]
        if is_datadome or any(kw in text_lower for kw in captcha_keywords) or any(kw in title for kw in captcha_keywords):
            return "captcha"

        # 3. Short 'idealista.com' page with NO elements
        if title == "idealista.com" and len(text_lower) < 1200:
            has_items = await page.evaluate("""() => {
                return !!document.querySelector('article, .item, [data-element-id], #h1-container');
            }""")
            if not has_items:
                return "block"
        
        return None
    except:
        return None

async def simulate_reading_time(text: str, log_func=None):
    """Wait proportional to description length (Extra Stealth behavior)."""
    if not text: return
    # Base: 0.5s per 100 chars, max 5s for enrichment
    wait_time = min(5.0, len(text) / 200.0)
    if wait_time > 1.0:
        if log_func: log_func("INFO", f"⌛ Simulando tiempo de lectura ({wait_time:.1f}s)...")
        await asyncio.sleep(wait_time)


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
    """Visit a URL and extract missing fields with advanced evasion and detection."""
    # 1. Advanced Evasion Setup
    await page.add_init_script(DEEP_STEALTH_SCRIPT)
    stop_jitter = asyncio.Event()
    jitter_task = asyncio.create_task(continuous_mouse_jitter(page, stop_jitter))
    
    try:
        # 2. Robust Navigation
        try:
            # Use 120s guard as in main scraper
            await asyncio.wait_for(
                _goto_with_retry(page, url, session=session),
                timeout=120.0
            )
        except asyncio.TimeoutError:
            log("ERR", f"⏰ NAVIGATION HANG en {url[:40]}...")
            return None
        except Exception as e:
            if "ns_error_abort" in str(e).lower():
                pass # Usually a minor redirect issue handled by retry
            else:
                log("WARN", f"Navegación fallida: {e}")
                return None

        # 3. Granular Block Detection
        block_type = await check_for_blocks(page, log_func=log)
        
        if block_type == "block":
            log("ERR", f"🚫 BLOQUEO (Uso indebido) detectado en {url[:40]}...")
            mark_current_profile_blocked()
            return {"__blocked__": True}
            
        if block_type == "captcha":
            log("WARN", f"⚠️ CAPTCHA detectado. Intentando resolución automática...")
            try:
                captcha_solved = await asyncio.wait_for(
                    solve_captcha_advanced(page, logger=log, use_proxy=True),
                    timeout=180.0
                )
            except asyncio.TimeoutError:
                captcha_solved = False
                log("WARN", "⏰ Timeout en resolución automática de captcha")

            if captcha_solved and not await check_for_blocks(page):
                log("OK", "✅ CAPTCHA resuelto automáticamente!")
            elif session:
                log("INFO", ">>> ESPERANDO RESOLUCIÓN MANUAL (30s max) <<<")
                play_captcha_alert()
                # Simple poll for 30s
                for _ in range(15):
                    await asyncio.sleep(2)
                    if not await check_for_blocks(page):
                        log("OK", "✅ CAPTCHA resuelto manualmente!")
                        break
                else:
                    log("ERR", "❌ CAPTCHA no resuelto. Rotando...")
                    return {"__blocked__": True}
            else:
                return {"__blocked__": True}

        # 4. Humanization (Reading time simulation)
        await simulate_human_interaction(page)
        
        # 5. Extraction
        data = await extract_detail_fields(page)
        
        if data.get("isBlocked") or data.get("__blocked__"):
             log("ERR", f"⛔ BLOQUEO DETECTADO por el extractor...")
             mark_current_profile_blocked()
             return {"__blocked__": True}

        # 6. Reading Time Simulation based on description
        desc = data.get("Descripcion", "")
        await simulate_reading_time(desc, log_func=log)
             
        # Filter fields
        enriched = {k: v for k, v in data.items() if k in ENRICH_FIELDS and v is not None}
        is_expired = data.get("Anuncio activo") == "No" or data.get("Baja anuncio") is not None
        
        if is_expired:
            log("INFO", f"  ✅ Anuncio de baja: {data.get('Baja anuncio') or 'No disponible'}")
        elif not enriched or len(enriched) < 3:
             log("WARN", f"Pocos datos extraídos para {url[:40]}. Posible renderizado incompleto.")
             return {"__incomplete__": True, "__enriched__": False}
             
        enriched["__enriched__"] = True
        enriched["Fecha Enriquecimiento"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return enriched
        
    finally:
        # Cleanup humanization tasks
        stop_jitter.set()
        try:
            await asyncio.wait_for(jitter_task, timeout=2.0)
        except:
            pass


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

    i = 0
    enriched_data = {}  # file -> list of enriched rows
    total_session_count = 0
    batch_count = 0
    consecutive_failures = 0
    
    while i < len(all_properties):
        if check_stop(): break
            
        # 1. Advanced Identity Rotation (with Cooldown)
        # Motores incompatibles con proxy autenticado en Windows
        _PROXY_INCOMPATIBLE = {"webkit", "firefox"}
        profile_config, wait_time = rotate_identity()
        while profile_config and profile_config.get("engine") in _PROXY_INCOMPATIBLE:
            log("WARN", f"⚠️ Saltando perfil '{profile_config['name']}' (motor incompatible con proxy)")
            mark_current_profile_blocked()
            profile_config, wait_time = rotate_identity()
        if wait_time > 0:
            log("WARN", f"⏳ Todos los perfiles en cooldown. Esperando {wait_time:.0f}s...")
            await asyncio.sleep(min(30, wait_time)) # Wait and retry
            continue

        profile_dir = get_profile_dir(profile_config["index"])
        executable = get_browser_executable_path(profile_config.get("channel"))

        log("INFO", f"🔄 Nueva sesión: Perfil {profile_config['index']} ({profile_config['name']})")

        error_in_session = False
        async with async_playwright() as p:
            # Launch persistent context to reuse profiles and cookies
            try:
                from scraper.app.scraper_wrapper import _build_browser_proxy
                _ew_proxy = _build_browser_proxy()

                # Use engine from config (chromium, firefox, webkit)
                browser_type = getattr(p, profile_config["engine"])

                browser = await browser_type.launch_persistent_context(
                    user_data_dir=profile_dir,
                    headless=profile_config.get("headless", True),
                    executable_path=executable,
                    user_agent=random.choice(USER_AGENTS),
                    viewport={"width": random.choice(VIEWPORT_SIZES)[0], "height": random.choice(VIEWPORT_SIZES)[1]},
                    args=[
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-web-security"
                    ] if profile_config["engine"] == "chromium" else [],
                    proxy=_ew_proxy,
                    ignore_https_errors=True
                )
                
                # Setup session for captcha monitoring
                page = browser.pages[0] if browser.pages else await browser.new_page()
                session = ScraperSession(cdp_endpoint="", out_xlsx="", sheet_name="")
                
                # 2. Main Processing Loop inside Session
                while i < len(all_properties) and not error_in_session:
                    if check_stop(): break
                    
                    prop = all_properties[i]
                    url = prop["url"]
                    file_path = prop["file"]
                    
                    log("INFO", f"[{i+1}/{len(all_properties)}] {url[:50]}...")
                    
                    try:
                        enriched = await enrich_single_property(page, url, session=session)
                    except Exception as e:
                        log("ERR", f"Error crítico enriqueciendo: {e}")
                        error_in_session = True
                        break

                    # --- SUCCESS/FAILURE EVALUATION ---
                    
                    # 1. HARD BLOCK (Identity Rotation)
                    if enriched and enriched.get("__blocked__"):
                        log("ERR", "⛔ BLOQUEO CONFIRMADO. Rotando identidad...")
                        error_in_session = True
                        break 
                    
                    # 2. INCOMPLETE DATA (Soft block suspicion)
                    if enriched and enriched.get("__incomplete__"):
                        consecutive_failures += 1
                        if consecutive_failures >= 3:
                            log("WARN", "⚠️ 3 fallos consecutivos (datos vacíos). Sospecha de soft-block. Rotando...")
                            consecutive_failures = 0
                            error_in_session = True
                            break 
                    else:
                        consecutive_failures = 0 

                    if enriched and not enriched.get("__incomplete__"):
                        # Update row
                        merged = {**prop["row_data"], **enriched}
                        if file_path not in enriched_data:
                            enriched_data[file_path] = []
                        enriched_data[file_path].append(merged)
                        
                        enriched_urls.add(url)
                        real_fields = [k for k in enriched.keys() if k not in ["__enriched__", "Fecha Enriquecimiento"]]
                        if real_fields:
                            log("OK", f"  Éxito: {len(real_fields)} campos nuevos")
                        else:
                            log("OK", "  Éxito: Solo metadatos (Anuncio marcado como enriquecido)")
                        
                        i += 1 
                    elif not enriched:
                        # General error (not block), skip
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
                                except: pass
                        enriched_data = {}

                    # Wait between pages
                    await asyncio.sleep(random.uniform(*DELAY_BETWEEN_PAGES))
                    
            except Exception as e:
                log("ERR", f"Error en sesión: {e}")
                error_in_session = True
            finally:
                if session.captchas_found > 0:
                    log("INFO", f"📊 Resumen sesión: {session.captchas_found} captchas encontrados, {session.captchas_solved} resueltos.")
                
                try: await browser.close()
                except: pass

        if check_stop(): break

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
