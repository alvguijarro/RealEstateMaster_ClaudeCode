import os
import sys
import time
import re
import datetime
import sqlite3
import asyncio
import json
import argparse
import csv
from pathlib import Path

# Worker ID para ejecución paralela (cada worker usa un proxy distinto)
_worker_id   = int(os.environ.get('SCRAPER_WORKER_ID',   '1'))
_num_workers = int(os.environ.get('SCRAPER_NUM_WORKERS', '1'))

# Setup paths
BASE_DIR = Path(__file__).parent
PROJECT_ROOT = BASE_DIR.parent
DATA_DIR = BASE_DIR / "data"
DB_PATH = DATA_DIR / "market_trends.db"
_sfx = f"_w{_worker_id}" if _num_workers > 1 else ""
CHECKPOINT_FILE = DATA_DIR / f"checkpoint{_sfx}.json"
STOP_FLAG_FILE = DATA_DIR / "TRACKER_STOP.flag"
LOG_FILE = DATA_DIR / "execution_log.jsonl"
RESUME_POINT_FILE = DATA_DIR / f"resume_point{_sfx}.json"
DEBUG_DIR = DATA_DIR / "debug"
MAPPING_FILE = PROJECT_ROOT / "scraper" / "documentation" / "province_urls_mapping.md"
SUBZONES_FILE = PROJECT_ROOT / "scraper" / "documentation" / "subzones_complete.json"

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
SCRAPER_DIR = PROJECT_ROOT / "scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))

from playwright.async_api import async_playwright
import random

# Import stealth and captcha utilities from main scraper
from browser_utils import get_browser_executable_path, generate_stealth_script
from idealista_scraper.config import VIEWPORT_SIZES, USER_AGENTS, BROWSER_ROTATION_POOL
from idealista_scraper.utils import solve_captcha_advanced, simulate_human_interaction, detect_captcha_or_block
from identity_manager import rotate_identity, mark_current_profile_blocked, get_profile_dir
from shared.proxy_config import PROXY_CONFIG

def init_db():
    """Inicializa/migra la base de datos SQLite añadiendo la columna subzone si no existe."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    cursor = conn.cursor()

    # Crear tabla si no existe (instalaciones nuevas)
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS inventory_trends (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_record TEXT NOT NULL,
            iso_year INTEGER NOT NULL,
            iso_week INTEGER NOT NULL,
            province TEXT NOT NULL,
            zone TEXT NOT NULL,
            subzone TEXT NOT NULL DEFAULT '',
            operation TEXT NOT NULL,
            total_properties INTEGER NOT NULL,
            UNIQUE(date_record, province, zone, subzone, operation)
        )
    ''')

    # Migración: añadir columna subzone si falta (instalaciones existentes)
    cursor.execute("PRAGMA table_info(inventory_trends)")
    cols = [row[1] for row in cursor.fetchall()]
    if 'subzone' not in cols:
        print("INFO: Migrando DB — añadiendo columna subzone y actualizando constraint...")
        cursor.execute("ALTER TABLE inventory_trends RENAME TO inventory_trends_old")
        cursor.execute('''
            CREATE TABLE inventory_trends (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_record TEXT NOT NULL,
                iso_year INTEGER NOT NULL,
                iso_week INTEGER NOT NULL,
                province TEXT NOT NULL,
                zone TEXT NOT NULL,
                subzone TEXT NOT NULL DEFAULT '',
                operation TEXT NOT NULL,
                total_properties INTEGER NOT NULL,
                UNIQUE(date_record, province, zone, subzone, operation)
            )
        ''')
        cursor.execute('''
            INSERT INTO inventory_trends
                SELECT id, date_record, iso_year, iso_week, province, zone, '', operation, total_properties
                FROM inventory_trends_old
        ''')
        cursor.execute("DROP TABLE inventory_trends_old")
        print("INFO: Migración completada.")

    conn.commit()
    conn.close()


def parse_mapping_v2(mapping_file, subzones_file):
    """Parsea el mapping de URLs omitiendo zonas hoja (sin sub-zonas).
    Las zonas hoja se capturan vía sidenotes al visitar la página de provincia.
    Devuelve (urls_list, subzones_data) donde urls_list contiene tuplas de 5 elementos:
    (province, zone, url, operation, level) con level='province' o 'zone'.
    """
    subzones_data = {}
    if subzones_file.exists():
        try:
            with open(subzones_file, 'r', encoding='utf-8') as f:
                subzones_data = json.load(f)
        except Exception as e:
            print(f"WARN: No se pudo cargar subzones_complete.json: {e}")

    def zone_subzone_status(province, zone):
        """Devuelve True si zona tiene sub-zonas conocidas, False si es hoja confirmada,
        None si la provincia/zona no está en el JSON (desconocida → tratar como visita directa).
        """
        prov_data = subzones_data.get(province)
        if prov_data is None:
            return None  # Provincia no mapeada aún — comportamiento conservador: visitar
        zone_data = prov_data.get(zone)
        if zone_data is None:
            return None  # Zona no mapeada aún — comportamiento conservador: visitar
        return bool(zone_data.get('subzones'))

    urls_to_scrape = []
    current_operation = None
    current_province = None

    with open(mapping_file, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            if line.startswith("## 🏠 Alquiler"):
                current_operation = "alquiler"
            elif line.startswith("## 💰 Venta"):
                current_operation = "venta"

            elif line.startswith("|") and not line.startswith("| :---"):
                parts = [p.strip() for p in line.split("|")]
                if len(parts) >= 4:
                    prov = parts[1].replace("**", "").strip()
                    zone = parts[2].strip()
                    url_md = parts[3].strip()

                    if prov and prov.lower() != "provincia":
                        current_province = prov

                    url_match = re.search(r'`(https?://[^`]+)`', url_md)
                    if url_match:
                        url = url_match.group(1)
                        if current_province and current_operation:
                            is_province_level = '(Toda la provincia)' in zone or zone.lower() == 'toda la provincia'
                            if is_province_level:
                                urls_to_scrape.append((current_province, zone, url, current_operation, 'province'))
                            else:
                                status = zone_subzone_status(current_province, zone)
                                if status is True:
                                    # Zona con sub-zonas conocidas → visitar para obtener sub-zonas vía sidenotes
                                    urls_to_scrape.append((current_province, zone, url, current_operation, 'zone'))
                                elif status is False:
                                    pass  # Zona hoja confirmada → capturada vía sidenote de provincia, no se visita
                                else:
                                    # Desconocida (provincia/zona no en JSON) → visitar directamente (comportamiento seguro)
                                    urls_to_scrape.append((current_province, zone, url, current_operation, 'zone'))

    return urls_to_scrape, subzones_data


async def extract_breadcrumb_sidenotes(page):
    """Extrae conteos de zonas/sub-zonas desde los sidenotes del breadcrumb en el DOM."""
    try:
        return await page.evaluate('''() => {
            return Array.from(document.querySelectorAll(
                'li.breadcrumb-dropdown-subitem-element-list'))
              .map(li => {
                const link = li.querySelector('a');
                const sidenote = li.querySelector('.breadcrumb-navigation-sidenote');
                const raw = sidenote ? sidenote.innerText.trim() : '';
                const m = raw.match(/([0-9][0-9.,]*)/);
                return {
                  name: link ? link.innerText.trim() : null,
                  href: link ? link.getAttribute('href') : null,
                  count: m ? parseInt(m[1].replace(/[.,]/g, '')) : 0
                };
              }).filter(i => i.name);
        }''')
    except Exception as e:
        print(f"  ⚠️ Error extrayendo sidenotes del breadcrumb: {e}")
        return []

async def extract_h1_number(page):
    """Extracts the leading number from the H1 element with improved robustness."""
    try:
        # Try multiple common selectors for the title count
        selectors = ["h1", ".main-info h1", "#h1-container h1", ".h1-container h1"]
        h1_text = ""
        
        for selector in selectors:
            try:
                # 15s timeout as requested by user
                h1_handle = await page.wait_for_selector(selector, timeout=15000)
                if h1_handle:
                    h1_text = await h1_handle.inner_text()
                    if h1_text:
                        break
            except:
                continue
        
        if not h1_text:
            return 0
            
        # Match numbers with potential dots/commas as thousands separators
        match = re.search(r'([0-9.,]+)', h1_text)
        if match:
            clean_num = match.group(1).replace(".", "").replace(",", "")
            if clean_num.isdigit():
                return int(clean_num)
    except Exception as e:
        pass
    return 0

async def detect_block(page):
    """Detects if we are hard-blocked or on a captcha page.
    Returns 'block', 'captcha', or None. Delegado a detect_captcha_or_block (función canónica en utils.py).
    """
    # Pantalla de verificación de dispositivo — no es un bloqueo
    if await is_verification_screen(page):
        return None
    return await detect_captcha_or_block(page)

async def take_debug_screenshot(page, province, zone, suffix=""):
    """Captures a screenshot when 0 properties are found or block detected."""
    try:
        DEBUG_DIR.mkdir(parents=True, exist_ok=True)
        safe_zone = re.sub(r'[^a-zA-Z0-9]', '_', zone)
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        filename = f"{province}_{safe_zone}_{timestamp}{suffix}.png"
        filepath = DEBUG_DIR / filename
        await page.screenshot(path=str(filepath))
        print(f"  📸 Debug screenshot saved: trends/data/debug/{filename}")
    except Exception as e:
        print(f"  ⚠️ Could not take debug screenshot: {e}")

async def is_legit_zero_results(page):
    """Checks if the page explicitly mentions that no results were found."""
    try:
        content = await page.evaluate("""() => {
            const bodyText = document.body ? document.body.innerText.toLowerCase() : '';
            return bodyText;
        }""")
        # Common Spanish strings for "no results" on Idealista
        no_results_keywords = [
            "no hay anuncios",
            "no hemos encontrado lo que buscas",
            "hemos mirado por todas partes",
            "0 anuncios",
            "no hay resultados"
        ]
        return any(kw in content for kw in no_results_keywords)
    except:
        return False

async def is_verification_screen(page):
    """Checks if the page is showing Idealista's 'Device Verification' screen."""
    try:
        content = await page.evaluate("""() => {
            const bodyText = document.body ? document.body.innerText.toLowerCase() : '';
            return bodyText;
        }""")
        return "verificación del dispositivo" in content or "verificando su dispositivo" in content
    except:
        return False

async def wait_for_verification(page, max_attempts=3):
    """Adaptive wait for Idealista's verification screen to disappear."""
    for i in range(1, max_attempts + 1):
        if await is_verification_screen(page):
            print(f"  ⏳ Idealista device verification detected. Waiting {i*10}s (Attempt {i}/{max_attempts})...")
            await asyncio.sleep(10)
        else:
            if i > 1:
                print("  ✅ Verification completed.")
            return True
    return False
    
async def save_to_db(date_record, iso_year, iso_week, province, zone, operation, total, subzone=''):
    """Saves the extracted total to the SQLite database."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR REPLACE INTO inventory_trends
            (date_record, iso_year, iso_week, province, zone, subzone, operation, total_properties)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (date_record, iso_year, iso_week, province, zone, subzone, operation, total))
        conn.commit()
    except Exception as e:
        print(f"DB Error: {e}")
    finally:
        conn.close()

async def record_exists_for_day(date_record, province, zone, operation, subzone=''):
    """Checks if a record already exists for today to avoid double scraping."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            SELECT total_properties FROM inventory_trends
            WHERE date_record = ? AND province = ? AND zone = ? AND subzone = ? AND operation = ?
        ''', (date_record, province, zone, subzone, operation))
        row = cursor.fetchone()
        if row:
            # If total is 0, we count it as non-existent to force a re-scrape (likely error)
            return row[0] > 0
        return False
    except Exception as e:
        print(f"DB Check Error: {e}")
        return False
    finally:
        conn.close()

def save_checkpoint(index, date_record):
    """Saves the current progress index to resume later."""
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "last_index": index,
                "date_record": date_record
            }, f)
    except Exception as e:
        print(f"Failed to save checkpoint: {e}")

def load_checkpoint():
    """Loads the checkpoint. Returns (last_index, date_record) or (0, None)."""
    if CHECKPOINT_FILE.exists():
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Support legacy weekly checkpoints (treat as expired)
                if "iso_week" in data and "date_record" not in data:
                    return 0, None
                return data.get("last_index", 0), data.get("date_record")
        except:
            pass
    return 0, None

def log_event(event: str, **kwargs):
    """Registra un evento en el log JSONL de ejecuciones (append-only)."""
    entry = {"ts": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"), "event": event}
    entry.update(kwargs)
    try:
        with open(LOG_FILE, 'a', encoding='utf-8') as f:
            f.write(json.dumps(entry, ensure_ascii=False) + '\n')
    except Exception as e:
        print(f"WARN: No se pudo escribir en execution_log: {e}")


def save_resume_point(index, date_record, reason="interrupted", urls_total=0):
    """Guarda el punto de reanudación cross-session."""
    try:
        with open(RESUME_POINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "last_index": index,
                "date_record": date_record,
                "saved_at": datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "reason": reason,
                "urls_total": urls_total
            }, f)
    except Exception as e:
        print(f"WARN: No se pudo guardar resume_point: {e}")


def load_resume_point():
    """Carga el punto de reanudación si existe."""
    if RESUME_POINT_FILE.exists():
        try:
            with open(RESUME_POINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None


def clear_resume_point():
    """Elimina el fichero de resume_point tras completar con éxito."""
    try:
        if RESUME_POINT_FILE.exists():
            RESUME_POINT_FILE.unlink()
            print("🗑️ Resume point eliminado (ejecución completada).")
    except Exception as e:
        print(f"WARN: No se pudo eliminar resume_point: {e}")


def send_failure_email(start_index, urls_len, urls_data, date_record):
    """Envía un email de notificación si el tracker se interrumpió inesperadamente."""
    from shared.config import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASSWORD, SMTP_FROM, SMTP_TO, SMTP_ENABLED
    if not SMTP_ENABLED:
        return
    import smtplib
    from email.mime.text import MIMEText

    pending = urls_data[start_index:]
    pending_count = len(pending)
    pending_lines = [
        f"  [{start_index+i+1}/{urls_len}] {p} / {z} ({op})"
        for i, (p, z, _, op, _) in enumerate(pending[:50])
    ]
    if pending_count > 50:
        pending_lines.append(f"  ... y {pending_count - 50} más.")

    subject = f"[RealEstateMaster] Trends Tracker interrumpido — {pending_count} URLs pendientes ({date_record})"
    body = (
        f"El Trends Tracker se ha interrumpido inesperadamente el {date_record}.\n\n"
        f"Progreso: {start_index}/{urls_len} URLs procesadas.\n"
        f"URLs pendientes: {pending_count}\n\n"
        f"Provincias/zonas pendientes:\n" + "\n".join(pending_lines) + "\n\n"
        f"El tracker reanudará automáticamente desde el índice {start_index} en la próxima ejecución.\n"
    )
    try:
        recipients = [r.strip() for r in SMTP_TO.split(",") if r.strip()]
        msg = MIMEText(body, 'plain', 'utf-8')
        msg['Subject'] = subject
        msg['From'] = SMTP_FROM
        msg['To'] = ", ".join(recipients)
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(SMTP_FROM, recipients, msg.as_string())
        print(f"✉️ Email de fallo enviado a {SMTP_TO}")
    except Exception as e:
        print(f"WARN: No se pudo enviar email de notificación: {e}")


def auto_export_csv():
    """Generates a CSV export of the entire SQLite DB to disk."""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # VENTA
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, subzone, operation, total_properties FROM inventory_trends WHERE operation = 'venta' ORDER BY id DESC")
        rows_venta = cursor.fetchall()

        # ALQUILER
        cursor.execute("SELECT date_record, iso_year, iso_week, province, zone, subzone, operation, total_properties FROM inventory_trends WHERE operation = 'alquiler' ORDER BY id DESC")
        rows_alquiler = cursor.fetchall()

        conn.close()

        ts = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        file_venta = DATA_DIR / f"market_trends_venta_{ts}.csv"
        file_alquiler = DATA_DIR / f"market_trends_alquiler_{ts}.csv"

        headers = ['Fecha', 'Año ISO', 'Semana ISO', 'Provincia', 'Zona', 'Subzona', 'Operación', 'Total Propiedades']

        if rows_venta:
            with open(file_venta, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows_venta)
            print(f"✅ Auto-exported {len(rows_venta)} records to {file_venta.name}")

        if rows_alquiler:
            with open(file_alquiler, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(headers)
                writer.writerows(rows_alquiler)
            print(f"✅ Auto-exported {len(rows_alquiler)} records to {file_alquiler.name}")

    except Exception as e:
        print(f"Error auto-exporting CSV: {e}")

async def run_tracker(resume=False, headless=False, force_date=None):
    print(f"Starting Robust Market Trends Tracker (Resume: {resume}, Headless: {headless})...", flush=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    # Ensure DB schema is current (runs migration if subzone column missing)
    init_db()

    urls_data, subzones_data = parse_mapping_v2(MAPPING_FILE, SUBZONES_FILE)
    if not urls_data:
        print("Warning: No URLs found in mapping file.")
        return

    if _num_workers > 1:
        urls_data = [u for i, u in enumerate(urls_data) if i % _num_workers == _worker_id - 1]
        print(f"🔀 Worker {_worker_id}/{_num_workers}: {len(urls_data)} URLs asignadas")

    leaf_zones_count = sum(1 for _, _, _, _, lvl in urls_data if lvl == 'province')
    zone_visits_count = sum(1 for _, _, _, _, lvl in urls_data if lvl == 'zone')
    print(f"Found {len(urls_data)} URLs to track ({leaf_zones_count} province-level, {zone_visits_count} zone-level). "
          f"Leaf zones captured via sidenotes (no direct visit).")

    # Get current Date Data (allow override for re-running past days)
    if force_date:
        now = datetime.datetime.strptime(force_date, "%d-%m-%Y")
        print(f"INFO: Fecha forzada a {force_date} (override manual)")
    else:
        now = datetime.datetime.now()
    date_formatted = now.strftime("%d-%m-%Y")
    iso_year, iso_week, _ = now.isocalendar()

    start_index = 0
    uncertain_zero_urls = []
    if resume:
        # Prioridad 1: checkpoint mismo día (comportamiento existente)
        last_idx, cp_date = load_checkpoint()
        if cp_date == date_formatted:
            start_index = last_idx
            print(f"Resuming from checkpoint: index {start_index} for day {date_formatted}")
        else:
            print("Checkpoint is from a previous day. Starting fresh.")
    elif not force_date:
        # Prioridad 2: resume_point cross-session (auto-detección sin --resume)
        rp = load_resume_point()
        if rp:
            start_index = rp["last_index"]
            print(f"🔄 Auto-resume: índice {start_index} (sesión anterior del {rp['date_record']}, motivo: {rp['reason']})")

    urls_len = len(urls_data)
    stopped_by_user = False
    log_event("session_start", idx=start_index, date=date_formatted, total_urls=urls_len)

    # Grabar checkpoint inicial con la fecha de hoy — así aunque el proceso muera
    # antes de la URL #20, el siguiente --resume encontrará la fecha correcta.
    save_checkpoint(start_index, date_formatted)

    # Remove old stop flag if exists
    if STOP_FLAG_FILE.exists():
        try: STOP_FLAG_FILE.unlink()
        except: pass
    
    while start_index < urls_len:
        if STOP_FLAG_FILE.exists():
            print("🔴 Stop flag detected. Halting outer loop.")
            stopped_by_user = True
            break
        
        # IDENTITY ROTATION
        # WebKit on Windows no soporta proxies autenticados — excluir del pool
        PROXY_INCOMPATIBLE_ENGINES = {"webkit", "firefox"}
        profile_config, wait_time = rotate_identity()
        while profile_config and profile_config.get("engine") in PROXY_INCOMPATIBLE_ENGINES:
            print(f"INFO: Saltando perfil {profile_config['name']} (motor {profile_config.get('engine')} incompatible con proxy autenticado en Windows)")
            mark_current_profile_blocked()
            profile_config, wait_time = rotate_identity()
        if wait_time > 0:
            print(f"WARN: All profiles in cooldown. Waiting {int(wait_time/60)}m...")
            await asyncio.sleep(wait_time)
            continue
            
        profile_dir = get_profile_dir(profile_config["index"])
        os.makedirs(profile_dir, exist_ok=True)
        
        async with async_playwright() as pw:
            print(f"INFO: Launching persistent browser with profile: {profile_config['name']}...")
            
            browser_args = [
                "--start-maximized",
                "--disable-dev-shm-usage",
                "--disable-infobars",
                "--no-first-run",
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--lang=es-ES,es",
                "--no-default-browser-check",
            ]
            
            exe_path = get_browser_executable_path(profile_config.get("channel"))
            viewport = random.choice(VIEWPORT_SIZES)
            
            engine_name = profile_config.get("engine", "chromium")
            browser_launcher = getattr(pw, engine_name)
            
            # Firefox y Opera siempre headless (sin ventana visible para el usuario)
            _ch = profile_config.get("channel")
            _is_headless = headless or engine_name == "firefox" or _ch == "opera"

            launch_options = {
                "user_data_dir": profile_dir,
                "headless": _is_headless,
                "viewport": {"width": viewport[0], "height": viewport[1]},
                "user_agent": random.choice(USER_AGENTS),
                "ignore_https_errors": True,
                "proxy": {
                    "server": f"http://{PROXY_CONFIG['host']}:{PROXY_CONFIG['port']}",
                    "username": f"{PROXY_CONFIG['login']}-session-{PROXY_CONFIG['sticky_session_id']}",
                    "password": PROXY_CONFIG['password'],
                },
            }
            
            if engine_name == "chromium":
                launch_options["args"] = browser_args
                launch_options["ignore_default_args"] = ["--enable-automation"]
                channel = profile_config.get("channel")
                # Only pass channel for Playwright-recognized values (chrome, msedge)
                # Non-standard channels (opera, brave, vivaldi) need executable_path instead
                if channel and channel not in ("opera", "brave", "vivaldi", "iron"):
                    launch_options["channel"] = channel
                if exe_path:
                    launch_options["executable_path"] = exe_path
            elif engine_name == "firefox":
                if exe_path:
                    launch_options["executable_path"] = exe_path
                launch_options["firefox_user_prefs"] = {
                    "dom.webdriver.enabled": False,
                    "useAutomationExtension": False,
                }
                
            try:
                context = await browser_launcher.launch_persistent_context(**launch_options)
                
                # ADVANCED GPU/DEEP STEALTH (14-phase full stealth, GPU randomized per call)
                await context.add_init_script(generate_stealth_script())
                
                page = context.pages[0] if context.pages else await context.new_page()
                
                # Close extra tabs potentially restored by portable browsers (like Opera)
                for p in context.pages:
                    if p != page:
                        try: await p.close()
                        except: pass
                
                # PROCESS URLs
                scan_idx = start_index
                consecutive_skips = 0
                made_progress = False
                
                while scan_idx < urls_len:
                    if STOP_FLAG_FILE.exists():
                        print("🔴 Stop flag detected. Halting inner loop.")
                        stopped_by_user = True
                        break

                    province, zone, url, operation, level = urls_data[scan_idx]
                    level_label = "🏙️ Provincia" if level == 'province' else "🗺️ Zona"
                    print(f"[{scan_idx+1}/{urls_len}] {level_label} {province} ({zone}) - {operation.upper()}...")

                    # Deduplication Check (daily) — solo para el registro principal (subzone='')
                    if await record_exists_for_day(date_formatted, province, zone, operation, subzone=''):
                        print(f"  -> Skipping. Data already exists for {date_formatted}.")
                        log_event("url_skip", idx=scan_idx, province=province, zone=zone, operation=operation)
                        scan_idx += 1
                        consecutive_skips += 1

                        # Auto-stop: if we've skipped all remaining URLs without any scrape
                        remaining = urls_len - start_index
                        if consecutive_skips >= remaining:
                            print(f"✅ Auto-stop: All {consecutive_skips} remaining URLs already have data for {date_formatted}. Finishing.")
                            scan_idx = urls_len  # Force exit
                            break
                        continue

                    # Reset skip counter when we actually attempt a scrape
                    consecutive_skips = 0

                    # Retry logic for this specific URL (up to 3 attempts)
                    success = False
                    for attempt in range(1, 4):
                        try:
                            if attempt > 1:
                                print(f"  -> Retry attempt {attempt}/3...")

                            await page.goto(url, timeout=45000, wait_until="domcontentloaded")
                            await asyncio.sleep(random.uniform(2.5, 4.5))
                            try:
                                await asyncio.wait_for(simulate_human_interaction(page), timeout=8.0)
                            except Exception:
                                pass

                            # Adaptive wait for "Device Verification" screen
                            if not await wait_for_verification(page):
                                print(f"  ⏳ Verification still active. Waiting 10s extra...")
                                await asyncio.sleep(10)

                            # Block/captcha detection (verification screens are excluded)
                            block_status = await detect_block(page)
                            if block_status == "block":
                                print(f"WARN: BLOCK detected on attempt {attempt} (El acceso se ha bloqueado).")
                                mark_current_profile_blocked()
                                raise RuntimeError("CAPTCHA_CRITICAL_BLOCK")
                            elif block_status == "captcha":
                                print(f"WARN: CAPTCHA detected on attempt {attempt}. Intentando resolver...")
                                solved = await solve_captcha_advanced(page)
                                if solved:
                                    print("INFO: Captcha resuelto. Extrayendo datos de la página actual (sin re-navegar)...")
                                    # solve_captcha_advanced ya dejó la página en la URL objetivo:
                                    # caer al extract_h1_number en vez de re-navegar con continue
                                else:
                                    print("WARN: Captcha no resuelto. Rotando identidad...")
                                    mark_current_profile_blocked()
                                    raise RuntimeError("CAPTCHA_CRITICAL_BLOCK")

                            total_properties = await extract_h1_number(page)

                            if total_properties == 0:
                                # Distinguish between real 0 and load error/block
                                if await is_legit_zero_results(page):
                                    print(f"  -> Legitimate 0 properties found (Confirmed by 'No hay anuncios').")
                                else:
                                    print(f"WARN: 0 properties found on attempt {attempt} (No confirmation message).")
                                    await take_debug_screenshot(page, province, zone, suffix=f"_0props_att{attempt}")
                                    if attempt < 3:
                                        continue  # Try again
                                    else:
                                        print(f"  -> Tagging {province} ({zone}) for Double-Check Phase.")
                                        uncertain_zero_urls.append((province, zone, url, operation))

                            print(f"  -> Found {total_properties} properties.")
                            if total_properties >= 0:
                                # Guardar registro principal (H1 de la URL visitada)
                                await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, total_properties, subzone='')

                                # Extraer sidenotes del breadcrumb y guardar registros secundarios
                                sidenotes = await extract_breadcrumb_sidenotes(page)
                                if sidenotes:
                                    if level == 'province':
                                        # Guardar solo zonas hoja (sin sub-zonas en subzones_data)
                                        prov_zones = subzones_data.get(province, {})
                                        saved_sidenotes = 0
                                        for item in sidenotes:
                                            zone_name = item['name']
                                            zone_entry = prov_zones.get(zone_name, {})
                                            is_leaf = not zone_entry.get('subzones')
                                            if is_leaf:
                                                await save_to_db(date_formatted, iso_year, iso_week, province, zone_name, operation, item['count'], subzone='')
                                                saved_sidenotes += 1
                                        if saved_sidenotes:
                                            print(f"  -> Sidenotes: {saved_sidenotes} zonas hoja guardadas desde breadcrumb.")
                                    elif level == 'zone':
                                        # Guardar sub-zonas de esta zona
                                        for item in sidenotes:
                                            await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, item['count'], subzone=item['name'])
                                        print(f"  -> Sidenotes: {len(sidenotes)} sub-zonas guardadas para {zone}.")

                                success = True
                                made_progress = True
                                log_event("url_ok", idx=scan_idx, province=province, zone=zone, operation=operation, total=total_properties)
                                break  # Exit retry loop

                        except Exception as e:
                            if "CAPTCHA_CRITICAL_BLOCK" in str(e):
                                raise e  # Propagate to outer loop for rotation
                            print(f"  ⚠️ Error attempt {attempt}: {e}")
                            if attempt < 3: await asyncio.sleep(5)

                    if not success:
                        log_event("url_fail", idx=scan_idx, province=province, zone=zone, operation=operation, total=None, reason="max_retries_exceeded")

                    # Move to next URL even if it failed all retries (to avoid infinite loops)
                    scan_idx += 1
                    start_index = scan_idx  # Keep outer loop in sync (critical for CAPTCHA rotation)

                    # Save Checkpoint every 20 urls
                    if scan_idx > 0 and scan_idx % 20 == 0:
                        print(f"💾 Saving Checkpoint at index {scan_idx}...")
                        save_checkpoint(scan_idx, date_formatted)
                        save_resume_point(scan_idx, date_formatted, urls_total=urls_len)

                if STOP_FLAG_FILE.exists() or scan_idx >= urls_len:
                    # --- DOUBLE-CHECK PHASE ---
                    if scan_idx >= urls_len and uncertain_zero_urls and not STOP_FLAG_FILE.exists():
                        print(f"\n🔍 STARTING DOUBLE-CHECK PHASE for {len(uncertain_zero_urls)} uncertain items...")
                        for prov, zn, u, op in uncertain_zero_urls:
                            if STOP_FLAG_FILE.exists(): break
                            print(f"  -> Re-checking {prov} ({zn})...")
                            try:
                                await page.goto(u, timeout=45000, wait_until="domcontentloaded")
                                await asyncio.sleep(random.uniform(5.0, 8.0))  # More conservative wait
                                await wait_for_verification(page)

                                recheck_val = await extract_h1_number(page)
                                if recheck_val > 0:
                                    print(f"    ✨ Corrected! Found {recheck_val} properties.")
                                    await save_to_db(date_formatted, iso_year, iso_week, prov, zn, op, recheck_val, subzone='')
                                elif await is_legit_zero_results(page):
                                    print(f"    ✅ Confirmed 0 properties (No hay anuncios).")
                                    await save_to_db(date_formatted, iso_year, iso_week, prov, zn, op, 0, subzone='')
                                else:
                                    print(f"    ❌ Still 0 properties (Unconfirmed).")
                            except Exception as re_e:
                                print(f"    ⚠️ Error in re-check: {re_e}")
                        print("✅ Double-Check Phase completed.\n")
                    break
            except Exception as e:
                err_str = str(e)
                if "CAPTCHA_CRITICAL_BLOCK" in err_str:
                    print(f"INFO: Identity rotation triggered (CAPTCHA/block detected).")
                else:
                    print(f"CRITICAL: Browser instance failed: {e}")
                    # Mark the failed profile as blocked so we don't re-select it immediately
                    # (e.g. Opera crash loop: launch_persistent_context fails, profile stays unblocked,
                    # rotate_identity picks it again → infinite crash)
                    try:
                        mark_current_profile_blocked()
                    except Exception:
                        pass
                # Preserve progress so outer loop doesn't restart from the beginning
                if 'scan_idx' in locals():
                    start_index = scan_idx
                    save_checkpoint(scan_idx, date_formatted)
                    save_resume_point(scan_idx, date_formatted, reason="captcha_block", urls_total=urls_len)

            finally:
                if 'context' in locals():
                    await context.close()
                # Profiles are kept between runs to accumulate cookie history and session credibility.
                # Only blocked profiles should be purged (handled by mark_current_profile_blocked).

    # Final checkpoint update
    save_checkpoint(start_index, date_formatted)
    log_event("session_end", idx=start_index, completed=(start_index >= urls_len), stopped_by_user=stopped_by_user)

    if start_index >= urls_len:
        clear_resume_point()
        print("Market Trends Tracking Completed Full List!")
    else:
        print(f"Tracker Stopped at index {start_index}.")
        if not stopped_by_user:
            send_failure_email(start_index, urls_len, urls_data, date_formatted)

    # Auto export to CSV locally at the end of run
    print("Initiating automatic database backup to CSV...", flush=True)
    auto_export_csv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--date", type=str, default=None, help="Forzar fecha del registro (formato DD-MM-YYYY)")
    args = parser.parse_args()

    asyncio.run(run_tracker(resume=args.resume, headless=args.headless, force_date=args.date))
