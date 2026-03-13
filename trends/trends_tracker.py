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

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
SCRAPER_DIR = PROJECT_ROOT / "scraper"
if str(SCRAPER_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_DIR))
SCRAPER_APP_DIR = SCRAPER_DIR / "app"
if str(SCRAPER_APP_DIR) not in sys.path:
    sys.path.insert(0, str(SCRAPER_APP_DIR))

from playwright.async_api import async_playwright
import random

# Import stealth and captcha utilities from main scraper
from browser_utils import get_browser_executable_path, generate_stealth_script
from idealista_scraper.config import VIEWPORT_SIZES, USER_AGENTS, BROWSER_ROTATION_POOL, RETRY_MAX_ATTEMPTS, RETRY_BASE_DELAY
from idealista_scraper.utils import solve_captcha_advanced, simulate_human_interaction, detect_captcha_or_block, play_captcha_alert, reset_tbv_counter
from identity_manager import rotate_identity, mark_current_profile_blocked, get_profile_dir
from shared.proxy_config import PROXY_CONFIG, PROXY_LABEL, get_proxy_pool, build_playwright_proxy, _generate_session_id
from shared_url_queue import SharedURLQueue

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

def save_checkpoint_v2(completed_keys, total, date_record):
    """Guarda checkpoint V2 con claves completadas en vez de índice secuencial."""
    try:
        with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
            json.dump({
                "version": 2,
                "date_record": date_record,
                "completed_keys": [list(k) for k in completed_keys],
                "completed_count": len(completed_keys),
                "total": total
            }, f)
    except Exception as e:
        print(f"Failed to save checkpoint V2: {e}")

def load_checkpoint_v2(date_record):
    """Carga checkpoint V2. Devuelve set de (province, zone, operation) completadas.
    Solo carga formato V2 nativo. Para V1 devuelve set vacío (el caller maneja la migración)."""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get("version") != 2:
            # V1 detectado — no intentar migrar aquí, dejar al caller
            return set()
        if data.get("date_record") != date_record:
            print(f"INFO: Checkpoint de {data.get('date_record')}, hoy es {date_record}. Empezando de cero.")
            return set()
        keys = set()
        for k in data.get("completed_keys", []):
            if len(k) >= 3:
                keys.add(tuple(k))
        print(f"INFO: Checkpoint V2 cargado: {len(keys)} URLs completadas de {data.get('total', '?')}.")
        return keys
    except Exception as e:
        print(f"WARN: Error cargando checkpoint V2: {e}")
        return set()

def _migrate_v1_checkpoint(date_record, urls_data):
    """Migra checkpoint V1 (last_index secuencial) a claves V2 usando el orden de urls_data.
    El tracker antiguo procesaba URLs en orden secuencial, así que los primeros last_index
    items de urls_data corresponden a las URLs ya completadas."""
    if not CHECKPOINT_FILE.exists():
        return set()
    try:
        with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if data.get("version") == 2:
            return set()  # Ya es V2, no migrar
        v1_date = data.get("date_record")
        last_index = data.get("last_index", 0)
        if v1_date != date_record:
            print(f"INFO: Checkpoint V1 de fecha {v1_date}, hoy es {date_record}. No se puede migrar.")
            return set()
        if last_index <= 0:
            print(f"INFO: Checkpoint V1 con last_index=0 — el tracker anterior no completó ninguna URL.")
            return set()
        # Reconstruir claves completadas de los primeros last_index items (orden secuencial)
        completed = set()
        for i, item in enumerate(urls_data):
            if i >= last_index:
                break
            completed.add((item[0], item[1], item[3]))  # (province, zone, operation)
        print(f"INFO: Migrado checkpoint V1 (last_index={last_index}) → {len(completed)} URLs completadas.")
        # Guardar como V2 inmediatamente para futuras reanudaciones
        save_checkpoint_v2(completed, len(urls_data), date_record)
        return completed
    except Exception as e:
        print(f"WARN: Error migrando checkpoint V1: {e}")
        return set()

def _recover_completed_from_db(date_record):
    """Recupera claves completadas consultando la BD (fallback cuando no hay checkpoint V2)."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT DISTINCT province, zone, operation FROM inventory_trends
            WHERE date_record = ? AND subzone = '' AND total_properties > 0
        ''', (date_record,))
        keys = set()
        for row in cursor.fetchall():
            keys.add(tuple(row))
        conn.close()
        if keys:
            print(f"INFO: Recuperadas {len(keys)} URLs completadas desde la BD para {date_record}.")
        else:
            print(f"INFO: No se encontraron URLs completadas en la BD para {date_record}.")
        return keys
    except Exception as e:
        print(f"WARN: No se pudo recuperar progreso desde la BD: {e}")
        return set()

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

_BROWSER_SHORT_NAMES = {
    "Chromium (Default)": "Chromium",
    "Google Chrome": "Chrome",
    "Microsoft Edge": "Edge",
}

def _short_browser_name(name):
    """Nombre corto del navegador para logs legibles."""
    return _BROWSER_SHORT_NAMES.get(name, name)


async def _launch_trends_context(pw, worker_id, proxy_cfg, headless=False):
    """Lanza un contexto Playwright para un worker del tracker.

    Worker 1: visible (salvo headless=True), usa rotate_identity() para seleccionar engine/channel.
    Workers 2+: Chromium headless, profile slot 90+worker_id.
    Todos: proxy propio, stealth script.
    Returns: (context, page, profile_config_or_None)
    """
    PROXY_INCOMPATIBLE_ENGINES = {"webkit", "firefox"}
    viewport = random.choice(VIEWPORT_SIZES)
    proxy_pw = build_playwright_proxy(proxy_cfg)

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

    if worker_id == 1:
        # Worker principal: usa rotate_identity para seleccionar perfil
        while True:
            profile_config, wait_time = rotate_identity()
            while profile_config and profile_config.get("engine") in PROXY_INCOMPATIBLE_ENGINES:
                print(f"[Worker {worker_id}] INFO: Saltando perfil {profile_config['name']} "
                      f"(motor {profile_config.get('engine')} incompatible con proxy)")
                mark_current_profile_blocked()
                profile_config, wait_time = rotate_identity()
            if wait_time <= 0:
                break
            print(f"[Worker {worker_id}] WARN: Todos los perfiles en cooldown. Esperando {int(wait_time/60)}m...")
            await asyncio.sleep(wait_time)

        profile_dir = get_profile_dir(profile_config["index"])
        os.makedirs(profile_dir, exist_ok=True)

        exe_path = get_browser_executable_path(profile_config.get("channel"))
        engine_name = profile_config.get("engine", "chromium")
        browser_launcher = getattr(pw, engine_name)

        _ch = profile_config.get("channel")
        _is_headless = headless or engine_name == "firefox" or _ch == "opera"

        launch_options = {
            "user_data_dir": profile_dir,
            "headless": _is_headless,
            "viewport": {"width": viewport[0], "height": viewport[1]},
            "user_agent": random.choice(USER_AGENTS),
            "ignore_https_errors": True,
            "proxy": proxy_pw,
        }

        if engine_name == "chromium":
            launch_options["args"] = browser_args
            launch_options["ignore_default_args"] = ["--enable-automation"]
            channel = profile_config.get("channel")
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

        context = await browser_launcher.launch_persistent_context(**launch_options)
        print(f"[{_short_browser_name(profile_config['name'])}/Proxy #{worker_id}] "
              f"Navegador lanzado ({'headless' if _is_headless else 'visible'})")
    else:
        # Workers 2+: Chrome/Edge headless con proxy, perfil dedicado
        profile_slot = 90 + worker_id
        profile_dir = get_profile_dir(profile_slot)
        os.makedirs(profile_dir, exist_ok=True)

        # Canal alternante: W2→chrome, W3→msedge, W4→chrome, W5→msedge
        _channel = None
        if sys.platform == "win32":
            _channel = "chrome" if worker_id % 2 == 0 else "msedge"

        # UA coherente con el canal asignado
        if _channel == "msedge":
            _ua_pool = [u for u in USER_AGENTS if 'Edg' in u]
        elif _channel == "chrome":
            _ua_pool = [u for u in USER_AGENTS if 'OPR' not in u and 'Edg' not in u]
        else:
            _ua_pool = [u for u in USER_AGENTS if 'OPR' not in u and 'Edg' not in u]
        ua = random.choice(_ua_pool) if _ua_pool else random.choice(USER_AGENTS)

        launch_options = {
            "user_data_dir": profile_dir,
            "headless": True,
            "viewport": {"width": viewport[0], "height": viewport[1]},
            "user_agent": ua,
            "ignore_https_errors": True,
            "proxy": proxy_pw,
            "args": browser_args,
            "ignore_default_args": ["--enable-automation"],
        }
        if _channel:
            launch_options["channel"] = _channel

        _channel_name = {"chrome": "Chrome", "msedge": "Edge"}.get(_channel, "Chromium")
        try:
            context = await pw.chromium.launch_persistent_context(**launch_options)
        except Exception as _launch_err:
            # Fallback a Chromium si Chrome/Edge no está instalado (Linux/Docker)
            print(f"[Worker {worker_id}] WARN: No se pudo lanzar {_channel_name}: {_launch_err}. Fallback a Chromium.")
            launch_options.pop("channel", None)
            _channel_name = "Chromium"
            ua = random.choice([u for u in USER_AGENTS if 'OPR' not in u and 'Edg' not in u])
            launch_options["user_agent"] = ua
            context = await pw.chromium.launch_persistent_context(**launch_options)

        print(f"[{_channel_name}/Proxy #{worker_id}] Navegador lanzado (headless, slot {profile_slot})")
        profile_config = {"name": _channel_name, "channel": _channel, "engine": "chromium", "index": profile_slot}

    # Stealth script para todos los workers
    await context.add_init_script(generate_stealth_script())

    page = context.pages[0] if context.pages else await context.new_page()

    # Cerrar tabs extra que podrían restaurarse
    for p in context.pages:
        if p != page:
            try: await p.close()
            except: pass

    return context, page, profile_config


async def _trends_worker(
    worker_id,
    pw,
    queue,
    proxy_cfg,
    date_formatted,
    iso_year,
    iso_week,
    subzones_data,
    uncertain_zero_urls,
    shared_state,
    checkpoint_lock,
    completed_keys,
    headless,
):
    """Worker paralelo del tracker. Reclama URLs de la cola compartida y las procesa."""
    MAX_RECOVERY = 3
    recovery_count = 0
    consecutive_captcha_fails = 0
    proxy_label = f"Proxy #{worker_id}"
    tag = f"[Chromium/{proxy_label}]"  # Default, actualizado tras lanzar navegador

    while recovery_count <= MAX_RECOVERY and not shared_state["stopped"]:
        context = None
        current_item = None

        try:
            context, page, _profile_cfg = await _launch_trends_context(
                pw, worker_id, proxy_cfg, headless=headless
            )

            # Construir tag con nombre real del navegador
            browser_name = _short_browser_name(_profile_cfg['name']) if _profile_cfg else 'Chromium'
            tag = f"[{browser_name}/{proxy_label}]"

            while not shared_state["stopped"]:
                item = await queue.claim()
                if item is None:
                    return  # Cola agotada

                current_item = item
                province, zone, url, operation, level = item
                item_key = (province, zone, operation)

                # Contadores globales para logging
                async with checkpoint_lock:
                    idx_display = shared_state["completed"] + 1
                total_display = shared_state["total"]

                print(f"[{idx_display}/{total_display}] {tag} Extrayendo {province} ({zone}) - {operation.upper()}...")

                # Dedup diaria
                if await record_exists_for_day(date_formatted, province, zone, operation, subzone=''):
                    print(f"  -> Ya existe dato para {date_formatted}. Saltando.")
                    log_event("url_skip", worker=worker_id, province=province, zone=zone, operation=operation)
                    async with checkpoint_lock:
                        shared_state["completed"] += 1
                        completed_keys.add(item_key)
                    current_item = None
                    continue

                # Retry logic con backoff exponencial (modelado en _goto_with_retry del scraper)
                success = False
                captcha_was_reason = False
                delay = RETRY_BASE_DELAY
                for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
                    try:
                        if attempt > 1:
                            print(f"  -> Reintento {attempt}/{RETRY_MAX_ATTEMPTS}...")

                        # Navegación con guard anti-hang (Playwright 60s + asyncio 120s)
                        try:
                            await asyncio.wait_for(
                                page.goto(url, timeout=60000, wait_until="domcontentloaded"),
                                timeout=120.0
                            )
                        except asyncio.TimeoutError:
                            print(f"  {tag} ⚠️ Navegación colgada (>120s) en intento {attempt}")
                            raise RuntimeError("NAVIGATION_HANG")

                        await asyncio.sleep(random.uniform(2.5, 4.5))
                        try:
                            await asyncio.wait_for(simulate_human_interaction(page), timeout=8.0)
                        except Exception:
                            pass

                        # Espera adaptativa para pantalla de verificación
                        if not await wait_for_verification(page):
                            print(f"  {tag} ⏳ Verificación activa. Esperando 10s extra...")
                            await asyncio.sleep(10)

                        # Detección de soft block (redirect a homepage)
                        current_url = page.url
                        current_title = (await page.title()).lower()
                        if current_url != url and current_title == "idealista.com":
                            print(f"{tag} WARN: SOFT BLOCK — redirigido a homepage en intento {attempt}.")
                            raise RuntimeError("BLOCK_DETECTED")

                        # Detección de bloqueo/captcha
                        block_status = await detect_block(page)
                        if block_status == "block":
                            print(f"{tag} WARN: BLOQUEO DURO detectado en intento {attempt}.")
                            if worker_id == 1 and _profile_cfg:
                                mark_current_profile_blocked()
                            raise RuntimeError("BLOCK_DETECTED")
                        elif block_status == "captcha":
                            captcha_was_reason = True
                            print(f"{tag} WARN: CAPTCHA detectado en intento {attempt}. Intentando resolver...")
                            try:
                                solved = await asyncio.wait_for(
                                    solve_captcha_advanced(page, use_proxy=True, proxy_config=proxy_cfg),
                                    timeout=180.0
                                )
                            except asyncio.TimeoutError:
                                print(f"{tag} WARN: ⏰ Timeout en solve_captcha_advanced (180s).")
                                solved = False

                            if solved:
                                print(f"{tag} INFO: Captcha resuelto automáticamente.")
                                captcha_was_reason = False
                            else:
                                # Worker 1 visible: intentar resolución manual
                                if worker_id == 1 and not headless:
                                    print(f"{tag} INFO: Captcha no resuelto. Esperando resolución manual (60s)...")
                                    play_captcha_alert()
                                    manual_solved = False
                                    for _poll in range(30):  # 30 x 2s = 60s
                                        await asyncio.sleep(2)
                                        poll_status = await detect_block(page)
                                        poll_title = (await page.title()).lower()
                                        if poll_status is None and "idealista" in poll_title and poll_title != "idealista.com":
                                            manual_solved = True
                                            break
                                    if manual_solved:
                                        print(f"{tag} INFO: Captcha resuelto manualmente.")
                                        captcha_was_reason = False
                                    else:
                                        print(f"{tag} WARN: Resolución manual agotada.")
                                        raise RuntimeError("CAPTCHA_UNSOLVED")
                                else:
                                    # Headless o Worker >1: directamente a retry
                                    print(f"{tag} WARN: Captcha no resuelto en intento {attempt}.")
                                    raise RuntimeError("CAPTCHA_UNSOLVED")

                        total_properties = await extract_h1_number(page)

                        if total_properties == 0:
                            if await is_legit_zero_results(page):
                                print(f"  -> 0 propiedades (confirmado: no hay anuncios).")
                            else:
                                print(f"{tag} WARN: 0 propiedades en intento {attempt}.")
                                await take_debug_screenshot(page, province, zone, suffix=f"_0props_att{attempt}")
                                if attempt < RETRY_MAX_ATTEMPTS:
                                    continue
                                else:
                                    print(f"  -> Marcado para doble verificación.")
                                    uncertain_zero_urls.append((province, zone, url, operation))

                        print(f"  -> Encontradas {total_properties} propiedades.")
                        if total_properties >= 0:
                            # Guardar registro principal
                            await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, total_properties, subzone='')

                            # Extraer sidenotes del breadcrumb
                            sidenotes = await extract_breadcrumb_sidenotes(page)
                            if sidenotes:
                                if level == 'province':
                                    prov_zones = subzones_data.get(province, {})
                                    saved_sidenotes = 0
                                    for sn_item in sidenotes:
                                        zone_name = sn_item['name']
                                        zone_entry = prov_zones.get(zone_name, {})
                                        is_leaf = not zone_entry.get('subzones')
                                        if is_leaf:
                                            await save_to_db(date_formatted, iso_year, iso_week, province, zone_name, operation, sn_item['count'], subzone='')
                                            saved_sidenotes += 1
                                    if saved_sidenotes:
                                        print(f"  -> Sidenotes: {saved_sidenotes} zonas hoja guardadas.")
                                elif level == 'zone':
                                    for sn_item in sidenotes:
                                        await save_to_db(date_formatted, iso_year, iso_week, province, zone, operation, sn_item['count'], subzone=sn_item['name'])
                                    print(f"  -> Sidenotes: {len(sidenotes)} sub-zonas guardadas para {zone}.")

                            success = True
                            captcha_was_reason = False
                            consecutive_captcha_fails = 0
                            reset_tbv_counter()  # Scrape exitoso: limpiar circuit breaker
                            log_event("url_ok", worker=worker_id, province=province, zone=zone, operation=operation, total=total_properties)
                            break  # Exit retry loop

                    except Exception as e:
                        err_msg = str(e)
                        # Bloqueo duro: escalar inmediatamente (no reintentar)
                        if "BLOCK_DETECTED" in err_msg:
                            raise RuntimeError("CAPTCHA_CRITICAL_BLOCK")
                        # Captcha no resuelto o navegación colgada: backoff + reintentar
                        if attempt < RETRY_MAX_ATTEMPTS:
                            print(f"  {tag} ⚠️ Error en intento {attempt}: {e} — Backoff {delay:.1f}s antes de reintentar...")
                            await asyncio.sleep(delay)
                            delay *= 2
                        else:
                            print(f"  {tag} ⚠️ Error en intento {attempt}: {e}")

                # Tras agotar todos los intentos: escalar solo si captcha fue la razón
                if not success and captcha_was_reason:
                    print(f"{tag} WARN: Captcha no resuelto tras {RETRY_MAX_ATTEMPTS} intentos. Escalando a rotación...")
                    raise RuntimeError("CAPTCHA_CRITICAL_BLOCK")

                if not success:
                    log_event("url_fail", worker=worker_id, province=province, zone=zone, operation=operation, reason="max_retries")

                # Marcar como completado (incluso si falló todos los reintentos, para no repetir)
                async with checkpoint_lock:
                    shared_state["completed"] += 1
                    completed_keys.add(item_key)
                    if shared_state["completed"] % 20 == 0:
                        print(f"💾 {tag} Checkpoint guardado ({shared_state['completed']}/{shared_state['total']})")
                        save_checkpoint_v2(completed_keys, shared_state["total"], date_formatted)
                        save_resume_point(shared_state["completed"], date_formatted, urls_total=shared_state["total"])
                current_item = None

        except Exception as e:
            err_str = str(e)
            if "CAPTCHA_CRITICAL_BLOCK" in err_str:
                consecutive_captcha_fails += 1
                print(f"{tag} Bloqueo/captcha. Consecutivos: {consecutive_captcha_fails}")

                # Devolver URL a la cola si estaba en proceso
                if current_item is not None:
                    await queue.release(current_item)
                    current_item = None

                if consecutive_captcha_fails >= 3:
                    pause_min = 20
                    print(f"{tag} 🌩️ Storm: {consecutive_captcha_fails} fallos consecutivos. "
                          f"Pausa {pause_min}min...")
                    log_event("worker_storm_pause", worker=worker_id,
                              consecutive_fails=consecutive_captcha_fails, pause_minutes=pause_min)
                    consecutive_captcha_fails = 0
                    await asyncio.sleep(pause_min * 60)

                recovery_count += 1
                if recovery_count > MAX_RECOVERY:
                    print(f"{tag} Max recuperaciones ({MAX_RECOVERY}) alcanzado. Worker finalizado.")
                    break

                # Regenerar sticky session para nuevo IP
                proxy_cfg['sticky_session_id'] = _generate_session_id()
                reset_tbv_counter()  # Nueva IP: reiniciar circuit breaker
                print(f"{tag} 🔑 Nueva sesión proxy. Cooldown 45s...")
                await asyncio.sleep(45)

            else:
                consecutive_captcha_fails = 0
                print(f"{tag} CRITICAL: Error inesperado: {e}")
                # Devolver URL a la cola si estaba en proceso
                if current_item is not None:
                    await queue.release(current_item)
                    current_item = None
                if worker_id == 1:
                    try: mark_current_profile_blocked()
                    except: pass
                recovery_count += 1
                if recovery_count > MAX_RECOVERY:
                    print(f"{tag} Max recuperaciones ({MAX_RECOVERY}) alcanzado.")
                    break
                await asyncio.sleep(10)

        finally:
            if context:
                try: await context.close()
                except: pass

    print(f"{tag} Worker finalizado. Recuperaciones: {recovery_count}/{MAX_RECOVERY}")


async def run_tracker(resume=True, headless=False, force_date=None):
    print(f"Starting Parallel Market Trends Tracker (Resume: {resume}, Headless: {headless})...", flush=True)
    os.makedirs(DATA_DIR, exist_ok=True)

    # Ensure DB schema is current (runs migration if subzone column missing)
    init_db()

    urls_data, subzones_data = parse_mapping_v2(MAPPING_FILE, SUBZONES_FILE)
    if not urls_data:
        print("Warning: No URLs found in mapping file.")
        return

    # Multi-process compat (mantener para ejecución distribuida entre máquinas)
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

    # Checkpoint: cargar claves completadas con cadena de fallback V2 → V1 → BD
    completed_keys = set()
    if resume:
        # 1. Intentar checkpoint V2 nativo
        completed_keys = load_checkpoint_v2(date_formatted)
        # 2. Si no hay V2, intentar migrar desde V1 (last_index secuencial)
        if not completed_keys:
            completed_keys = _migrate_v1_checkpoint(date_formatted, urls_data)
        # 3. Si tampoco hay V1 migrable, recuperar desde la BD
        if not completed_keys:
            completed_keys = _recover_completed_from_db(date_formatted)
        if not completed_keys:
            print("INFO: No se encontró progreso previo — comenzando desde el principio.")
    elif not force_date:
        # Auto-resume: detectar checkpoint o resume_point del mismo día
        rp = load_resume_point()
        if rp:
            completed_keys = load_checkpoint_v2(date_formatted)
            if not completed_keys:
                completed_keys = _migrate_v1_checkpoint(date_formatted, urls_data)
            if not completed_keys:
                completed_keys = _recover_completed_from_db(date_formatted)
            if completed_keys:
                print(f"🔄 Auto-resume: {len(completed_keys)} URLs ya completadas")

    # Poblar cola con URLs no completadas
    queue = SharedURLQueue()
    queued = 0
    for item in urls_data:
        key = (item[0], item[1], item[3])  # (province, zone, operation)
        if key not in completed_keys:
            await queue.put(item)
            queued += 1
    queue.close()

    urls_len = len(urls_data)

    if queued == 0:
        print(f"✅ El scraping del día {date_formatted} ya ha sido ejecutado.")
        clear_resume_point()
        auto_export_csv()
        return

    print(f"📋 Cola: {queued} URLs pendientes de {urls_len} totales.")

    # Remove old stop flag if exists
    if STOP_FLAG_FILE.exists():
        try: STOP_FLAG_FILE.unlink()
        except: pass

    # Determinar workers según proxies disponibles
    proxy_pool = get_proxy_pool()
    if _num_workers > 1:
        # Multi-proceso activo: 1 worker in-process por proceso (evitar N*M explosión de proxies)
        n_workers = 1
    else:
        n_workers = min(len(proxy_pool), 5)
    print(f"🚀 Lanzando {n_workers} worker(s) paralelos ({len(proxy_pool)} proxies disponibles).")

    shared_state = {
        "completed": len(completed_keys),
        "stopped": False,
        "total": urls_len,
    }
    checkpoint_lock = asyncio.Lock()
    uncertain_zero_urls = []

    log_event("session_start", date=date_formatted, total_urls=urls_len,
              already_completed=len(completed_keys), queued=queued, workers=n_workers)

    # Guardar checkpoint inicial
    save_checkpoint_v2(completed_keys, urls_len, date_formatted)

    # Limpiar circuit breaker t=bv de sesiones anteriores
    reset_tbv_counter()

    async with async_playwright() as pw:
        tasks = []
        for i in range(n_workers):
            w_id = i + 1
            # Cada worker tiene su propia copia del dict proxy (para regenerar session independientemente)
            w_proxy = dict(proxy_pool[i]) if i < len(proxy_pool) else dict(proxy_pool[0])
            w_headless = headless or w_id > 1  # Worker 1 visible salvo --headless
            tasks.append(asyncio.create_task(
                _trends_worker(
                    worker_id=w_id,
                    pw=pw,
                    queue=queue,
                    proxy_cfg=w_proxy,
                    date_formatted=date_formatted,
                    iso_year=iso_year,
                    iso_week=iso_week,
                    subzones_data=subzones_data,
                    uncertain_zero_urls=uncertain_zero_urls,
                    shared_state=shared_state,
                    checkpoint_lock=checkpoint_lock,
                    completed_keys=completed_keys,
                    headless=w_headless,
                )
            ))

        # Monitor de stop flag + tecla "s" como tarea aparte
        async def _stop_watcher():
            # Importar msvcrt para detección de teclas en Windows (no-bloqueante)
            _kbhit = None
            _getch = None
            if sys.platform == "win32":
                try:
                    import msvcrt
                    _kbhit = msvcrt.kbhit
                    _getch = msvcrt.getch
                    print("ℹ️  Pulsa 's' en cualquier momento para detener el tracker (se guarda todo el progreso).")
                except ImportError:
                    pass

            while not shared_state["stopped"]:
                # Check file-based stop flag
                if STOP_FLAG_FILE.exists():
                    print("🔴 Stop flag detected. Señalando parada a todos los workers...")
                    shared_state["stopped"] = True
                    return
                # Check keyboard "s" key (Windows only, non-blocking)
                if _kbhit and _kbhit():
                    key = _getch()
                    if key in (b's', b'S'):
                        print("\n🛑 Tecla 's' pulsada. Deteniendo tracker tras completar URLs en curso...")
                        shared_state["stopped"] = True
                        return
                await asyncio.sleep(0.3)
        tasks.append(asyncio.create_task(_stop_watcher()))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Log errores de workers
        for i, r in enumerate(results[:-1]):  # Excluir _stop_watcher
            if isinstance(r, Exception):
                print(f"ERR: Worker {i+1} terminó con error: {r}")

        # --- DOUBLE-CHECK PHASE ---
        if uncertain_zero_urls and not shared_state["stopped"]:
            print(f"\n🔍 DOUBLE-CHECK PHASE: {len(uncertain_zero_urls)} items inciertos...")
            try:
                dc_proxy = build_playwright_proxy(proxy_pool[0])
                dc_profile_dir = get_profile_dir(89)
                os.makedirs(dc_profile_dir, exist_ok=True)
                _dc_launch_opts = {
                    "user_data_dir": dc_profile_dir,
                    "headless": True,
                    "viewport": {"width": 1280, "height": 800},
                    "user_agent": random.choice(USER_AGENTS),
                    "ignore_https_errors": True,
                    "proxy": dc_proxy,
                    "args": ["--disable-dev-shm-usage", "--no-first-run"],
                    "ignore_default_args": ["--enable-automation"],
                }
                if sys.platform == "win32":
                    _dc_launch_opts["channel"] = "msedge"
                dc_ctx = await pw.chromium.launch_persistent_context(**_dc_launch_opts)
                await dc_ctx.add_init_script(generate_stealth_script())
                dc_page = dc_ctx.pages[0] if dc_ctx.pages else await dc_ctx.new_page()

                for prov, zn, u, op in uncertain_zero_urls:
                    if STOP_FLAG_FILE.exists():
                        break
                    print(f"  -> Re-checking {prov} ({zn})...")
                    try:
                        await dc_page.goto(u, timeout=45000, wait_until="domcontentloaded")
                        await asyncio.sleep(random.uniform(5.0, 8.0))
                        await wait_for_verification(dc_page)

                        recheck_val = await extract_h1_number(dc_page)
                        if recheck_val > 0:
                            print(f"    ✨ Corrected! Found {recheck_val} properties.")
                            await save_to_db(date_formatted, iso_year, iso_week, prov, zn, op, recheck_val, subzone='')
                        elif await is_legit_zero_results(dc_page):
                            print(f"    ✅ Confirmed 0 properties.")
                            await save_to_db(date_formatted, iso_year, iso_week, prov, zn, op, 0, subzone='')
                        else:
                            print(f"    ❌ Still 0 properties (unconfirmed).")
                    except Exception as re_e:
                        print(f"    ⚠️ Error in re-check: {re_e}")

                await dc_ctx.close()
                print("✅ Double-Check Phase completed.\n")
            except Exception as dc_e:
                print(f"WARN: Double-check phase falló: {dc_e}")

    # Final checkpoint
    save_checkpoint_v2(completed_keys, urls_len, date_formatted)
    completed = shared_state["completed"]
    stopped_by_user = shared_state["stopped"]

    log_event("session_end", completed=completed, total=urls_len,
              finished=(completed >= urls_len), stopped_by_user=stopped_by_user)

    if completed >= urls_len:
        clear_resume_point()
        print("Market Trends Tracking Completed Full List!")
    else:
        print(f"Tracker Stopped. {completed}/{urls_len} URLs procesadas.")
        save_resume_point(completed, date_formatted,
                          reason="stopped" if stopped_by_user else "incomplete",
                          urls_total=urls_len)
        if not stopped_by_user:
            try:
                # Adaptar parámetros para send_failure_email (espera start_index, urls_len, urls_data)
                pending_data = [(p, z, u, op, lv) for p, z, u, op, lv in urls_data
                                if (p, z, op) not in completed_keys]
                send_failure_email(0, len(pending_data), pending_data, date_formatted)
            except Exception as e:
                print(f"WARN: No se pudo enviar email de fallo: {e}")

    # Auto export to CSV locally at the end of run
    print("Initiating automatic database backup to CSV...", flush=True)
    auto_export_csv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--resume", action="store_true", default=True,
                        help="Resume from last checkpoint (activado por defecto)")
    parser.add_argument("--headless", action="store_true", help="Run browser in headless mode")
    parser.add_argument("--date", type=str, default=None, help="Forzar fecha del registro (formato DD-MM-YYYY)")
    args = parser.parse_args()

    asyncio.run(run_tracker(resume=True, headless=args.headless, force_date=args.date))

    # Esperar tecla antes de cerrar la ventana para que el usuario vea el resultado
    print("\nPulsa cualquier tecla para cerrar...")
    try:
        if sys.platform == "win32":
            import msvcrt
            msvcrt.getch()
        else:
            input()
    except (EOFError, KeyboardInterrupt):
        pass
