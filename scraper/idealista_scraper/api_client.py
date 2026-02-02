"""API Client for Idealista7 RapidAPI.

Provides reusable functions to fetch market data and map it to the project's data schema.
"""
import http.client
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Callable

# Add project root to path for shared imports
_PROJECT_ROOT = Path(__file__).parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Import from shared config
try:
    from shared.config import RAPIDAPI_HOST, RAPIDAPI_KEY, API_MAX_PRICE
except ImportError:
    # Fallback for standalone execution
    RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
    RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"
    API_MAX_PRICE = 300000

# Import ORDERED_BASE for schema mapping
try:
    from . import ORDERED_BASE
except ImportError:
    # Fallback for direct script execution
    from idealista_scraper import ORDERED_BASE


def fetch_api_page(page_num: int, location_id: str, operation: str = "rent", 
                   max_items: int = 40, location_name: str = "Search",
                   max_price: Optional[int] = None) -> Optional[dict]:
    """Fetch a single page from Idealista7 API.
    
    Args:
        page_num: Page number to fetch (1-indexed)
        location_id: Idealista location ID (e.g., '0-EU-ES-28' for Madrid)
        operation: 'rent' or 'sale'
        max_items: Max items per page (default 40)
        location_name: Human-readable location name
        max_price: Maximum price filter in EUR (uses API_MAX_PRICE from config if None)
    """
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    
    # URL Encode location name just in case
    import urllib.parse
    loc_encoded = urllib.parse.quote(location_name)
    
    # Build query with optional price filter
    # Use max_price if provided, else fall back to global config
    price_limit = max_price if max_price is not None else API_MAX_PRICE
    
    query = (f"/listhomes?order=relevance&operation={operation}&locationId={location_id}"
             f"&locationName={loc_encoded}&numPage={page_num}&maxItems={max_items}&location=es&locale=es")
    
    # Add price filter if set (only applies to 'sale' operation typically, but API may support for rent too)
    if price_limit is not None:
        query += f"&maxPrice={price_limit}"
    
    try:
        conn.request("GET", query, headers=headers)
        res = conn.getresponse()
        data = res.read()
        if res.status != 200:
            return {"error": res.status, "message": data.decode("utf-8")}
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        return {"error": 500, "message": str(e)}

def fmt_bool(val) -> str:
    """Format boolean to Si/No."""
    return "Sí" if val else "No"

def fmt_floor(val) -> Optional[str]:
    """Format floor number."""
    if val is None or val == "": return None
    s = str(val).lower()
    if s == "bj" or "bajo" in s: return "Bajo"
    if s == "en" or "entre" in s: return "Entresuelo"
    if s == "ss" or "semi" in s: return "Semisótano"
    if s == "st" or "sot" in s: return "Sótano"
    try:
        n = int(s)
        return f"{n}ª"
    except:
        return val

def normalize_tipo(item: dict) -> str:
    """Normalize property type to scraper conventions."""
    t = str(item.get('propertyType', '')).lower()
    dt = str(item.get('detailedType', {}).get('typology', '')).lower()
    
    if 'chalet' in t or 'house' in t or 'chalet' in dt: return 'Casa o chalet'
    if 'flat' in t or 'flat' in dt: return 'Piso'
    if 'penthouse' in t or 'penthouse' in dt: return 'Ático'
    if 'duplex' in t or 'duplex' in dt: return 'Dúplex'
    if 'studio' in t or 'studio' in dt: return 'Estudio'
    return item.get('propertyType', '')

def map_item_to_row(item: dict) -> dict:
    """Map API item to ORDERED_BASE schema."""
    features = item.get('features', {})
    price_info = item.get('priceInfo', {}).get('price', {})
    drop_info = price_info.get('priceDropInfo', {})
    
    row = {col: None for col in ORDERED_BASE}
    
    row['Titulo'] = item.get('suggestedTexts', {}).get('title', item.get('address'))
    row['price'] = item.get('price', 0)
    row['old price'] = drop_info.get('formerPrice')
    row['price change %'] = drop_info.get('priceDropPercentage')
    row['Ubicacion'] = item.get('suggestedTexts', {}).get('subtitle')
    
    drop_ts = item.get('dropDate') or item.get('priceDropDate')
    if drop_ts:
        try:
            dt = datetime.fromtimestamp(drop_ts / 1000)
            row['actualizado hace'] = f"Bajó el {dt.strftime('%d/%m')}"
        except:
            pass
    
    row['m2 construidos'] = item.get('size')
    row['habs'] = item.get('rooms')
    row['banos'] = item.get('bathrooms')
    row['Num plantas'] = item.get('floors')
    
    # Booleans
    row['Terraza'] = fmt_bool(features.get('hasTerrace'))
    has_pkg = features.get('hasParking', False)
    if 'parkingSpace' in item and item['parkingSpace'].get('hasParking'):
        has_pkg = True
    row['Garaje'] = fmt_bool(has_pkg)
    row['Trastero'] = fmt_bool(features.get('hasBoxRoom'))
    row['aire acond'] = fmt_bool(features.get('hasAirConditioning'))
    row['piscina'] = fmt_bool(features.get('hasSwimmingPool'))
    row['jardin'] = fmt_bool(features.get('hasGarden'))
    row['ascensor'] = fmt_bool(item.get('hasLift'))
    
    row['tipo'] = normalize_tipo(item)
    row['altura'] = fmt_floor(item.get('floor'))
    row['exterior'] = "Exterior" if item.get('exterior') else "Interior"
    
    row['Calle'] = item.get('address')
    row['Barrio'] = item.get('neighborhood')
    row['Distrito'] = item.get('district') or "Sin Distrito"
    row['Ciudad'] = item.get('municipality')
    row['Provincia'] = item.get('province')
    
    row['estado'] = "Buen estado" if item.get('status') == 'good' else item.get('status')
    row['Descripcion'] = item.get('description')
    row['URL'] = item.get('url')
    row['Fecha Scraping'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row['Anuncio activo'] = "TRUE" 
    row['nombre anunciante'] = item.get('contactInfo', {}).get('commercialName')
    
    return row

def fetch_data_generator(location_id: str, operation: str = "rent", max_pages: int = 50, 
                         on_log: Callable[[str, str], None] = None, location_name: str = "Search"):
    """Generator that yields rows (or pages) and logs progress."""
    
    total_items = 0
    all_rows = []
    
    for p in range(1, max_pages + 1):
        if on_log:
            on_log("INFO", f"API Fetching page {p}/{max_pages}...")
            
        json_data = fetch_api_page(p, location_id, operation, location_name=location_name)
        
        if not json_data:
            if on_log: on_log("ERR", "API returned no data/connection error")
            break
            
        if "error" in json_data:
            err_msg = json_data.get("message", "Unknown error")
            if on_log: on_log("ERR", f"API Error {json_data['error']}: {err_msg}")
            break
            
        items = json_data.get('elementList', [])
        if not items:
            if on_log: on_log("INFO", f"Page {p} empty. Finished.")
            break
            
        page_rows = []
        for item in items:
            row = map_item_to_row(item)
            page_rows.append(row)
            all_rows.append(row)
            
        total_items += len(page_rows)
        
        # Yield batch of rows
        yield {
            "type": "batch",
            "rows": page_rows
        }
        
        # Yield progress stats
        yield {
            "type": "progress",
            "page": p,
            "items": len(page_rows),
            "total": total_items
        }
        
        # Check pagination
        total_pages_api = json_data.get('totalPages', max_pages)
        if p >= total_pages_api:
            if on_log: on_log("INFO", f"Reached last page ({p}).")
            break
            
        time.sleep(0.5) # Rate limit courtesy
        
    return all_rows

