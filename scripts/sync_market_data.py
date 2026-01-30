"""
Script to synchronize market data (Rent/Sale) using the Idealista7 API (RapidAPI).
Fetches massive amounts of data and exports it in the exact same format as the main scraper.
"""
import http.client
import json
import os
import sys
import time
from datetime import datetime
import pandas as pd
from pathlib import Path

# Add parent to path to import project modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from scraper.idealista_scraper import ORDERED_BASE
from scraper.idealista_scraper.excel_writer import export_split_by_distrito

# RAPIDAPI CONFIG
RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"  # User provided key

# CONFIG
LOCATION_ID = "0-EU-ES-28-07-001-079" # Madrid
OPERATION = "rent" # 'rent' or 'sale'
MAX_PAGES = 50 # 50 pages * 40 items = 2000 listings
OUTPUT_DIR = str(Path(__file__).parent.parent / "scraper" / "salidas")

def fetch_page(page_num):
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    # Using 'listhomes' endpoint
    query = f"/listhomes?order=relevance&operation={OPERATION}&locationId={LOCATION_ID}&locationName=Madrid&numPage={page_num}&maxItems=40&location=es&locale=es"
    
    try:
        conn.request("GET", query, headers=headers)
        res = conn.getresponse()
        data = res.read()
        if res.status != 200:
            print(f"Error: {res.status}")
            return None
        return json.loads(data.decode("utf-8"))
    except Exception as e:
        print(f"Exception: {e}")
        return None

def fmt_bool(val):
    """Convert boolean/truthy to 'Sí'/'No'."""
    return "Sí" if val else "No"

def fmt_floor(val):
    """Convert floor number/string to ordinal (1ª, 2ª) or special text."""
    if val is None or val == "":
        return None
    
    s = str(val).lower()
    if s == "bj" or "bajo" in s: return "Bajo"
    if s == "en" or "entre" in s: return "Entresuelo"
    if s == "ss" or "semi" in s: return "Semisótano"
    if s == "st" or "sot" in s: return "Sótano"
    
    # Try parsing number
    try:
        n = int(s)
        # 1 -> 1ª
        return f"{n}ª"
    except:
        return val # Return as is if unknown

def normalize_tipo(item):
    """Normalize API type to scraper format."""
    t = str(item.get('propertyType', '')).lower()
    dt = str(item.get('detailedType', {}).get('typology', '')).lower()
    
    if 'chalet' in t or 'house' in t or 'chalet' in dt: return 'Casa o chalet'
    if 'flat' in t or 'flat' in dt: return 'Piso'
    if 'penthouse' in t or 'penthouse' in dt: return 'Ático'
    if 'duplex' in t or 'duplex' in dt: return 'Dúplex'
    if 'studio' in t or 'studio' in dt: return 'Estudio'
    return item.get('propertyType', '')

def map_item_to_row(item):
    """Map API item to ORDERED_BASE schema with formatting."""
    features = item.get('features', {})
    price_info = item.get('priceInfo', {}).get('price', {})
    drop_info = price_info.get('priceDropInfo', {})
    
    row = {col: None for col in ORDERED_BASE}
    
    row['Titulo'] = item.get('suggestedTexts', {}).get('title', item.get('address'))
    row['price'] = item.get('price', 0)
    row['old price'] = drop_info.get('formerPrice')
    row['price change %'] = drop_info.get('priceDropPercentage')
    row['Ubicacion'] = item.get('suggestedTexts', {}).get('subtitle')
    
    # Date handling for "actualizado hace"
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
    
    # Booleans with "Sí"/"No" formatting
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
    
    # Location
    row['Calle'] = item.get('address')
    row['Barrio'] = item.get('neighborhood')
    row['Distrito'] = item.get('district') or "Sin Distrito"
    row['Ciudad'] = item.get('municipality')
    row['Provincia'] = item.get('province')
    
    row['estado'] = "Buen estado" if item.get('status') == 'good' else item.get('status')
    row['Descripcion'] = item.get('description')
    
    row['URL'] = item.get('url')
    row['Fecha Scraping'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    row['Anuncio activo'] = "TRUE" # Keep explicit boolean or string? Scraper uses "TRUE" usually or just empty implied. Let's use Boolean string if Excel needs it? "verdadero"? No, user asked for "Sí/No" for attributes. "Anuncio activo" is usually boolean in DataFrame. Let's keep it clean.
    row['nombre anunciante'] = item.get('contactInfo', {}).get('commercialName')
    
    return row

def main():
    print(f"--- SYNC MARKET DATA ({OPERATION.upper()}) ---")
    print(f"Target: {MAX_PAGES} pages...")
    
    all_rows = []
    
    for p in range(1, MAX_PAGES + 1):
        print(f"Fetching page {p}/{MAX_PAGES}...", end="\r")
        json_data = fetch_page(p)
        if not json_data: break
            
        items = json_data.get('elementList', [])
        if not items: break
            
        for item in items:
            all_rows.append(map_item_to_row(item))
            
        time.sleep(0.5) 
        
    print(f"\nCollected {len(all_rows)} listings.")
    if not all_rows: return

    # DataFrame creation
    df = pd.DataFrame(all_rows)
    
    # FILENAME
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    filename = f"API_{OPERATION.upper()}_{timestamp}_Distritos.xlsx"
    out_path = os.path.join(OUTPUT_DIR, filename)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print(f"Exporting (Splitting by District) to {out_path}...")
    
    # Use the project's native export function that handles splitting logic!
    # passing empty DataFrame for 'existing' as this is a fresh import
    try:
        export_split_by_distrito(
            existing_df=pd.DataFrame(),
            additions=all_rows,
            out_path=out_path,
            carry_cols=set()
        )
        print("Success! File saved with multiple tabs (one per District).")
    except Exception as e:
        print(f"Error exporting: {e}")

if __name__ == "__main__":
    main()
