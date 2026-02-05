import requests
import time
import unicodedata
import sys

# List from batch_api_scan.py
PROVINCES = [
    "Alava", "Albacete", "Alicante", "Almeria", "Avila", "Badajoz", "Baleares", "Barcelona", "Burgos", "Caceres",
    "Cadiz", "Castellon", "Ciudad Real", "Cordoba", "A Coruna", "Cuenca", "Girona", "Granada", "Guadalajara",
    "Guipuzcoa", "Huelva", "Huesca", "Jaen", "Leon", "Lleida", "La Rioja", "Lugo", "Madrid", "Malaga", "Murcia",
    "Navarra", "Ourense", "Asturias", "Palencia", "Las Palmas", "Pontevedra", "Salamanca", "Santa Cruz de Tenerife",
    "Cantabria", "Segovia", "Sevilla", "Soria", "Tarragona", "Teruel", "Toledo", "Valencia", "Valladolid",
    "Vizcaya", "Zamora", "Zaragoza", "Ceuta", "Melilla"
]

# Manual overrides for tricky slugs if standard normalization fails
SLUG_OVERRIDES = {
    "A Coruna": "a-coruna", # standard normalization handles space, but confirming
    "Baleares": "balears-illes", # Idealista specific
    "Vizcaya": "vizcaya", # or bizkaia? Idealista usually supports both or redirects
    "Guipuzcoa": "guipuzcoa", # or gipuzkoa?
    "Alava": "alava", # or araba?
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def normalize_slug(text):
    """Normalize text to slug (lowercase, ascii only, no spaces)"""
    # Normalize unicode characters to closest ASCII
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    return text.lower().strip().replace(" ", "-")

valid_urls = []
errors = []

print("Verifying 52 provinces for < 300k filter...")

for prov in PROVINCES:
    slug = SLUG_OVERRIDES.get(prov)
    if not slug:
        slug = normalize_slug(prov)
    
    # Try standard pattern
    url = f"https://www.idealista.com/venta-viviendas/{slug}-provincia/con-precio-hasta_300000/"
    
    try:
        # Slow down to avoid block
        time.sleep(1) 
        
        # HEAD first to be fast? Idealista blocks HEAD often. Use GET.
        # timeout=5 should be enough
        resp = requests.get(url, headers=HEADERS, timeout=10)
        
        if resp.status_code == 200:
            # Check for soft 404
            if "Lo sentimos" in resp.text or "no corresponde a ninguna página" in resp.text:
                 # Try alternative slugs?
                 errors.append(f"{prov} -> Soft 404 ({url})")
            else:
                 valid_urls.append(f"{prov}|{url}")
                 print(f"[OK] {prov}")
        elif resp.status_code == 403:
            print(f"[BLOCK] Blocked on {prov} ({resp.status_code})")
            # Stop if blocked
            break
        else:
            errors.append(f"{prov} -> {resp.status_code} ({url})")
            
    except Exception as e:
        errors.append(f"{prov} -> Error: {str(e)}")

print("\n\n=== RESULTS ===")
print("\nINVALID/ERRORS:")
for e in errors:
    print(e)

print("\nVALID LIST (Format: Province|URL):")
for v in valid_urls:
    print(v)
