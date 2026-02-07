import requests
import time
import unicodedata
import re
import sys

# Force UTF-8 output
sys.stdout.reconfigure(encoding='utf-8')

PROVINCES_LIST = [
    {"id": "0-EU-ES-01", "name": "Alava"}, {"id": "0-EU-ES-02", "name": "Albacete"}, {"id": "0-EU-ES-03", "name": "Alicante"}, 
    {"id": "0-EU-ES-04", "name": "Almeria"}, {"id": "0-EU-ES-05", "name": "Avila"}, {"id": "0-EU-ES-06", "name": "Badajoz"},
    {"id": "0-EU-ES-07", "name": "Baleares"}, {"id": "0-EU-ES-08", "name": "Barcelona"}, {"id": "0-EU-ES-09", "name": "Burgos"},
    {"id": "0-EU-ES-10", "name": "Caceres"}, {"id": "0-EU-ES-11", "name": "Cadiz"}, {"id": "0-EU-ES-12", "name": "Castellon"},
    {"id": "0-EU-ES-13", "name": "Ciudad Real"}, {"id": "0-EU-ES-14", "name": "Cordoba"}, {"id": "0-EU-ES-15", "name": "A Coruna"},
    {"id": "0-EU-ES-16", "name": "Cuenca"}, {"id": "0-EU-ES-17", "name": "Girona"}, {"id": "0-EU-ES-18", "name": "Granada"},
    {"id": "0-EU-ES-19", "name": "Guadalajara"}, {"id": "0-EU-ES-20", "name": "Guipuzcoa"}, {"id": "0-EU-ES-21", "name": "Huelva"},
    {"id": "0-EU-ES-22", "name": "Huesca"}, {"id": "0-EU-ES-23", "name": "Jaen"}, {"id": "0-EU-ES-24", "name": "Leon"},
    {"id": "0-EU-ES-25", "name": "Lleida"}, {"id": "0-EU-ES-26", "name": "La Rioja"}, {"id": "0-EU-ES-27", "name": "Lugo"},
    {"id": "0-EU-ES-28", "name": "Madrid"}, {"id": "0-EU-ES-29", "name": "Malaga"}, {"id": "0-EU-ES-30", "name": "Murcia"},
    {"id": "0-EU-ES-31", "name": "Navarra"}, {"id": "0-EU-ES-32", "name": "Ourense"}, {"id": "0-EU-ES-33", "name": "Asturias"},
    {"id": "0-EU-ES-34", "name": "Palencia"}, {"id": "0-EU-ES-35", "name": "Las Palmas"}, {"id": "0-EU-ES-36", "name": "Pontevedra"},
    {"id": "0-EU-ES-37", "name": "Salamanca"}, {"id": "0-EU-ES-38", "name": "Santa Cruz de Tenerife"},
    {"id": "0-EU-ES-39", "name": "Cantabria"}, {"id": "0-EU-ES-40", "name": "Segovia"}, {"id": "0-EU-ES-41", "name": "Sevilla"},
    {"id": "0-EU-ES-42", "name": "Soria"}, {"id": "0-EU-ES-43", "name": "Tarragona"}, {"id": "0-EU-ES-44", "name": "Teruel"},
    {"id": "0-EU-ES-45", "name": "Toledo"}, {"id": "0-EU-ES-46", "name": "Valencia"}, {"id": "0-EU-ES-47", "name": "Valladolid"},
    {"id": "0-EU-ES-48", "name": "Vizcaya"}, {"id": "0-EU-ES-49", "name": "Zamora"}, {"id": "0-EU-ES-50", "name": "Zaragoza"},
    {"id": "0-EU-ES-51", "name": "Ceuta"}, {"id": "0-EU-ES-52", "name": "Melilla"}
]

def slugify(text):
    text = unicodedata.normalize('NFKD', text).encode('ascii', 'ignore').decode('utf-8')
    text = re.sub(r'[^\w\s-]', '', text).strip().lower()
    return re.sub(r'[-\s]+', '-', text)

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
}

print("| Provincia | URL Generada | Estado | Acción Sugerida |")
print("|---|---|---|---|")

session = requests.Session()
session.headers.update(headers)

for p in PROVINCES_LIST:
    name = p['name']
    slug = slugify(name)
    
    # 1. Try standard pattern: {slug}-provincia
    url_standard = f"https://www.idealista.com/alquiler-viviendas/{slug}-provincia/"
    
    # Special overrides that might already exist in scraper logic? We test standard first.
    
    try:
        # Check Standard
        r = session.get(url_standard, timeout=5, allow_redirects=True)
        is_404 = False
        if r.status_code == 404 or "no corresponde a ninguna página" in r.text or "404" in r.text:
            is_404 = True
        
        status_display = r.status_code
        if is_404: status_display = "404 (Not Found)"
        elif r.status_code == 403: status_display = "403 (Blocked)"
        elif r.history: status_display = f"301 -> {r.status_code}"

        if not is_404 and r.status_code == 200:
            print(f"| {name} | `{slug}-provincia` | ✅ 200 OK | Ninguna |")
        else:
            # 2. Try Alternate: {slug} (valid for single-province communities usually)
            url_alt = f"https://www.idealista.com/alquiler-viviendas/{slug}/"
            r2 = session.get(url_alt, timeout=5, allow_redirects=True)
            
            is_404_alt = False
            if r2.status_code == 404 or "no corresponde a ninguna página" in r2.text or "404" in r2.text:
                is_404_alt = True
            
            if not is_404_alt and r2.status_code == 200:
                print(f"| {name} | `{slug}-provincia` | ❌ {status_display} | Usar `{slug}` |")
            else:
                # 3. Try Cultural/Co-official names if needed
                alt_slugs = []
                if slug == 'alava': alt_slugs.append('araba')
                if slug == 'vizcaya': alt_slugs.append('bizkaia')
                if slug == 'guipuzcoa': alt_slugs.append('gipuzkoa')
                if slug == 'baleares': alt_slugs.extend(['balears-illes', 'illes-balears'])
                if slug == 'a-coruna': alt_slugs.append('a-coruna') # Already checked?
                
                found = False
                for alt in alt_slugs:
                    url_culture = f"https://www.idealista.com/alquiler-viviendas/{alt}/"
                    r3 = session.get(url_culture, timeout=5)
                    if r3.status_code == 200 and not ("no corresponde a ninguna página" in r3.text):
                        print(f"| {name} | `{slug}-provincia` | ❌ {status_display} | Usar `{alt}` |")
                        found = True
                        break
                
                if not found:
                    print(f"| {name} | `{slug}-provincia` | ❌ {status_display} | ⚠️ REVISAR MANUALMENTE |")

    except Exception as e:
        print(f"| {name} | `{slug}-provincia` | 💥 Error | {str(e)} |")
    
    time.sleep(0.2)
