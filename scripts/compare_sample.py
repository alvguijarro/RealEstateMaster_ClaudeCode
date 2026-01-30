import json
import pandas as pd

def normalize_tipo(t, detailed):
    t = str(t).lower()
    d = str(detailed).lower()
    if 'chalet' in t or 'house' in t or 'chalet' in d: return 'Casa/Chalet'
    if 'flat' in t or 'flat' in d: return 'Piso'
    if 'penthouse' in t or 'penthouse' in d: return 'Ático'
    if 'duplex' in t or 'duplex' in d: return 'Dúplex'
    if 'studio' in t or 'studio' in d: return 'Estudio'
    return t

def map_item(item):
    return {
        'Título': item.get('suggestedTexts', {}).get('title', ''),
        'Precio': item.get('price'),
        'm2 construidos': item.get('size'),
        'habs': item.get('rooms'),
        'banos': item.get('bathrooms'),
        'tipo': normalize_tipo(item.get('propertyType'), item.get('detailedType', {}).get('typology')),
        'Distrito': item.get('district'),
        'Barrio': item.get('neighborhood'),
        'garaje': item.get('features', {}).get('hasParking', False) or item.get('parkingSpace', {}).get('hasParking', False),
        'terraza': item.get('features', {}).get('hasTerrace', False),
        'ascensor': item.get('hasLift', False),
        'exterior': item.get('exterior', False),
        'URL': item.get('url')
    }

try:
    with open('api_response.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    items = data.get('elementList', [])
    mapped_items = [map_item(i) for i in items[:5]] # Take 5 sample items
    
    df = pd.DataFrame(mapped_items)
    
    print("--- COMPRARACION DE MUESTRA (API -> SCRAPER FORMAT) ---")
    print(df.to_string(index=False))
    print("\n--- CAMPOS EXTRA DISPONIBLES EN API ---")
    print("- Latitud/Longitud (Exactas)")
    print("- Agencia Inmobiliaria")
    print("- Historial de Bajadas de Precio (DropValue, Date)")
    print("- Certificado Energético (status)")
    
except Exception as e:
    print(f"Error: {e}")
