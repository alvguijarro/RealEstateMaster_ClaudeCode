import http.client
import json
import sys
import urllib.parse

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"

def search_location(query):
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    
    encoded_query = urllib.parse.quote(query)
    # Generic autocomplete endpoint
    endpoint = f"/locations/search?query={encoded_query}&locale=es"
    
    try:
        conn.request("GET", endpoint, headers=headers)
        res = conn.getresponse()
        data = res.read()
        
        if res.status != 200:
            print(f"Error: {res.status}")
            return
            
        json_data = json.loads(data.decode("utf-8"))
        print(f"\n--- Resultados para '{query}' ---")
        
        # Check structure (usually a list of locations)
        # Adapt based on actual response structure if needed
        locations = json_data if isinstance(json_data, list) else json_data.get('locations', [])
        
        for loc in locations:
            # Look for locationId or id
            lid = loc.get('locationId') or loc.get('id')
            name = loc.get('name') or loc.get('text')
            category = loc.get('type') or loc.get('category')
            print(f"ID: {lid} | {name} ({category})")
            
    except Exception as e:
        print(f"Exception: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python scripts/get_locations.py \"Nombre de Ciudad\"")
    else:
        search_location(sys.argv[1])
