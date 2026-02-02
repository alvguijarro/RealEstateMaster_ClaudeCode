import http.client
import json

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"

def probe_page(page, location_id="0-EU-ES-12", location_name="Castellon"):
    print(f"--- Probing Page {page} for {location_name} ({location_id}) ---")
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    
    # URL Encode location name just in case
    import urllib.parse
    loc_encoded = urllib.parse.quote(location_name)

    query = (f"/listhomes?order=relevance&operation=sale&locationId={location_id}"
             f"&locationName={loc_encoded}&numPage={page}&maxItems=40&location=es&locale=es")
    
    try:
        conn.request("GET", query, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        
        if res.status != 200:
            print(f"Error Status: {res.status}")
            print(data[:200])
            return

        j = json.loads(data)
        items = j.get('elementList', [])
        total_pages = j.get('totalPages', 'Unknown')
        total_items = j.get('total', 'Unknown') # Some APIs return total items count explicitly
        
        print(f"Items found: {len(items)}")
        print(f"API says Total Pages: {total_pages}")
        if total_items != 'Unknown':
            print(f"API says Total Items: {total_items}")
            
        if items:
            print(f"First item: {items[0].get('address')} - {items[0].get('price')} EUR")
        else:
            print("No items in elementList.")
            
    except Exception as e:
        print(f"Exception: {e}")

# Probe page 50 (should exist)
probe_page(50)

# Probe page 51 (The suspected limit boundary)
probe_page(51)

# Probe page 60 (Deep probe)
probe_page(60)
