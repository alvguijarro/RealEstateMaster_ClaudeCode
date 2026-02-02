import http.client
import json
import urllib.parse
import sys

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"

def search_locations(query_text):
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    
    # Try different endpoints that might work for autocomplete/search
    # Option 1: /locations/search (Standard? Failed before with 404/400?)
    # Option 2: /suggestion (Common in these APIs)
    # Option 3: /search (Generic)
    
    print(f"Searching for '{query_text}'...")
    
    encoded = urllib.parse.quote(query_text)
    
    # Let's try to mimic the 'listhomes' call but maybe there is a 'search' param?
    # Or try the one that worked for others: /locations/search?locationId=... no that's circular.
    
    # Discovery mode: Try to hit a few common paths
    endpoints = [
        # Explicit locations/autocomplete guesses
        f"/locations?location=es&prefix={encoded}",
        f"/locations?location=es&query={encoded}",
        f"/locations/query?location=es&text={encoded}",
        f"/search?location=es&prefix={encoded}",
        f"/auto-complete?location=es&prefix={encoded}",
        f"/autosearch?location=es&prefix={encoded}",
        f"/suggest?location=es&prefix={encoded}",
        
        # Mimicking official structure variants
        f"/v1/locations?location=es&prefix={encoded}",
        f"/3.5/es/locations?prefix={encoded}",
        
        # Is it part of listhomes?
        f"/listhomes?locationName={encoded}&operation=rent&location=es&locale=es&numPage=1&maxItems=1" 
    ]
    
    for ep in endpoints:
        print(f"Testing endpoint: {ep}")
        try:
            conn.request("GET", ep, headers=headers)
            res = conn.getresponse()
            data = res.read()
            print(f"Status: {res.status}")
            if res.status == 200:
                print("SUCCESS!")
                print(data.decode("utf-8")[:500] + "...") # Print first 500 chars
                return
            else:
                # print(data.decode("utf-8"))
                pass
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    q = "Toledo"
    if len(sys.argv) > 1:
        q = sys.argv[1]
    search_locations(q)
