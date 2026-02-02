import http.client
import json

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"

def test_call(desc, params):
    print(f"--- Testing: {desc} ---")
    conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
    headers = {
        'x-rapidapi-key': RAPIDAPI_KEY,
        'x-rapidapi-host': RAPIDAPI_HOST
    }
    
    url = f"/listhomes?{params}&location=es&locale=es"
    print(f"URL: {url}")
    
    try:
        conn.request("GET", url, headers=headers)
        res = conn.getresponse()
        data = res.read().decode("utf-8")
        
        print(f"Status: {res.status}")
        try:
            j = json.loads(data)
            if 'elementList' in j:
                print(f"Success! Found {len(j['elementList'])} elements.")
            else:
                print("Response JSON (partial):", data[:200])
        except:
            print("Response Body:", data[:200])
            
    except Exception as e:
        print(f"Error: {e}")
    print("\n")

# 1. Valid ID (Madrid City) - Fixed maxItems
test_call("Madrid City (Known ID)", "order=relevance&operation=sale&locationId=0-EU-ES-28-07-001-079&locationName=Madrid&numPage=1&maxItems=40")

# 2. Province ID Hypothesis (Madrid Province = 28)
test_call("Madrid Province (0-EU-ES-28)", "order=relevance&operation=sale&locationId=0-EU-ES-28&locationName=Madrid&numPage=1&maxItems=40")

# 3. Province ID Hypothesis (Barcelona Province = 08)
test_call("Barcelona Province (0-EU-ES-08)", "order=relevance&operation=sale&locationId=0-EU-ES-08&locationName=Barcelona&numPage=1&maxItems=40")

# 4. Name Only (No ID) - Fixed maxItems
test_call("Name Only (No ID)", "order=relevance&operation=sale&locationName=Madrid&numPage=1&maxItems=40")
