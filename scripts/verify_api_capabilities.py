import http.client
import json

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"
LOCATION_ID = "0-EU-ES-28-07-001-079"

conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
headers = {
    'x-rapidapi-key': RAPIDAPI_KEY,
    'x-rapidapi-host': RAPIDAPI_HOST
}

print("Querying Idealista7 API...")
try:
    # Query 1 item
    query = f"/listhomes?order=relevance&operation=rent&locationId={LOCATION_ID}&locationName=Madrid&numPage=1&maxItems=40&location=es&locale=es"
    conn.request("GET", query, headers=headers)
    res = conn.getresponse()
    data = res.read()
    
    print(f"Status: {res.status}")
    if res.status == 200:
        json_obj = json.loads(data.decode("utf-8"))
        print(json.dumps(json_obj, indent=2, ensure_ascii=False))
    else:
        print(data.decode("utf-8"))

except Exception as e:
    print(f"Error: {e}")
