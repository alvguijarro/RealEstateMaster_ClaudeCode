import http.client
import json

RAPIDAPI_HOST = "idealista7.p.rapidapi.com"
RAPIDAPI_KEY = "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d"

conn = http.client.HTTPSConnection(RAPIDAPI_HOST)
headers = {
    'x-rapidapi-key': RAPIDAPI_KEY,
    'x-rapidapi-host': RAPIDAPI_HOST
}

ids_to_test = [
    "0-EU-ES-45", # Toledo Province
    "0-EU-ES-45-168", # Toledo Municipality (Random guess based on alphabetical index?) - No, usually code. 45168 is postal code?
    # Toledo municipality code is 168 in INE? 45168.
    "0-EU-ES-45-168-001-001",
    "0-EU-ES-45-00-000-000"
]

print("Testing Toledo Location IDs...")

# Try searching via 'suggest' endpoint which might exist?
# Some docs say '/locations/query' or similar.
# Let's try to just hit 'listhomes' with the province ID and see the "locationName" in response or just get data.

lid = "0-EU-ES-45"
print(f"Testing ID: {lid}")
query = f"/listhomes?order=relevance&operation=rent&locationId={lid}&locationName=Toledo&numPage=1&maxItems=40&location=es&locale=es"

try:
    conn.request("GET", query, headers=headers)
    res = conn.getresponse()
    data = res.read()
    print(f"Status: {res.status}")
    if res.status == 200:
        json_obj = json.loads(data.decode("utf-8"))
        print("Success! Found data.")
        # Print first item location to verify
        if json_obj.get('elementList'):
            print("First item address:", json_obj['elementList'][0].get('address'))
            print("First item district:", json_obj['elementList'][0].get('district'))
            print("First item municipality:", json_obj['elementList'][0].get('municipality'))
        else:
            print("No items found.")
            print(json.dumps(json_obj, indent=2))
    else:
        print("Error response:")
        print(data.decode("utf-8"))

except Exception as e:
    print(f"Error: {e}")
