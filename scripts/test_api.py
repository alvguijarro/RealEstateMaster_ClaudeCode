import http.client
import json

conn = http.client.HTTPSConnection("idealista2.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d",
    'x-rapidapi-host': "idealista2.p.rapidapi.com"
}

print("Querying Idealista2 API (RapidAPI)...")
try:
    # Testing standard endpoint for APIDojo style
    # Usually: /properties/list or just /list
    conn.request("GET", "/properties/list?locationId=0-EU-ES-28&operation=rent&numPage=1&maxItems=1&location=es&locale=es", headers=headers)

    res = conn.getresponse()
    data = res.read()
    
    decoded_data = data.decode("utf-8")
    
    print(f"Status: {res.status}")
    try:
        json_obj = json.loads(decoded_data)
        print(json.dumps(json_obj, indent=2, ensure_ascii=False))
    except json.JSONDecodeError:
        print("Response is not JSON:")
        print(decoded_data)

except Exception as e:
    print(f"Error: {e}")
