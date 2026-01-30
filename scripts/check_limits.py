import http.client
import json

conn = http.client.HTTPSConnection("idealista7.p.rapidapi.com")

headers = {
    'x-rapidapi-key': "0f45666904mshbae4e59c6a93975p1c04c7jsn7c1c3240e93d",
    'x-rapidapi-host': "idealista7.p.rapidapi.com"
}

print("Checking API Limits...")
try:
    # Minimal request to get headers
    conn.request("GET", "/listhomes?locationId=0-EU-ES-28-07-001-079&operation=rent&numPage=1&maxItems=1", headers=headers)
    res = conn.getresponse()
    
    print("--- HEADERS ---")
    for k, v in res.getheaders():
        if "ratelimit" in k.lower() or "quota" in k.lower():
            print(f"{k}: {v}")
            
    print(f"Status: {res.status}")

except Exception as e:
    print(f"Error: {e}")
