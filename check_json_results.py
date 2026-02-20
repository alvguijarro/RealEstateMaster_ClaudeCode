import json
import os
import glob

def check_json():
    salidas_dir = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\analyzer\salidas"
    json_files = sorted(glob.glob(os.path.join(salidas_dir, "resultado_Baix_*.json")), key=os.path.getmtime, reverse=True)
    
    if not json_files:
        print("No JSON files found.")
        return
        
    latest_file = json_files[0]
    print(f"Checking latest JSON: {latest_file}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    if isinstance(data, list):
        opps = data
        top100 = []
    else:
        opps = data.get('opportunities', [])
        top100 = data.get('top_100', [])
        
    print(f"Total Opportunities: {len(opps)}")
    print(f"Total Top 100: {len(top100)}")
    
    def search_in_list(l, name):
        results = []
        for i, item in enumerate(l):
            price = item.get('Precio')
            # Check for 159000 or 159k as string/number
            if price == 159000 or price == 159000.0 or str(price) == '159000':
                results.append((i, item))
        return results

    print("\nSearching for 159k in Opportunities:")
    found_opps = search_in_list(opps, "Opportunities")
    for i, item in found_opps:
        print(f"  [{i}] {item.get('Propiedad')} | {item.get('Distrito')} | yield: {item.get('Rentabilidad_Bruta_%')}")

    print("\nSearching for 159k in Top 100:")
    found_top = search_in_list(top100, "Top 100")
    for i, item in found_top:
        print(f"  [{i}] {item.get('Propiedad')} | {item.get('Distrito')} | yield: {item.get('Rentabilidad_Bruta_%')}")

    if not found_opps and not found_top:
        print("\nNOT FOUND. Showing prices of top 5 items in Top 100 for context:")
        for item in top100[:5]:
             print(f"  {item.get('Precio')} EUR | yield: {item.get('Rentabilidad_Bruta_%')}")
        
        print("\nShowing yields of bottom 5 items in Top 100:")
        for item in top100[-5:]:
             print(f"  {item.get('Precio')} EUR | yield: {item.get('Rentabilidad_Bruta_%')}")

if __name__ == "__main__":
    check_json()
