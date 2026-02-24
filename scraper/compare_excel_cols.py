import pandas as pd
import os

salidas_dir = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
ref_file = os.path.join(salidas_dir, "idealista_Madrid_venta.xlsx")
api_file = os.path.join(salidas_dir, "idealista_Madrid_venta_api.xlsx")

def get_cols(path):
    try:
        df = pd.read_excel(path, nrows=0)
        return list(df.columns)
    except Exception as e:
        return str(e)

ref_cols = get_cols(ref_file)
api_cols = get_cols(api_file)

print(f"--- REFERENCE ({os.path.basename(ref_file)}) ---")
print(ref_cols)
print(f"\n--- API ({os.path.basename(api_file)}) ---")
print(api_cols)

if isinstance(ref_cols, list) and isinstance(api_cols, list):
    only_in_ref = [c for c in ref_cols if c not in api_cols]
    only_in_api = [c for c in api_cols if c not in ref_cols]
    
    print("\n--- DIFFERENCES ---")
    print(f"Only in Reference: {only_in_ref}")
    print(f"Only in API: {only_in_api}")
    
    # Check for order differences
    common = [c for c in ref_cols if c in api_cols]
    common_api = [c for c in api_cols if c in ref_cols]
    if common != common_api:
        print("\n--- ORDER DIFFERENCE ---")
        print("The common columns are in different order.")
