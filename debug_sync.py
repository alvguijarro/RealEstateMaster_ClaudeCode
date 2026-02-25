import pandas as pd
import json
import os
from pathlib import Path

# Paths
EXCEL_PATH = Path("scraper/salidas/idealista_Madrid_venta.xlsx")
JSON_PATH = Path("scraper/salidas/.enrich_state.json")

def check_sync():
    if not EXCEL_PATH.exists():
        print(f"Excel {EXCEL_PATH} not found")
        return
    
    if not JSON_PATH.exists():
        print(f"JSON {JSON_PATH} not found")
        return

    # Load JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)
    json_urls = set(state.get("enriched_urls", []))
    print(f"URLs in JSON: {len(json_urls)}")

    # Load Excel
    sheets = pd.read_excel(EXCEL_PATH, sheet_name=None)
    df = pd.concat(sheets.values(), ignore_index=True)
    
    if "__enriched__" not in df.columns:
        print("Column '__enriched__' not found in Excel")
        excel_urls = set()
    else:
        # Handle different types of __enriched__ values
        excel_urls = set(df[df["__enriched__"].astype(str).str.lower() == "true"]["URL"].dropna())
    
    print(f"URLs enriched in Excel: {len(excel_urls)}")
    
    # Missing in Excel but present in JSON
    missing = json_urls - excel_urls
    print(f"URLs to RECOVER (in JSON but not in Excel): {len(missing)}")
    
    if missing:
        print("\nFirst 10 missing URLs:")
        for url in list(missing)[:10]:
            print(f" - {url}")

if __name__ == "__main__":
    check_sync()
