import pandas as pd
import json
import os
from pathlib import Path

# Paths
EXCEL_PATH = Path("scraper/salidas/idealista_Madrid_venta.xlsx")
JSON_PATH = Path("scraper/salidas/.enrich_state.json")

def sync_and_clean():
    if not EXCEL_PATH.exists() or not JSON_PATH.exists():
        print("Missing files")
        return

    # Load JSON
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        state = json.load(f)
    json_urls = state.get("enriched_urls", [])
    print(f"URLs in JSON: {len(json_urls)}")

    # Load Excel
    sheets = pd.read_excel(EXCEL_PATH, sheet_name=None)
    df = pd.concat(sheets.values(), ignore_index=True)
    
    if "__enriched__" not in df.columns:
        excel_urls = set()
    else:
        # Check __enriched__ flag
        excel_urls = set(df[df["__enriched__"].astype(str).str.lower() == "true"]["URL"].dropna())
    
    print(f"URLs enriched in Excel: {len(excel_urls)}")
    
    # Keep only those in JSON that are actually enriched in Excel
    new_json_urls = [url for url in json_urls if url in excel_urls]
    
    print(f"URLs to KEEP in JSON: {len(new_json_urls)}")
    print(f"URLs REMOVED from JSON (to be re-scraped): {len(json_urls) - len(new_json_urls)}")
    
    # Save back to JSON
    state["enriched_urls"] = new_json_urls
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)
    
    print("SUCCESS: JSON history synchronized with current Excel.")

if __name__ == "__main__":
    sync_and_clean()
