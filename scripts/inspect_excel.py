import pandas as pd
import os

path = r'C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Baleares_venta.xlsx'

if not os.path.exists(path):
    print(f"File not found: {path}")
else:
    print(f"Inspecting: {path}")
    try:
        with pd.ExcelFile(path) as xls:
            print(f"Sheets: {xls.sheet_names}")
            for sh in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sh, nrows=5)
                print(f"\nSheet '{sh}':")
                print(f"  Columns: {df.columns.tolist()}")
                print(f"  Row count (approx): {pd.read_excel(xls, sheet_name=sh, usecols=[0]).shape[0]}")
    except Exception as e:
        print(f"Error: {e}")
