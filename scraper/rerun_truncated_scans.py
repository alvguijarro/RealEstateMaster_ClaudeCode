import pandas as pd
import os
import glob
import subprocess
import sys

directory = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
pattern = os.path.join(directory, "API_BATCH_*.xlsx")
files = glob.glob(pattern)

to_rerun = {"rent": [], "sale": []}

print("Analyzing files for truncation...")

for f in files:
    filename = os.path.basename(f)
    if "_updated" in filename:
        continue
        
    try:
        # Quick count
        xl = pd.ExcelFile(f)
        file_total = 0
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            if 'URL' in df.columns:
                file_total += df['URL'].dropna().nunique()
        
        # If it's near the 2000 limit, it was likely truncated
        if file_total >= 1990:
            print(f"Found truncated file: {filename} ({file_total} URLs)")
            
            # Extract parts: API_BATCH_Province_Name_operation_timestamp.xlsx
            # We need the province name and operation
            parts = filename.replace(".xlsx", "").split("_")
            # Usually: parts[0]=API, parts[1]=BATCH, parts[2...n-2]=Name, parts[n-1]=op, parts[n]=timestamp
            # Example: API_BATCH_Santa_Cruz_de_Tenerife_rent_20260202_1246
            
            # Simple heuristic: operation is usually 'rent' or 'sale'
            op = None
            name_parts = []
            for i in range(2, len(parts)):
                if parts[i] in ["rent", "sale"]:
                    op = parts[i]
                    break
                name_parts.append(parts[i])
            
            if op and name_parts:
                province_name = " ".join(name_parts)
                to_rerun[op].append(province_name)
                
    except Exception as e:
        print(f"Error checking {filename}: {e}")

print("\nPlan:")
for op, provs in to_rerun.items():
    if provs:
        print(f"  {op.upper()}: {', '.join(provs)}")

# Launch the scans
for op, provs in to_rerun.items():
    if not provs:
        continue
    
    prov_str = ",".join(provs)
    cmd = [sys.executable, "scripts/batch_api_scan.py", "--operation", op, "--max-pages", "2000", "--provinces", prov_str]
    print(f"\nLaunching: {' '.join(cmd)}")
    subprocess.run(cmd, cwd=r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster")
