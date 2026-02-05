import pandas as pd
import os
import glob
import sys

directory = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
pattern = os.path.join(directory, "API_BATCH_*.xlsx")
files = glob.glob(pattern)

truncated = {"rent": [], "sale": []}

for f in files:
    filename = os.path.basename(f)
    if "_updated" in filename:
        continue
        
    try:
        # Quick header check to avoid reading full file if possible? 
        # No, need row count.
        xl = pd.ExcelFile(f)
        total = 0
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            if 'URL' in df.columns:
                total += len(df)
        
        # Check strict 2000 limit (or close to it)
        if total >= 1990:
            parts = filename.replace(".xlsx", "").split("_")
            op = None
            name_parts = []
            for i in range(2, len(parts)):
                if parts[i] in ["rent", "sale"]:
                    op = parts[i]
                    break
                name_parts.append(parts[i])
            
            if op and name_parts:
                prov = " ".join(name_parts)
                truncated[op].append(prov)
                
    except: pass

# Print comma separated lists
print("LIST_RENT:" + ",".join(sorted(list(set(truncated["rent"])))))
print("LIST_SALE:" + ",".join(sorted(list(set(truncated["sale"])))))
