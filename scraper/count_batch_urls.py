import pandas as pd
import os
import glob

directory = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
pattern = os.path.join(directory, "API_BATCH_*.xlsx")
files = glob.glob(pattern)

total_urls = 0
filtered_files = []

for f in files:
    filename = os.path.basename(f)
    if "_updated" not in filename:
        filtered_files.append(f)
        try:
            # Read all sheets
            xl = pd.ExcelFile(f)
            file_total = 0
            for sheet in xl.sheet_names:
                df = xl.parse(sheet)
                if 'URL' in df.columns:
                    # Count non-empty URLs
                    count = df['URL'].dropna().nunique()
                    file_total += count
            
            print(f"{filename}: {file_total} URLs")
            total_urls += file_total
        except Exception as e:
            print(f"Error reading {filename}: {e}")

print("-" * 30)
print(f"TOTAL URLs across {len(filtered_files)} files: {total_urls}")
