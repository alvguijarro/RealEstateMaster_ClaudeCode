import pandas as pd
import os
import glob

output_dir = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
excel_files = glob.glob(os.path.join(output_dir, "idealista_*.xlsx"))

results = []

print(f"{'File':<50} | {'Total':<8} | {'Okupados':<8} | {'%'}")
print("-" * 80)

for file_path in excel_files:
    if "_REPARADO" in file_path:
        continue
    try:
        xls = pd.ExcelFile(file_path)
        total_rows = 0
        okupados = 0
        
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet)
            total_rows += len(df)
            if 'okupado' in df.columns:
                okupados += (df['okupado'] == 'Sí').sum()
        
        pct = (okupados / total_rows * 100) if total_rows > 0 else 0
        filename = os.path.basename(file_path)
        print(f"{filename:<50} | {total_rows:<8} | {okupados:<8} | {pct:.2f}%")
        
        if pct > 10: # Threshold for "likely affected"
            results.append(filename)
            
    except Exception as e:
        # print(f"Error reading {file_path}: {e}")
        pass

print("\n\nLately affected files (>10%):")
for f in results:
    print(f" - {f}")
