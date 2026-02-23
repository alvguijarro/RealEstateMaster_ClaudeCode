
import pandas as pd
import os
import re

def fold_text(text):
    if not text: return ""
    return str(text).lower().strip()

def verify_okupado(desc, items):
    """Refined logic from extractors.py to verify if 'Sí' is real."""
    desc_fold = fold_text(desc)
    joined_items = fold_text(" | ".join(map(str, items)))
    
    # 1. Direct high-confidence tags
    if "ocupada ilegalmente" in joined_items or "vivienda ocupada" in joined_items:
        return True
        
    # 2. Pattern check (matching the new extractors.py logic)
    body_patterns = r"\b(ocupada ilegal|ocupante sin t[ií]tulo|ocupaci[oó]n ilegal|vivienda ocupada|inmueble ocupado|sin posesi[oó]n)\b"
    
    # Safe checks
    if re.search(r"\bokupa\b", desc_fold) or re.search(body_patterns, desc_fold):
        return True
    
    if re.search(r"\bocupado\b", desc_fold) and re.search(r"\b(tercero|persona|sin justo t[ií]tulo|sin posesi[oó]n)\b", desc_fold):
        return True
        
    return False

output_dir = r"C:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas"
files_to_fix = [
    "idealista_Madrid-este_venta.xlsx",
    "idealista_Madrid-corona sur este_venta.xlsx",
    "idealista_Madrid-Corredor del Henares_venta.xlsx",
    "idealista_Madrid-norte_venta.xlsx",
    "idealista_Madrid-oeste_venta.xlsx",
    "idealista_Talavera_venta_status_updated.xlsx",
    "idealista_Madrid-oeste_venta - idealista_Madrid-este_venta_MERGE.xlsx"
]

for filename in files_to_fix:
    path = os.path.join(output_dir, filename)
    if not os.path.exists(path):
        print(f"Skipping {filename} (not found)")
        continue
        
    print(f"Repairing: {filename}...")
    try:
        xls = pd.ExcelFile(path)
        new_sheets = {}
        total_repaired = 0
        
        for sheet_name in xls.sheet_names:
            df = pd.read_excel(path, sheet_name=sheet_name)
            if 'okupado' not in df.columns:
                new_sheets[sheet_name] = df
                continue
            
            # Identify rows marked as 'Sí'
            mask = df['okupado'] == 'Sí'
            
            # Using a list to hold the count to avoid nonlocal issues in all Python versions
            stats = {'rep': 0}

            def apply_fix(row):
                desc = row.get('Descripcion', '')
                if verify_okupado(desc, []):
                    return 'Sí'
                else:
                    stats['rep'] += 1
                    return 'No'

            if mask.any():
                df.loc[mask, 'okupado'] = df[mask].apply(apply_fix, axis=1)
            
            total_repaired += stats['rep']
            new_sheets[sheet_name] = df
            
        # Save repaired file
        repaired_path = path.replace('.xlsx', '_REPARADO.xlsx')
        with pd.ExcelWriter(repaired_path) as writer:
            for name, df in new_sheets.items():
                df.to_excel(writer, sheet_name=name, index=False)
        
        print(f"  Done. Repaired {total_repaired} false positives. Saved to {os.path.basename(repaired_path)}")
        
    except Exception as e:
        print(f"  Error repairing {filename}: {e}")

print("\nBulk repair complete.")
