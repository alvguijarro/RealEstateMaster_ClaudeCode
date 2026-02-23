import pandas as pd
import re

def fold_text(text):
    if not text: return ""
    return str(text).lower().strip()

file_path = r'scraper\salidas\idealista_Almería_venta.xlsx'
df = pd.read_excel(file_path, sheet_name=None)
all_df = pd.concat(df.values())

okupados = all_df[all_df['okupado'] == 'Sí']
print(f"Total Okupados to analyze: {len(okupados)}")

# Let's check the first 5
for i, row in okupados.head(5).iterrows():
    desc = str(row.get('Descripcion', ''))
    # We don't have the 'full_text' in the Excel, but we have 'Descripcion'.
    # If it's NOT in the description, then it MUST have been in the 'full_text' (body).
    
    desc_fold = fold_text(desc)
    
    # Logic from extractors.py
    is_ok = False
    body_patterns = r"\b(ocupada ilegal|ocupante sin t[ií]tulo|ocupaci[oó]n ilegal|vivienda ocupada|inmueble ocupado|sin posesi[oó]n)\b"
    
    if re.search(r"\bokupa\b", desc_fold) or re.search(body_patterns, desc_fold):
        is_ok = True
    elif re.search(r"\bocupado\b", desc_fold) and re.search(r"\b(tercero|persona|sin justo t[ií]tulo|sin posesi[oó]n)\b", desc_fold):
        is_ok = True

    print(f"\nURL: {row.get('URL')}")
    if is_ok:
        print(f"  VERIFIED OKUPADO per new logic.")
    else:
        print(f"  FALSE POSITIVE (Filtered out by new logic).")

