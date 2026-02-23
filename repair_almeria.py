import pandas as pd
import re
import os

def fold_text(text):
    if not text: return ""
    return str(text).lower().strip()

def is_actually_okupado(row):
    # Tags check (these are high confidence)
    # Note: Tags aren't explicitly in Excel columns unless we find them in 'Descripcion' or 'Ubicacion'? 
    # Actually, in extraction they come from 'raw_items'. 
    # Since we only have descriptions and basic fields in Excel, we use the text patterns.
    
    desc = fold_text(row.get('Descripcion', ''))
    
    # Restrictive logic
    body_patterns = r"\b(ocupada ilegal|ocupante sin t[ií]tulo|ocupaci[oó]n ilegal|vivienda ocupada|inmueble ocupado|sin posesi[oó]n)\b"
    
    if re.search(r"\bokupa\b", desc) or re.search(body_patterns, desc):
        return "Sí"
    if re.search(r"\bocupado\b", desc) and re.search(r"\b(tercero|persona|sin justo t[ií]tulo|sin posesi[oó]n)\b", desc):
        return "Sí"
    
    return "No"

file_path = r'scraper\salidas\idealista_Almería_venta.xlsx'
output_path = r'scraper\salidas\idealista_Almería_venta_REPARADO.xlsx'

print(f"Analyzing: {file_path}")
try:
    xls = pd.ExcelFile(file_path)
except Exception as e:
    print(f"Could not open {file_path}. Make sure it is closed. Error: {e}")
    sys.exit(1)

with pd.ExcelWriter(output_path, engine='openpyxl') as writer:

    for sheet in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet)
        if 'okupado' in df.columns:
            print(f"Repairing sheet: {sheet}")
            df['okupado'] = df.apply(is_actually_okupado, axis=1)
        df.to_excel(writer, sheet_name=sheet, index=False)

print(f"Repair complete: {file_path}")
