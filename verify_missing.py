import pandas as pd
import numpy as np
import os
import sys

# Add current dir to path for analysis imports if needed
sys.path.append(os.getcwd())

def verify():
    file_path = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Baix_Segura_venta.xlsx"
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    print(f"Loading {file_path}...")
    xl = pd.ExcelFile(file_path)
    df = pd.concat([pd.read_excel(xl, sheet_name=s).assign(_source=s) for s in xl.sheet_names], ignore_index=True)
    
    print(f"Total rows loaded: {len(df)}")
    
    # Target properties
    target_1599 = df[df['price'] == 159900]
    target_159 = df[df['price'] == 159000]
    target_158 = df[df['price'] == 158000]
    
    print(f"Found {len(target_1599)} properties with price 159,900")
    print(f"Found {len(target_159)} properties with price 159,000")
    print(f"Found {len(target_158)} properties with price 158,000")

    print("\nDetalles 159.9k:")
    for i, row in target_1599.iterrows():
        print(f"  Row {i}: {row.get('Titulo')} | {row.get('Distrito')} | {row.get('m2 construidos')}m2")

    print("\nDetalles 159k:")
    for i, row in target_159.iterrows():
        print(f"  Row {i}: {row.get('Titulo')} | {row.get('Distrito')} | {row.get('m2 construidos')}m2")

    print("\nDetalles 158k:")
    for i, row in target_158.iterrows():
        print(f"  Row {i}: {row.get('Titulo')} | {row.get('Distrito')} | {row.get('m2 construidos')}m2")

    # Check for exclusions in 159k
    print("\nChecking for exclusions in 159k properties...")
    for i, row in target_159.iterrows():
        # search text
        search_text = " ".join([str(row.get(c, '')).lower() for c in ['Titulo', 'Descripcion', 'descripción'] if c in df.columns])
        is_okupa = 'okupad' in search_text or 'ocupado ilegalmente' in search_text
        is_nuda = 'nuda propiedad' in search_text
        print(f"  Row {i}: Okupa={is_okupa}, Nuda={is_nuda}")

    # Check common districts
    # Need to load Alquiler too to see if they share districts
    alq_file = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Baix_Segura_alquiler.xlsx"
    if os.path.exists(alq_file):
        xl_alq = pd.ExcelFile(alq_file)
        df_alq = pd.concat([pd.read_excel(xl_alq, sheet_name=s) for s in xl_alq.sheet_names], ignore_index=True)
        common_districts = set(df['Distrito'].dropna().unique()) & set(df_alq['Distrito'].dropna().unique())
        print(f"\nTotal districts in Venta: {len(df['Distrito'].unique())}")
        print(f"Total districts in Alquiler: {len(df_alq['Distrito'].unique())}")
        print(f"Common districts: {len(common_districts)}")
        
        # --- CHECK COMPARABLES FOR ROW 231 ---
        row_231 = df.iloc[231]
        print(f"\nChecking comparables for Row 231: {row_231.get('Titulo')} (103m2, 159k, Torre de la Horadada)")
        
        # Simple logic from analysis.py
        def norm_tipo(t):
            t = str(t).lower()
            if 'casa' in t or 'chalet' in t: return 'casa'
            return 'piso'
        
        v_tipo = norm_tipo(row_231.get('tipo', 'piso'))
        m2_v = row_231['m2 construidos']
        distrito_v = str(row_231.get('Distrito', '')).strip()
        
        if os.path.exists(alq_file):
            df_alq['tipo_norm'] = df_alq['tipo'].apply(norm_tipo)
            # Match by district and type
            comps = df_alq[
                (df_alq['Distrito'].astype(str).str.strip() == distrito_v) & 
                (df_alq['tipo_norm'] == v_tipo)
            ]
            print(f"  Found {len(comps)} rentals in '{distrito_v}' with type '{v_tipo}'")
            
            # Filter by M2 range (±40%)
            m2_comps = comps[(comps['m2 construidos'] >= m2_v * 0.6) & (comps['m2 construidos'] <= m2_v * 1.4)]
            print(f"  Found {len(m2_comps)} rentals within m2 range ({m2_v*0.6:.1f} - {m2_v*1.4:.1f})")
            
            if not m2_comps.empty:
                mediana_alq_m2 = m2_comps['price'].median() / m2_comps['m2 construidos'].median()
                renta_est = mediana_alq_m2 * m2_v
                yield_est = (renta_est * 12) / 159000 * 100
                print(f"  Estimated Rent: {renta_est:.2f} EUR/mo")
                print(f"  Estimated Yield (Bruta): {yield_est:.2f}%")
            else:
                print("  NO COMPARABLES FOUND by M2 range.")
        else:
            print("  Alquiler file not found.")
        
        for i, row in target.iterrows():
            d = row.get('Distrito')
            print(f"  Row {i}: Distrito '{d}' is in common: {d in common_districts}")
    else:
        print("\nAlquiler file not found, cannot check common districts.")

if __name__ == "__main__":
    verify()
