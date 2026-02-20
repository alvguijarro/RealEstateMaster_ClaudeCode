import pandas as pd
import numpy as np
import os
import sys

def verify_full():
    venta_file = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Baix_Segura_venta.xlsx"
    alq_file = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster\scraper\salidas\idealista_Baix_Segura_alquiler.xlsx"
    
    if not os.path.exists(venta_file) or not os.path.exists(alq_file):
        print("Required files missing.")
        return

    print("--- PHASE 1: LOAD ---")
    xl_v = pd.ExcelFile(venta_file)
    df_venta = pd.concat([pd.read_excel(xl_v, sheet_name=s).assign(_source=s) for s in xl_v.sheet_names], ignore_index=True)
    xl_a = pd.ExcelFile(alq_file)
    df_alquiler = pd.concat([pd.read_excel(xl_a, sheet_name=s).assign(_source=s) for s in xl_a.sheet_names], ignore_index=True)
    print(f"Loaded Venta: {len(df_venta)}, Alquiler: {len(df_alquiler)}")

    # Track target indices
    target_159k_indices = df_venta[df_venta['price'] == 159000].index.tolist()
    target_158k_indices = df_venta[df_venta['price'] == 158000].index.tolist()
    target_1599_indices = df_venta[df_venta['price'] == 159900].index.tolist()
    
    print(f"Tracking: 159k={target_159k_indices}, 158k={target_158k_indices}, 159.9k={target_1599_indices}")

    print("\n--- PHASE 2: CLEAN ---")
    def clean_numeric(s):
        if s.dtype == 'object':
            s = s.astype(str).str.replace('.', '', regex=False).str.replace(',', '.', regex=False).str.replace(' ', '', regex=False)
            s = s.replace(['nan', 'None', ''], np.nan)
        return pd.to_numeric(s, errors='coerce')
    
    for col in ['price', 'm2 construidos', 'habs', 'banos']:
        df_venta[col] = clean_numeric(df_venta[col])
        df_alquiler[col] = clean_numeric(df_alquiler[col])
    
    # Exclude Okupa/Nuda (Simulated)
    # Filter districts
    common = set(df_venta['Distrito'].dropna().unique()) & set(df_alquiler['Distrito'].dropna().unique())
    df_venta = df_venta[df_venta['Distrito'].isin(common)].copy()
    df_alquiler = df_alquiler[df_alquiler['Distrito'].isin(common)].copy()
    
    df_venta = df_venta.dropna(subset=['price', 'm2 construidos', 'Distrito'])
    df_alquiler = df_alquiler.dropna(subset=['price', 'm2 construidos', 'Distrito'])
    
    df_venta['precio_m2'] = df_venta['price'] / df_venta['m2 construidos']
    df_alquiler['precio_m2'] = df_alquiler['price'] / df_alquiler['m2 construidos']
    
    print(f"After Clean: Venta={len(df_venta)}, Alquiler={len(df_alquiler)}")
    for idx in target_159k_indices:
        print(f"  Row {idx} survived Clean: {idx in df_venta.index}")

    print("\n--- PHASE 3: MARKET ---")
    zona_stats = df_venta.groupby('Distrito')['precio_m2'].agg([('mediana_venta_m2', 'median')]).reset_index()
    # Deduplicate before merge? No.
    df_venta = df_venta.merge(zona_stats, on='Distrito', how='left')
    
    print("\n--- PHASE 4: YIELDS ---")
    # Simulation of find_comparables + yield
    def get_est_rent(row, df_a):
        d = str(row['Distrito']).strip()
        m2 = row['m2 construidos']
        # Simple match
        comps = df_a[
            (df_a['Distrito'].astype(str).str.strip() == d) & 
            (df_a['m2 construidos'] >= m2 * 0.6) & 
            (df_a['m2 construidos'] <= m2 * 1.4)
        ]
        if comps.empty: return np.nan
        mediana_alq_m2 = comps['price'].median() / comps['m2 construidos'].median()
        return mediana_alq_m2 * m2

    df_venta['renta_estimada'] = df_venta.apply(lambda r: get_est_rent(r, df_alquiler), axis=1)
    df_venta['yield_bruta'] = (df_venta['renta_estimada'] * 12 / df_venta['price']) * 100
    
    all_targets = target_159k_indices + target_158k_indices + target_1599_indices
    # Filter to survivors
    target_results = df_venta.loc[df_venta.index.intersection(all_targets)].copy()
    
    print("\nResults for Target Properties:")
    print(target_results[['_source', 'price', 'm2 construidos', 'Distrito', 'renta_estimada', 'yield_bruta']].sort_values('yield_bruta', ascending=False))
    
    print("\n--- TOP 100 RANKING ---")
    df_venta = df_venta.sort_values('yield_bruta', ascending=False)
    df_venta['rank'] = range(1, len(df_venta) + 1)
    
    top100_cutoff_yield = df_venta.iloc[min(99, len(df_venta)-1)]['yield_bruta']
    print(f"Top 100 Cutoff Yield: {top100_cutoff_yield:.2f}%")
    
    print("\n--- DEEP DIVE ROW 486 (The 'Winner' 158k) ---")
    # Search by price 158000 again to be sure of the index in this run
    r486_subset = df_venta[df_venta['price'] == 158000]
    if not r486_subset.empty:
        r486 = r486_subset.iloc[0]
        print(f"  Titulo: {r486.get('Titulo')}")
        print(f"  m2: {r486.get('m2 construidos')}")
        print(f"  Yield: {r486.get('yield_bruta')}")
        print(f"  URL: {r486.get('URL')}")
        # Search for m2 in title/description
        search_text = " ".join([str(r486.get(c, '')).lower() for c in ['Titulo', 'Descripcion', 'descripción'] if c in df_venta.columns])
        print(f"  Search Text sample: {search_text[:200]}...")
    else:
        print("  158k property not found in this run.")

if __name__ == "__main__":
    verify_full()
