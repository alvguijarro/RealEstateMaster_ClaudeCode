import os
import sys
import pandas as pd
import numpy as np

# Add repo to path
repo_path = r"c:\Users\alvgu\.gemini\antigravity\playground\RealEstateMaster"
sys.path.append(repo_path)
sys.path.append(os.path.join(repo_path, "analyzer"))

import analysis

def trace_property():
    venta_file = os.path.join(repo_path, "scraper", "salidas", "idealista_Baix_Segura_venta.xlsx")
    alquiler_file = os.path.join(repo_path, "scraper", "salidas", "idealista_Baix_Segura_alquiler.xlsx")
    
    config = analysis.DEFAULT_CONFIG.copy()
    config['venta_file'] = venta_file
    config['alquiler_file'] = alquiler_file
    # Simulate UI filters (1-5 habs, 1-3 banos, 0-max price)
    config['filters'] = {
        'active': True,
        'habs': [1,2,3,4,5],
        'banos': [1,2,3],
        'price_min': 0,
        'price_max': 999999,
        'include_especial': [],
        'estado': ['Obra nueva', 'Segunda mano/buen estado', 'A reformar'],
        'ascensor': ['Si', 'No'],
        'garaje': ['Si', 'No'],
        'terraza': ['Si', 'No'],
        'altura': ['Bajos', 'Intermedios', 'Aticos'],
        'tipo': ['Pisos', 'Casas/Chalets']
    }

    # Target URL for the 159k property
    target_url = "https://www.idealista.com/inmueble/110577443/"

    print("--- PHASE 1: LOAD ---")
    df_venta, df_alquiler = analysis.phase_load(config, use_cache=False)
    print(f"Loaded: Venta={len(df_venta)}, Target in load: {target_url in df_venta['URL'].values}")

    print("\n--- PHASE 2: CLEAN ---")
    df_venta, df_alquiler, log = analysis.phase_clean(config, df_venta, df_alquiler, use_cache=False)
    print(f"After Clean: Venta={len(df_venta)}, Target survived: {target_url in df_venta['URL'].values}")
    
    if target_url not in df_venta['URL'].values:
        print("Target dropped in CLEAN. Checking why...")
        # Check original row
        xl = pd.ExcelFile(venta_file)
        df_orig = pd.concat([pd.read_excel(xl, sheet_name=s) for s in xl.sheet_names])
        row = df_orig[df_orig['URL'] == target_url].iloc[0]
        print(f"Row data: {row[['Titulo', 'price', 'habs', 'banos', 'Distrito', 'tipo']]}")
        return

    print("\n--- PHASE 3: MARKET ---")
    df_venta, zona_stats = analysis.phase_market(config, df_venta, df_alquiler, use_cache=False)
    print(f"After Market: Venta={len(df_venta)}, Target survived: {target_url in df_venta['URL'].values}")

    print("\n--- PHASE 4: YIELDS ---")
    df_venta = analysis.phase_yields(config, df_venta, df_alquiler, zona_stats, use_cache=False)
    target_row = df_venta[df_venta['URL'] == target_url].iloc[0]
    print(f"Yield Results for Target:")
    print(f"  Rent Estimada: {target_row.get('renta_estimada')}")
    print(f"  Yield Bruta: {target_row.get('yield_bruta')}")
    print(f"  Yield Neta: {target_row.get('yield_neta')}")

    print("\n--- PHASE 5: SCORE ---")
    df_venta, zona_stats = analysis.phase_score(config, df_venta, zona_stats, use_cache=False)
    target_row = df_venta[df_venta['URL'] == target_url].iloc[0]
    print(f"Score: {target_row.get('score')}, Oportunidad: {target_row.get('oportunidad')}")

    print("\n--- PHASE 6: EXPORT (Filtering Check) ---")
    top100 = df_venta[
        (df_venta['renta_estimada'] > 0) & 
        (df_venta['yield_bruta'] > 0)
    ].sort_values('yield_bruta', ascending=False)
    print(f"Total available for Top 100: {len(top100)}")
    print(f"Target Rank in Yields: {list(top100['URL']).index(target_url) + 1 if target_url in top100['URL'].values else 'N/A'}")

if __name__ == "__main__":
    trace_property()
