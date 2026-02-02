"""
=============================================================================
ANALISIS DE INVERSION INMOBILIARIA - Pipeline Modular con Checkpoints
=============================================================================
Self-contained, portable analysis pipeline.

QUICK START:
    1. Place this file in a folder with your Excel files
    2. Run: pip install -r requirements.txt
    3. Run: python analysis.py

USAGE:
    python analysis.py                          # Auto-detect input files
    python analysis.py --venta X.xlsx --alquiler Y.xlsx
    python analysis.py --resume-from market     # Resume from a phase
    python analysis.py --force                  # Ignore cache, full rerun
=============================================================================
"""

import sys
import io
import os
import pickle
import argparse
import glob
import re
import math
from pathlib import Path


# Fix Windows console encoding
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# =============================================================================
# DEPENDENCY CHECK
# =============================================================================
def check_dependencies():
    """Check if required packages are installed."""
    missing = []
    
    try:
        import pandas
    except ImportError:
        missing.append('pandas')
    
    try:
        import numpy
    except ImportError:
        missing.append('numpy')
    
    try:
        import openpyxl
    except ImportError:
        missing.append('openpyxl')
    
    # sklearn is optional
    try:
        import sklearn
        sklearn_ok = True
    except ImportError:
        sklearn_ok = False
    
    if missing:
        print("=" * 60)
        print("ERROR: Missing required dependencies")
        print("=" * 60)
        print(f"  Missing: {', '.join(missing)}")
        print()
        print("  Install with:")
        print("    pip install -r requirements.txt")
        print()
        print("  Or manually:")
        print(f"    pip install {' '.join(missing)}")
        print("=" * 60)
        sys.exit(1)
    
    return sklearn_ok

SKLEARN_AVAILABLE = check_dependencies()

import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

# =============================================================================
# DATABASE INTEGRATION
# =============================================================================
try:
    # Add scraper directory to path to import database_manager
    scraper_path = os.path.join(os.path.dirname(__file__), '..', 'scraper')
    if scraper_path not in sys.path:
        sys.path.append(scraper_path)
    from database_manager import DatabaseManager
    DB_AVAILABLE = True
    print("  [DB] Database Manager loaded successfully")
except Exception as e:
    DB_AVAILABLE = False
    print(f"  [DB] Database Manager not available: {e}")

# =============================================================================
# ML RENT MODEL IMPORT (with fallback)
# =============================================================================
try:
    from ml_rent_model import (
        train_rent_model, 
        predict_rent, 
        calculate_precision_score,
        prepare_features
    )
    ML_RENT_AVAILABLE = True
    print("  [ML] ML Rent Model module loaded successfully")
except ImportError as e:
    ML_RENT_AVAILABLE = False
    print(f"  [ML] ML Rent Model not available: {e}")
    print("  [ML] Falling back to percentile-based estimation")

# Global variable to store trained ML model (cached during session)
ML_RENT_MODEL = None

# =============================================================================
# AUTO-DETECT INPUT FILES
# =============================================================================
def find_input_files():
    """Auto-detect VENTA and ALQUILER Excel files in current directory."""
    xlsx_files = glob.glob("*.xlsx")
    
    venta_file = None
    alquiler_file = None
    
    # Look for files with keywords
    for f in xlsx_files:
        f_lower = f.lower()
        if 'venta' in f_lower or 'sale' in f_lower:
            venta_file = f
        elif 'alquiler' in f_lower or 'rent' in f_lower:
            alquiler_file = f
    
    return venta_file, alquiler_file


# =============================================================================
# CONFIGURATION & CONSTANTS
# =============================================================================
DEFAULT_CONFIG = {
    'venta_file': None,    # Auto-detected
    'alquiler_file': None, # Auto-detected
    'output_file': 'analisis_resultado.xlsx',
    'cache_dir': '.cache',
    
    # Financial Assumptions
    'gastos_recurrentes': 0.15, # 15% of gross rent
    'vacancia': 0.05,           # 5% vacancy/maintenance
    'costes_compra': 0.10,      # 10% purchase costs (ITP, Notary)
    
    # Analysis Thresholds
    'min_propiedades_zona': 5,  # Min props to analyse a zone
    'tolerancia_m2': 0.20,      # +/- 20% m2 for comparables
    'umbral_residual': -0.10,   # -10% residual to be considered opportunity
    
    # Scoring Weights (0-100)
    'pesos_score': {
        'w1_descuento': 0.40,   # 40% Weight for price discount
        'w2_yield': 0.30,       # 30% Weight for gross yield
        'w3_rebajado': 0.10,    # 10% Bonus for price drop
        'w4_zscore': 0.10,      # 10% Statistical significance
        'w5_comps': 0.10        # 10% Confidence (number of comps)
    },
    
    # LLM Config
    'google_api_key': os.getenv('GOOGLE_API_KEY'),
    'llm_model': 'gemini-1.5-flash-latest'
}

PHASES = ['load', 'clean', 'market', 'yields', 'score', 'export']

# =============================================================================
# INTERACTIVE FILTERS
# =============================================================================
def get_user_filters():
    """
    Prompt user for interactive filters.
    Returns a dictionary of filters.
    """
    filters = {
        'active': False,
        'estado': [],
        'include_especial': [],
        'ascensor': [],
        'garaje': [],
        'terraza': [],
        'altura': [],
        'tipo': []
    }
    
    print("\n" + "="*60)
    print("CONFIGURACION DE FILTROS (Pulse Enter para seleccionar TODAS)")
    print("="*60)
    
    def ask_options(title, options, key_name):
        print(f"\n{title}:")
        for k, v in options.items():
            print(f"  {k}. {v}")
        
        sel = input(f"  > Seleccion (ej: 1,3) [Todas]: ").strip()
        
        selected_values = []
        if not sel:
            # Default: All options
            selected_values = list(options.values())
        else:
            try:
                # Parse "1, 2" -> [1, 2]
                keys = [int(x.strip()) for x in sel.split(',') if x.strip().isdigit()]
                for k in keys:
                    if k in options:
                        selected_values.append(options[k])
            except:
                print("  [!] Seleccion invalida, se usaran TODAS.")
                selected_values = list(options.values())
        
        # Store in filters
        filters[key_name] = selected_values
        
        # Print confirmation
        display = ", ".join(selected_values) if selected_values else "Todas"
        print(f"  -> Seleccionado: {display}")
        return selected_values

    # 1. ESTADO
    opts_estado = {1: 'Obra nueva', 2: 'Segunda mano/buen estado', 3: 'A reformar'}
    ask_options("1. ESTADO", opts_estado, 'estado')
    
    # 2. INCLUIR ESPECIALES
    # Special logic: Default is None (exclude all). Selecting means INCLUDE.
    print(f"\n2. INCLUIR VIVIENDAS ESPECIALES (Por defecto se EXCLUYEN):")
    opts_esp = {1: 'Okupas/Ilegal', 2: 'Nuda Propiedad', 3: 'Copropiedad', 4: 'Con Inquilino', 5: 'Cesión Remate'}
    for k, v in opts_esp.items():
        print(f"  {k}. {v}")
    sel = input(f"  > Seleccion (ej: 1) [Ninguna]: ").strip()
    selected_esp = []
    if sel:
        try:
            keys = [int(x.strip()) for x in sel.split(',') if x.strip().isdigit()]
            for k in keys:
                if k in opts_esp:
                    selected_esp.append(opts_esp[k])
        except:
            pass
    filters['include_especial'] = selected_esp
    display = ", ".join(selected_esp) if selected_esp else "Ninguna (Excluir todas)"
    print(f"  -> Seleccionado: {display}")
    
    # 3. ASCENSOR
    opts_bool = {1: 'Si', 2: 'No'}
    ask_options("3. ASCENSOR", opts_bool, 'ascensor')
    
    # 4. GARAJE
    ask_options("4. GARAJE", opts_bool, 'garaje')
    
    # 5. TERRAZA
    ask_options("5. TERRAZA", opts_bool, 'terraza')
    
    # 6. ALTURA
    opts_altura = {1: 'Bajos', 2: 'Intermedios', 3: 'Aticos'}
    ask_options("6. ALTURA", opts_altura, 'altura')
    
    # 7. TIPO
    opts_tipo = {1: 'Pisos', 2: 'Casas/Chalets'}
    ask_options("7. TIPO", opts_tipo, 'tipo')
    
    filters['active'] = True
    print("\n" + "-"*60)
    return filters


# =============================================================================
# CHECKPOINT UTILITIES
# =============================================================================
def get_cache_path(config, phase_name):
    """Get the cache file path for a phase."""
    cache_dir = Path(config['cache_dir'])
    cache_dir.mkdir(exist_ok=True)
    return cache_dir / f"checkpoint_{phase_name}.pkl"


def save_checkpoint(config, phase_name, data):
    """Save phase output to cache file."""
    cache_path = get_cache_path(config, phase_name)
    with open(cache_path, 'wb') as f:
        pickle.dump(data, f)
    print(f"    [CACHE] Saved checkpoint: {cache_path}")


def load_checkpoint(config, phase_name):
    """Load phase output from cache file if exists."""
    cache_path = get_cache_path(config, phase_name)
    if cache_path.exists():
        with open(cache_path, 'rb') as f:
            data = pickle.load(f)
        print(f"    [CACHE] Loaded checkpoint: {cache_path}")
        return data
    return None


def clear_cache(config):
    """Clear all checkpoint files."""
    cache_dir = Path(config['cache_dir'])
    if cache_dir.exists():
        for f in cache_dir.glob("checkpoint_*.pkl"):
            f.unlink()
        print(f"    [CACHE] Cleared all checkpoints")


# =============================================================================
# PHASE 1: LOAD DATA
# =============================================================================
def phase_load(config, use_cache=True):
    """
    Load data from Excel files.
    
    Returns: (df_venta, df_alquiler)
    """
    print("\n" + "=" * 60)
    print("PHASE 1: LOAD DATA")
    print("=" * 60)
    
    # Check cache
    if use_cache:
        cached = load_checkpoint(config, 'load')
        if cached:
            return cached
    
    # Load VENTA
    print(f"  Loading VENTA from: {config['venta_file']}")
    xl_venta = pd.ExcelFile(config['venta_file'])
    df_venta = pd.concat(
        [pd.read_excel(xl_venta, sheet_name=s).assign(_source=s) for s in xl_venta.sheet_names],
        ignore_index=True
    )
    print(f"    -> {len(df_venta)} rows from {len(xl_venta.sheet_names)} sheets")
    
    # Load ALQUILER
    print(f"  Loading ALQUILER from: {config['alquiler_file']}")
    xl_alquiler = pd.ExcelFile(config['alquiler_file'])
    df_alquiler = pd.concat(
        [pd.read_excel(xl_alquiler, sheet_name=s).assign(_source=s) for s in xl_alquiler.sheet_names],
        ignore_index=True
    )
    print(f"    -> {len(df_alquiler)} rows from {len(xl_alquiler.sheet_names)} sheets")
    
    # --- ENRICH WITH HISTORICAL DATA ---
    if DB_AVAILABLE:
        try:
            print("\n  [BQ] Checking for historical data in BigQuery...")
            
            # Initialize BigQuery connection using service account
            scraper_path_local = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'scraper')
            key_file = os.path.join(scraper_path_local, 'service-account.json')
            
            if os.path.exists(key_file):
                from google.oauth2 import service_account
                import pandas_gbq
                import json
                
                with open(key_file, 'r') as f:
                    creds_data = json.load(f)
                    project_id = creds_data.get('project_id')
                    
                credentials = service_account.Credentials.from_service_account_file(key_file)
                
                # 1. Identify Target Province
                provinces = []
                if 'Provincia' in df_venta.columns:
                    provinces.extend(df_venta['Provincia'].dropna().unique())
                
                # Initialize empty by default in case no province found
                df_hist_alq = pd.DataFrame() 
                
                if provinces:
                    target_prov = pd.Series(provinces).mode()[0]
                    print(f"    -> Target Province detected: {target_prov}")
                    
                    # 2. Fetch Historical ALQUILER Data from BigQuery
                    query = f"""
                        SELECT *
                        FROM `{project_id}.real_estate.oportunidades`
                        WHERE LOWER(provincia) LIKE '%{target_prov.lower()}%'
                        AND LOWER(source_file) LIKE '%alquiler%'
                    """
                    
                    print(f"    -> Executing BQ Query: SELECT * ... WHERE province~'{target_prov}' AND type~'alquiler'")
                    
                    df_hist_alq = pandas_gbq.read_gbq(
                        query,
                        project_id=project_id,
                        credentials=credentials
                    )
            else:
                print("    [WARN] Service account key not found. Skipping BigQuery.")
                df_hist_alq = pd.DataFrame()
            
            # --- MERGE LOGIC (Fixed Indentation) ---
            if not df_hist_alq.empty:
                print(f"    -> Found {len(df_hist_alq)} historical rental records in BigQuery")
                
                # 3. Normalize Columns (BQ snake_case -> Analyzer Format)
                # Simple mapping for critical columns
                rename_map = {
                    'titulo': 'Titulo',
                    'price': 'price',
                    'm2_construidos': 'm2 construidos',
                    'm2_utiles': 'm2 utiles',
                    'm2_construidos': 'm2 construidos', # Duplicate key in dict, python allows but overwrites.
                    'm2_utiles': 'm2 utiles',
                    'num_plantas': 'Num plantas',
                    'habs': 'habs',
                    'banos': 'banos',
                    'ascensor': 'ascensor',
                    'garaje': 'Garaje',
                    'terraza': 'Terraza',
                    'trastero': 'Trastero',
                    'provincia': 'Provincia',
                    'ciudad': 'Ciudad',
                    'distrito': 'Distrito',
                    'barrio': 'Barrio',
                    'url': 'URL',
                    'latitud': 'Latitud',
                    'longitud': 'Longitud'
                }
                df_hist_alq = df_hist_alq.rename(columns=rename_map)
                
                # 3. Merge with current df_alquiler
                
                combined_alq = pd.concat([df_alquiler, df_hist_alq], ignore_index=True)
                
                # Deduplicate by URL (keep latest from current file if conflict? or keep latest date?)
                # If 'URL' column exists
                if 'URL' in combined_alq.columns:
                    before_dedup = len(combined_alq)
                    combined_alq = combined_alq.drop_duplicates(subset=['URL'], keep='last')
                    print(f"    -> Merged & Deduplicated: {len(df_alquiler)} + {len(df_hist_alq)} => {len(combined_alq)} rows")
                
                df_alquiler = combined_alq
                    
            else:
                print("    -> No historical rental data found in BigQuery for this province.")
                
        except Exception as e:
            print(f"    [WARN] Database enrichment failed: {e}")
            
    result = (df_venta, df_alquiler)
    save_checkpoint(config, 'load', result)
    return result


# =============================================================================
# PHASE 2: CLEAN DATA
# =============================================================================
def phase_clean(config, df_venta, df_alquiler, use_cache=True):
    """
    Clean and prepare data.
    
    Returns: (df_venta_clean, df_alquiler_clean, log_calidad)
    """
    print("\n" + "=" * 60)
    print("PHASE 2: CLEAN DATA")
    print("=" * 60)
    
    if use_cache:
        cached = load_checkpoint(config, 'clean')
        if cached:
            return cached
    
    log_calidad = []
    log_calidad.append({'phase': 'load', 'dataset': 'VENTA', 'rows': len(df_venta), 'note': 'initial'})
    log_calidad.append({'phase': 'load', 'dataset': 'ALQUILER', 'rows': len(df_alquiler), 'note': 'initial'})
    
    # Detect Room Mode
    is_room_mode = 'habitacion_m2' in df_alquiler.columns
    if is_room_mode:
        config['is_room_mode'] = True
        print("  [INFO] ROOM MODE DETECTED: Analyzing Room Rentals")
    
    # Helper functions
    def clean_numeric(s):
        if s.dtype == 'object':
            s = s.astype(str).str.replace('.', '', regex=False)
            s = s.str.replace(',', '.', regex=False).str.replace(' ', '', regex=False)
            s = s.replace(['nan', 'None', ''], np.nan)
        return pd.to_numeric(s, errors='coerce')
    
    def clean_boolean(s):
        if s.dtype == 'object':
            return s.isin(['Si', 'si', 'SI', 'Sí', 'sí', 'True', 'true', '1'])
        return s.fillna(False).astype(bool)
    
    # Clean numeric columns
    print("  Cleaning numeric columns...")
    num_cols = ['price', 'm2 construidos', 'habs', 'banos', 'construido en']
    for col in num_cols:
        if col in df_venta.columns:
            df_venta[col] = clean_numeric(df_venta[col])
        if col in df_alquiler.columns:
            df_alquiler[col] = clean_numeric(df_alquiler[col])
            
    if is_room_mode:
        for col in ['habitacion_m2', 'piso_m2', 'num_habitaciones_total']:
             if col in df_alquiler.columns:
                 df_alquiler[col] = clean_numeric(df_alquiler[col])
    
    # Clean boolean columns
    print("  Cleaning boolean columns...")
    bool_cols = ['Terraza', 'Garaje', 'ascensor', 'piscina', 'nuda propiedad', 'okupado']
    for col in bool_cols:
        if col in df_venta.columns:
            df_venta[col] = clean_boolean(df_venta[col])
        if col in df_alquiler.columns:
            df_alquiler[col] = clean_boolean(df_alquiler[col])
    
    # Exclusions - ENHANCED: Check both boolean columns AND text fields
    print("  Applying exclusions...")
    
    # Create text columns for searching
    desc_col = None
    titulo_col = None
    for col in df_venta.columns:
        if col.lower() in ['descripción', 'descripcion', 'description']:
            desc_col = col
        if col.lower() in ['titulo', 'title', 'nombre']:
            titulo_col = col
    
    # Combine text fields for searching
    df_venta['_search_text'] = ''
    if desc_col:
        df_venta['_search_text'] += df_venta[desc_col].fillna('').astype(str).str.lower()
    if titulo_col:
        df_venta['_search_text'] += ' ' + df_venta[titulo_col].fillna('').astype(str).str.lower()
    
    # Check if filters allow specials
    filters = config.get('filters', {})
    include_nuda = 'Nuda Propiedad' in filters.get('include_especial', [])
    include_okupa = 'Okupas/Ilegal' in filters.get('include_especial', [])
    include_copropiedad = 'Copropiedad' in filters.get('include_especial', [])
    
    # NUDA PROPIEDAD exclusion
    if not include_nuda:
        # Check boolean column
        nuda_bool = pd.Series(False, index=df_venta.index)
        if 'nuda propiedad' in df_venta.columns:
            nuda_bool = df_venta['nuda propiedad'].fillna(False).astype(bool)
        
        # Check text fields for "nuda propiedad" or "nuda-propiedad"
        nuda_text = df_venta['_search_text'].str.contains('nuda propiedad|nuda-propiedad|nudapropiedad', regex=True, na=False)
        
        # Combine both conditions
        nuda_mask = nuda_bool | nuda_text
        n_nuda = nuda_mask.sum()
        df_venta = df_venta[~nuda_mask].copy()
        log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_nuda, 'note': 'excluded nuda propiedad (bool + text)'})
        print(f"    -> Excluded {n_nuda} nuda propiedad")
    else:
        print("    -> [FILTER] Including Nuda Propiedad")
    
    # Recalculate search text after filtering
    df_venta['_search_text'] = ''
    if desc_col:
        df_venta['_search_text'] += df_venta[desc_col].fillna('').astype(str).str.lower()
    if titulo_col:
        df_venta['_search_text'] += ' ' + df_venta[titulo_col].fillna('').astype(str).str.lower()
    
    # OKUPADO exclusion
    if not include_okupa:
        # Check boolean column
        okup_bool = pd.Series(False, index=df_venta.index)
        if 'okupado' in df_venta.columns:
            okup_bool = df_venta['okupado'].fillna(False).astype(bool)
        
        # Check text fields for "okupado", "ocupado ilegalmente", "ocupado por terceros", etc.
        okup_text = df_venta['_search_text'].str.contains(
            'okupad|ocupado ilegalmente|ocupado por terceros|ocupacion ilegal|inmueble ocupado|vivienda ocupada|piso ocupado', 
            regex=True, na=False
        )
        
        # Combine both conditions
        okup_mask = okup_bool | okup_text
        n_okup = okup_mask.sum()
        df_venta = df_venta[~okup_mask].copy()
        log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_okup, 'note': 'excluded okupado (bool + text)'})
        print(f"    -> Excluded {n_okup} okupado/ocupado ilegalmente")
    else:
        print("    -> [FILTER] Including Okupas")
    
    # Recalculate search text after filtering
    df_venta['_search_text'] = ''
    if desc_col:
        df_venta['_search_text'] += df_venta[desc_col].fillna('').astype(str).str.lower()
    if titulo_col:
        df_venta['_search_text'] += ' ' + df_venta[titulo_col].fillna('').astype(str).str.lower()
    
    # COPROPIEDAD exclusion
    if not include_copropiedad:
        # Check boolean/string column
        coprop_bool = pd.Series(False, index=df_venta.index)
        if 'Copropiedad' in df_venta.columns:
            coprop_bool = df_venta['Copropiedad'].fillna('').astype(str).str.lower().isin(['sí', 'si', 'yes', 'true', '1'])
        
        # Check text fields for "copropiedad"
        coprop_text = df_venta['_search_text'].str.contains('copropiedad', regex=False, na=False)
        
        # Combine both conditions
        coprop_mask = coprop_bool | coprop_text
        n_coprop = coprop_mask.sum()
        df_venta = df_venta[~coprop_mask].copy()
        log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_coprop, 'note': 'excluded copropiedad (bool + text)'})
        print(f"    -> Excluded {n_coprop} copropiedad")
    else:
        print("    -> [FILTER] Including Copropiedad")
    
    # Recalculate search text after filtering
    df_venta['_search_text'] = ''
    if desc_col:
        df_venta['_search_text'] += df_venta[desc_col].fillna('').astype(str).str.lower()
    if titulo_col:
        df_venta['_search_text'] += ' ' + df_venta[titulo_col].fillna('').astype(str).str.lower()
    
    # CON INQUILINO exclusion (properties with existing tenants)
    include_inquilino = 'Con Inquilino' in filters.get('include_especial', [])
    if not include_inquilino:
        # Check string column for "Sí" values
        inquilino_col = pd.Series(False, index=df_venta.index)
        if 'con inquilino' in df_venta.columns:
            inquilino_col = df_venta['con inquilino'].fillna('').astype(str).str.lower().isin(['sí', 'si', 'yes', 'true', '1'])
        
        # Check text fields for "con inquilino" or "inquilino"
        inquilino_text = df_venta['_search_text'].str.contains('con inquilino|alquilado|arrendado|con arrendatario', regex=True, na=False)
        
        # Combine both conditions
        inquilino_mask = inquilino_col | inquilino_text
        n_inquilino = inquilino_mask.sum()
        df_venta = df_venta[~inquilino_mask].copy()
        log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_inquilino, 'note': 'excluded con inquilino (col + text)'})
        print(f"    -> Excluded {n_inquilino} con inquilino")
    else:
        print("    -> [FILTER] Including Con Inquilino")
    
    # CESIÓN REMATE exclusion (assignment of auction)
    include_cesion = 'Cesión Remate' in filters.get('include_especial', [])
    if not include_cesion:
        # Check 'ces. remate' column for "Sí" values
        cesion_mask = pd.Series(False, index=df_venta.index)
        if 'ces. remate' in df_venta.columns:
            cesion_mask = df_venta['ces. remate'].fillna('').astype(str).str.lower().isin(['sí', 'si', 'yes', 'true', '1'])
        
        n_cesion = cesion_mask.sum()
        df_venta = df_venta[~cesion_mask].copy()
        log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_cesion, 'note': 'excluded cesión remate'})
        print(f"    -> Excluded {n_cesion} cesión de remate")
    else:
        print("    -> [FILTER] Including Cesión Remate")
    
    # Clean up temporary column
    df_venta = df_venta.drop(columns=['_search_text'])

    # =========================================================================
    # APPLY USER FILTERS (ESTADO, FEATURE, TYPE, FLOOR)
    # =========================================================================
    if filters.get('active'):
        print("  Applying interactive filters...")
        initial_rows = len(df_venta)
        
        # 1. ESTADO
        # Map dataset values to our categories
        # 'Segunda mano/buen estado' -> 2
        # 'Para reformar' -> 3
        # 'Obra nueva' -> 1
        allowed_estados = filters.get('estado', [])
        if allowed_estados and len(allowed_estados) < 3: # If not all selected
            # logic to classify
            def classify_estado(val):
                val = str(val).lower()
                if 'reformar' in val: return 'A reformar'
                if 'nueva' in val or 'nuevo' in val: return 'Obra nueva'
                return 'Segunda mano/buen estado'
            
            mask_estado = df_venta['estado'].apply(classify_estado).isin(allowed_estados)
            df_venta = df_venta[mask_estado]
        
        # 3. ASCENSOR
        asc_sel = filters.get('ascensor', [])
        if 'Si' in asc_sel and 'No' not in asc_sel:
            # Require elevator
            if 'ascensor' in df_venta.columns:
                df_venta = df_venta[df_venta['ascensor'] == True]
        elif 'No' in asc_sel and 'Si' not in asc_sel:
            # Require NO elevator
            if 'ascensor' in df_venta.columns:
                df_venta = df_venta[df_venta['ascensor'] == False]
        
        # 4. GARAJE
        gar_sel = filters.get('garaje', [])
        if 'Si' in gar_sel and 'No' not in gar_sel:
            if 'Garaje' in df_venta.columns:
                df_venta = df_venta[df_venta['Garaje'] == True]
        elif 'No' in gar_sel and 'Si' not in gar_sel:
            if 'Garaje' in df_venta.columns:
                df_venta = df_venta[df_venta['Garaje'] == False]
                
        # 5. TERRAZA
        ter_sel = filters.get('terraza', [])
        if 'Si' in ter_sel and 'No' not in ter_sel:
            if 'Terraza' in df_venta.columns:
                df_venta = df_venta[df_venta['Terraza'] == True]
        elif 'No' in ter_sel and 'Si' not in ter_sel:
            if 'Terraza' in df_venta.columns:
                df_venta = df_venta[df_venta['Terraza'] == False]
        
        # 6. ALTURA
        alt_sel = filters.get('altura', [])
        if alt_sel and len(alt_sel) < 3:
            def classify_altura(row):
                alt = str(row.get('altura', '')).lower()
                tipo = str(row.get('tipo', '')).lower()
                
                # Aticos
                if 'ático' in alt or 'atico' in alt or 'ático' in tipo or 'atico' in tipo:
                    return 'Aticos'
                
                # Bajos
                if 'bajo' in alt or 'entresuelo' in alt:
                    return 'Bajos'
                
                # Rest is Intermedios (Simplification)
                # Note: 'chalet' often has altura 'nan' or 'chalet'. 
                # If it is a chalet, it might be classified as 'Bajos' or 'Intermedios' depending on interpretation.
                # Assuming 'Intermedios' for standard flats not low or attic.
                return 'Intermedios'
            
            mask_altura = df_venta.apply(classify_altura, axis=1).isin(alt_sel)
            df_venta = df_venta[mask_altura]

        # 7. TIPO (Piso vs Casa)
        tipo_sel = filters.get('tipo', [])
        if tipo_sel and len(tipo_sel) < 2:
            def classify_tipo(val):
                val = str(val).lower()
                if val in ['casa', 'chalet', 'unifamiliar', 'independiente', 'pareado']:
                    return 'Casas/Chalets'
                return 'Pisos' # Default includes piso, atico, duplex, estudio
            
            mask_tipo = df_venta['tipo'].apply(classify_tipo).isin(tipo_sel)
            df_venta = df_venta[mask_tipo]

        filtered_count = initial_rows - len(df_venta)
        if filtered_count > 0:
            print(f"    -> Filtered out {filtered_count} rows based on user criteria")
            log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': filtered_count, 'note': 'interactive filters (basic)'})

        # =========================================================================
        # NEW FILTERS: Price, Habs, Banos
        # =========================================================================
        
        # 8. PRICE RANGE
        p_min = filters.get('price_min')
        p_max = filters.get('price_max')
        if p_min or p_max:
            try:
                p_min = float(p_min) if p_min else 0
                p_max = float(p_max) if p_max else float('inf')
                
                # Filter VENTA by price (ALQUILER usually checked by rental price, but here filter implies purchase price opportunity)
                # Usually we only filter VENTA for opportunities.
                df_venta = df_venta[(df_venta['price'] >= p_min) & (df_venta['price'] <= p_max)]
            except ValueError:
                pass

        # 9. HABS
        habs_sel = filters.get('habs', [])
        if habs_sel:
            # habs_sel is list of strings/ints e.g. [1, 2, '5']
            # Logic: Exact match for 1-4. '5' usually means 5+ in UI logic 'data-value="5"'.
            # We need to handle this carefully.
            # Convert to ints
            sel_ints = []
            has_plus = False
            plus_val = 5
            
            for h in habs_sel:
                try:
                    h_int = int(h)
                    if h_int >= 5: # Assuming 5 means 5+ as per UI
                        has_plus = True
                        plus_val = 5
                    sel_ints.append(h_int)
                except: pass
            
            if has_plus:
                # Logic: (habs in sel_ints) OR (habs >= 5)
                # Since sel_ints includes 5, isin check works for exactly 5.
                # We need explicit check for >5 if data has 6, 7 etc.
                df_venta = df_venta[df_venta['habs'].isin(sel_ints) | (df_venta['habs'] >= plus_val)]
            else:
                df_venta = df_venta[df_venta['habs'].isin(sel_ints)]

        # 10. BANOS
        banos_sel = filters.get('banos', [])
        if banos_sel:
            sel_ints = []
            has_plus = False
            plus_val = 3
            
            for b in banos_sel:
                try:
                    b_int = int(b)
                    if b_int >= 3:
                        has_plus = True
                        plus_val = 3
                    sel_ints.append(b_int)
                except: pass
                
            if has_plus:
                df_venta = df_venta[df_venta['banos'].isin(sel_ints) | (df_venta['banos'] >= plus_val)]
            else:
                df_venta = df_venta[df_venta['banos'].isin(sel_ints)]

    
    # Filter to common districts
    print("  Filtering to common districts...")
    common = set(df_venta['Distrito'].dropna().unique()) & set(df_alquiler['Distrito'].dropna().unique())
    
    n_v = len(df_venta[~df_venta['Distrito'].isin(common)])
    n_a = len(df_alquiler[~df_alquiler['Distrito'].isin(common)])
    
    df_venta = df_venta[df_venta['Distrito'].isin(common)].copy()
    df_alquiler = df_alquiler[df_alquiler['Distrito'].isin(common)].copy()
    
    log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': n_v, 'note': 'excluded non-common distrito'})
    log_calidad.append({'phase': 'clean', 'dataset': 'ALQUILER', 'rows': n_a, 'note': 'excluded non-common distrito'})
    print(f"    -> {len(common)} common districts")
    
    # Drop missing critical values
    print("  Dropping rows with missing critical values...")
    df_venta = df_venta.dropna(subset=['price', 'm2 construidos', 'Distrito'])
    
    if is_room_mode:
        # Room cleaning
        df_alquiler = df_alquiler.dropna(subset=['price', 'Distrito'])
        # Aliasing for compatibility
        if 'habitacion_m2' in df_alquiler.columns:
             df_alquiler['m2'] = df_alquiler['habitacion_m2']
    else:
        df_alquiler = df_alquiler.dropna(subset=['price', 'm2 construidos', 'Distrito'])
    
    # Calculate derived fields
    print("  Calculating derived fields...")
    df_venta['precio_m2'] = df_venta['price'] / df_venta['m2 construidos']
    
    if is_room_mode:
        # Use absolute price for rooms statistics (Average Rent per Room)
        df_alquiler['precio_m2'] = df_alquiler['price']
    else:
        df_alquiler['precio_m2'] = df_alquiler['price'] / df_alquiler['m2 construidos']
    df_venta['rebajado'] = df_venta['old price'].notna()
    
    log_calidad.append({'phase': 'clean', 'dataset': 'VENTA', 'rows': len(df_venta), 'note': 'final'})
    log_calidad.append({'phase': 'clean', 'dataset': 'ALQUILER', 'rows': len(df_alquiler), 'note': 'final'})
    
    print(f"  RESULT: VENTA={len(df_venta)}, ALQUILER={len(df_alquiler)}")
    
    result = (df_venta, df_alquiler, log_calidad)
    save_checkpoint(config, 'clean', result)
    return result


# =============================================================================
# PHASE 3: MARKET ANALYSIS
# =============================================================================
def phase_market(config, df_venta, df_alquiler, use_cache=True):
    """
    Analyze market prices using percentile and ML approaches.
    
    Returns: (df_venta_analyzed, zona_stats)
    """
    print("\n" + "=" * 60)
    print("PHASE 3: MARKET ANALYSIS")
    print("=" * 60)
    
    if use_cache:
        cached = load_checkpoint(config, 'market')
        if cached:
            return cached
    
    # Zone stats - VENTA
    print("  Calculating VENTA zone statistics...")
    zona_venta = df_venta.groupby('Distrito')['precio_m2'].agg([
        ('mediana_venta_m2', 'median'),
        ('p25_venta_m2', lambda x: x.quantile(0.25)),
        ('p75_venta_m2', lambda x: x.quantile(0.75)),
        ('iqr_venta_m2', lambda x: x.quantile(0.75) - x.quantile(0.25)),
        ('mean_venta_m2', 'mean'),
        ('std_venta_m2', 'std'),
        ('n_venta', 'count')
    ]).reset_index()
    
    # Zone stats - ALQUILER
    print("  Calculating ALQUILER zone statistics...")
    zona_alquiler = df_alquiler.groupby('Distrito').agg({
        'precio_m2': ['median', lambda x: x.quantile(0.25), lambda x: x.quantile(0.75), 'count'],
        'price': 'median'
    }).reset_index()
    zona_alquiler.columns = ['Distrito', 'mediana_alquiler_m2', 'p25_alquiler_m2', 'p75_alquiler_m2', 'n_alquiler', 'mediana_alquiler']
    
    zona_stats = pd.merge(zona_venta, zona_alquiler, on='Distrito', how='inner')
    
    # Merge to properties
    print("  Merging zone stats to properties...")
    df_venta = df_venta.merge(
        zona_stats[['Distrito', 'mediana_venta_m2', 'p25_venta_m2', 'iqr_venta_m2', 
                    'mean_venta_m2', 'std_venta_m2', 'mediana_alquiler_m2', 'n_alquiler']],
        on='Distrito', how='left'
    )
    
    # Percentile approach
    print("  Applying percentile criteria...")
    df_venta['below_p25'] = df_venta['precio_m2'] < df_venta['p25_venta_m2']
    df_venta['below_iqr'] = df_venta['precio_m2'] < (df_venta['mediana_venta_m2'] - df_venta['iqr_venta_m2'])
    df_venta['below_market_percentile'] = df_venta['below_p25'] | df_venta['below_iqr']
    df_venta['descuento_vs_mercado_pct'] = (1 - df_venta['precio_m2'] / df_venta['mediana_venta_m2']) * 100
    df_venta['z_score'] = (df_venta['precio_m2'] - df_venta['mean_venta_m2']) / df_venta['std_venta_m2'].replace(0, np.nan)
    
    n_perc = df_venta['below_market_percentile'].sum()
    print(f"    -> {n_perc} below market (percentile)")
    
    # ML approach
    print("  Applying ML model approach...")
    try:
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.preprocessing import LabelEncoder
        
        df_model = df_venta.copy()
        le_tipo = LabelEncoder()
        df_model['tipo_encoded'] = le_tipo.fit_transform(df_model['tipo'].fillna('Unknown').astype(str))
        le_distrito = LabelEncoder()
        df_model['distrito_encoded'] = le_distrito.fit_transform(df_model['Distrito'].astype(str))
        le_estado = LabelEncoder()
        df_model['estado_encoded'] = le_estado.fit_transform(df_model['estado'].fillna('Unknown').astype(str))
        df_model['habs'] = df_model['habs'].fillna(df_model['habs'].median())
        df_model['banos'] = df_model['banos'].fillna(df_model['banos'].median())
        df_model['ascensor'] = df_model['ascensor'].fillna(False).astype(int)
        
        X = df_model[['m2 construidos', 'habs', 'banos', 'ascensor', 'tipo_encoded', 'distrito_encoded', 'estado_encoded']].values
        y = df_model['precio_m2'].values
        
        rf = RandomForestRegressor(n_estimators=100, max_depth=10, random_state=42, n_jobs=-1)
        rf.fit(X, y)
        
        df_model['precio_m2_pred'] = rf.predict(X)
        df_model['residual'] = (df_model['precio_m2'] - df_model['precio_m2_pred']) / df_model['precio_m2_pred']
        df_model['below_market_model'] = df_model['residual'] <= config['umbral_residual']
        
        df_venta['precio_m2_pred'] = df_model['precio_m2_pred']
        df_venta['residual'] = df_model['residual']
        df_venta['below_market_model'] = df_model['below_market_model']
        
        n_model = df_venta['below_market_model'].sum()
        print(f"    -> {n_model} below market (model)")
        
    except ImportError:
        print("    [WARN] sklearn not available, skipping ML model")
        df_venta['below_market_model'] = False
    
    # Combined
    df_venta['below_market_combined'] = df_venta['below_market_percentile'] | df_venta.get('below_market_model', False)
    n_combined = df_venta['below_market_combined'].sum()
    print(f"  RESULT: {n_combined} total opportunities")
    
    result = (df_venta, zona_stats)
    save_checkpoint(config, 'market', result)
    return result


# =============================================================================
# FIND COMPARABLE ALQUILER PROPERTIES (ENHANCED with Precision)
# =============================================================================
def find_comparables(venta_row, df_alquiler, strict=True, alquiler_index=None):
    """
    Find ALQUILER properties comparable to a VENTA property.
    
    Enhanced version with:
    - Precision scoring per comparable
    - Extended matching criteria (garaje, terraza, ascensor)
    - Weighted similarity calculation
    - Optional pre-indexed lookup for performance
    
    Strict criteria: Same distrito, same habs, same banos, ±20% m²
    Relaxed criteria: ±1 habs, ±1 banos, ±30% m²
    
    Args:
        venta_row: Row from VENTA DataFrame
        df_alquiler: Full ALQUILER DataFrame (fallback if no index)
        strict: Use strict or relaxed matching criteria
        alquiler_index: Optional dict {distrito: DataFrame} for O(1) lookup
    
    Returns: DataFrame of comparable ALQUILER properties with 'similarity' and 'precision' columns
    """
    distrito = venta_row['Distrito']
    m2 = venta_row['m2 construidos']
    habs = venta_row.get('habs', 2)
    banos = venta_row.get('banos', 1)
    
    # Extended features for precision
    venta_garaje = bool(venta_row.get('Garaje') or venta_row.get('garaje'))
    venta_terraza = bool(venta_row.get('Terraza') or venta_row.get('terraza'))
    venta_ascensor = bool(venta_row.get('ascensor'))
    
    # Handle NaN values
    if pd.isna(habs): habs = 2
    if pd.isna(banos): banos = 1
    if pd.isna(m2): m2 = 80  # Default assumption
    
    # Use pre-indexed lookup if available (O(1) instead of O(n))
    if alquiler_index is not None and distrito in alquiler_index:
        df_distrito = alquiler_index[distrito]
    else:
        # Fallback to full scan
        df_distrito = df_alquiler[df_alquiler['Distrito'] == distrito]
    
    # Return empty if no properties in distrito
    # Return empty if no properties in distrito
    if len(df_distrito) == 0:
        return pd.DataFrame()
    
    # --- TYPE NORMALIZATION ---
    def normalize_tipo(t):
        t = str(t).lower()
        if 'casa' in t or 'chalet' in t or 'unifamiliar' in t or 'pareado' in t:
            return 'casa'
        return 'piso' # Default to piso (includes atico, duplex, estudio)
    
    venta_tipo = normalize_tipo(venta_row.get('tipo', 'piso'))
    
    # Check types in dataset
    if 'tipo_norm' not in df_distrito.columns:
        df_distrito = df_distrito.copy()
        df_distrito['tipo_norm'] = df_distrito['tipo'].apply(normalize_tipo)
    
    if strict:
        # Strict: Same type, same habs, same banos, ±20% m²
        # This is CRITICAL: Do not compare Flats with Houses in strict mode
        mask_tipo = df_distrito['tipo_norm'] == venta_tipo
        
        m2_margin = 0.20
        mask_habs = df_distrito['habs'] == habs
        mask_banos = df_distrito['banos'] == banos
        mask_m2 = (df_distrito['m2 construidos'] >= m2 * (1 - m2_margin)) & \
                  (df_distrito['m2 construidos'] <= m2 * (1 + m2_margin))
        
        mask = mask_tipo & mask_habs & mask_banos & mask_m2
    else:
        # Relaxed: Prefer same type but allow if needed? 
        # Actually, for Rent Estimation, mixing types is very bad. 
        # We will enforce type even in relaxed, but widen other params.
        mask_tipo = df_distrito['tipo_norm'] == venta_tipo
        
        m2_margin = 0.30
        mask_habs = (df_distrito['habs'] >= habs - 1) & (df_distrito['habs'] <= habs + 1)
        mask_banos = (df_distrito['banos'] >= banos - 1) & (df_distrito['banos'] <= banos + 1)
        mask_m2 = (df_distrito['m2 construidos'] >= m2 * (1 - m2_margin)) & \
                  (df_distrito['m2 construidos'] <= m2 * (1 + m2_margin))
        
        # If we are desperate (relaxed), we might drop the type constraint?
        # Let's keep it for now as "Type" is a strong price determinant.
        mask = mask_tipo & mask_habs & mask_banos & mask_m2
    
    comparables = df_distrito[mask].copy()
    
    # Calculate similarity and precision for each comparable
    if len(comparables) > 0:
        # Similarity score (lower = more similar)
        comp_m2 = comparables['m2 construidos'].fillna(m2)
        comp_habs = comparables['habs'].fillna(habs)
        comp_banos = comparables['banos'].fillna(banos)
        
        # Add mismatch penalty if we ever relax the type constraint
        comp_tipo_mismatch = (comparables['tipo_norm'] != venta_tipo).astype(int)
        
        comparables['similarity'] = (
            abs(comp_m2 - m2) / m2 +
            abs(comp_habs - habs) * 0.5 +
            abs(comp_banos - banos) * 0.5 +
            comp_tipo_mismatch * 2.0  # Huge penalty for wrong type
        )
        
        # Precision score (0-100%, higher = more precise match)
        # VECTORIZED VERSION
        
        # M2 similarity (20% weight) - USER REQUEST
        comp_m2_vals = comparables['m2 construidos'].fillna(m2)
        m2_diffs = np.abs(comp_m2_vals - m2) / m2 if m2 > 0 else 0
        m2_scores = 0.20 * np.maximum(0, 1 - m2_diffs)
        
        # Type match (15% weight) - USER REQUEST
        tipo_scores = 0.15 * (comparables['tipo_norm'] == venta_tipo).astype(float)
        
        # Habs exact match (20% weight) - USER REQUEST
        comp_habs_vals = comparables['habs'].fillna(habs)
        habs_diffs = np.abs(comp_habs_vals - habs)
        habs_scores = 0.20 * np.where(habs_diffs == 0, 1.0, np.where(habs_diffs == 1, 0.5, 0))
        
        # Banos exact match (15% weight) - USER REQUEST
        comp_banos_vals = comparables['banos'].fillna(banos)
        banos_diffs = np.abs(comp_banos_vals - banos)
        banos_scores = 0.15 * np.where(banos_diffs == 0, 1.0, np.where(banos_diffs == 1, 0.5, 0))
        
        # Distrito always matches (15% weight) - USER REQUEST
        distrito_scores = 0.15
        
        # Extras (5% each = 15% total) - REMAINING
        # Garaje match (5% weight)
        comp_garaje = (comparables.get('Garaje', pd.Series(False, index=comparables.index)).fillna(False).astype(bool) | 
                       comparables.get('garaje', pd.Series(False, index=comparables.index)).fillna(False).astype(bool))
        garaje_scores = 0.05 * np.where(comp_garaje == venta_garaje, 1.0, 0.5)
        
        # Terraza match (5% weight)
        comp_terraza = (comparables.get('Terraza', pd.Series(False, index=comparables.index)).fillna(False).astype(bool) | 
                        comparables.get('terraza', pd.Series(False, index=comparables.index)).fillna(False).astype(bool))
        terraza_scores = 0.05 * np.where(comp_terraza == venta_terraza, 1.0, 0.5)
        
        # Ascensor match (5% weight)
        comp_ascensor = comparables.get('ascensor', pd.Series(False, index=comparables.index)).fillna(False).astype(bool)
        ascensor_scores = 0.05 * np.where(comp_ascensor == venta_ascensor, 1.0, 0.5)
        
        # Sum all scores
        total_scores = m2_scores + tipo_scores + habs_scores + banos_scores + distrito_scores + garaje_scores + terraza_scores + ascensor_scores
        
        comparables['precision'] = np.round(total_scores * 100, 1)
        comparables = comparables.sort_values('similarity')
    
    return comparables


def calculate_aggregate_precision(comparables_df):
    """
    Calculate aggregate precision score from a set of comparables.
    
    Returns:
        float: Weighted average precision (0-100%)
    """
    if comparables_df is None or len(comparables_df) == 0:
        return 0.0
    
    if 'precision' not in comparables_df.columns:
        return 50.0  # Default if no precision calculated
    
    # Weight by inverse similarity (more similar = higher weight)
    if 'similarity' in comparables_df.columns:
        weights = 1 / (comparables_df['similarity'] + 0.1)  # Avoid div by zero
        weighted_precision = (comparables_df['precision'] * weights).sum() / weights.sum()
    else:
        weighted_precision = comparables_df['precision'].mean()
    
    # Bonus for number of comparables
    n_bonus = min(10, len(comparables_df) * 2)  # +2% per comparable, max +10%
    
    return min(100.0, weighted_precision + n_bonus)



# =============================================================================
# PHASE 4: YIELD CALCULATION (ENHANCED with ML Model)
# =============================================================================
def phase_yields(config, df_venta, df_alquiler, zona_stats, use_cache=True):
    """
    Calculate gross and net yields using ML model and comparable ALQUILER properties.
    
    Enhanced version with:
    - ML model for rent estimation (if available)
    - Precision scoring per property
    - Rent range output (p5-p95)
    - Rounding to 25€
    
    Returns: df_venta_with_yields
    """
    global ML_RENT_MODEL
    
    print("\n" + "=" * 60)
    print("PHASE 4: YIELD CALCULATION (with ML Model + Comparables)")
    print("=" * 60)
    
    if use_cache:
        cached = load_checkpoint(config, 'yields')
        if cached is not None:
            # Validate cache has new columns
            if 'renta_rango' in cached.columns and 'precision' in cached.columns:
                return cached
            print("  [CACHE] Cache missing new columns (renta_rango/precision), re-calculating...")
    
    # --- Try ML Model approach first ---
    ml_success = False
    
    if ML_RENT_AVAILABLE:
        try:
            # Try to load from disk first
            if ML_RENT_MODEL is None:
                import joblib
                import os
                model_path = 'rent_model.joblib'
                if os.path.exists(model_path):
                    print(f"  [ML] Loading trained model from {model_path}...")
                    ML_RENT_MODEL = joblib.load(model_path)
                else:
                    print("  [ML] No trained model found on disk. Training on the fly...")
                    ML_RENT_MODEL = train_rent_model(df_alquiler, target_col='price', round_to=25)
            
            print("  [ML] Predicting rent for VENTA properties...")
            
            # Predict rent with confidence intervals
            df_venta = predict_rent(ML_RENT_MODEL, df_venta, include_range=True)
            
            ml_success = True
            print("  [ML] ML prediction successful!")
            
            if config.get('is_room_mode'):
                 # ML model predicted Room Price (since we trained on 'price' = room rent)
                 # We need to scale to Total Flat Rent = RoomPrice * NumRooms
                 print("  [ML] Room Mode: Scaling predicted Room Price by number of rooms...")
                 df_venta['renta_estimada'] = df_venta['renta_estimada'] * df_venta['habs'].fillna(3) # assume 3 if missing
                 
                 if 'renta_p05' in df_venta.columns:
                      df_venta['renta_p05'] *= df_venta['habs'].fillna(3)
                      df_venta['renta_p95'] *= df_venta['habs'].fillna(3)
            
        except Exception as e:
            print(f"  [ML] ML prediction failed: {e}")
            print("  [ML] Falling back to comparable-based estimation...")
            ml_success = False
    
    # --- Comparables approach (always run for references + precision) ---
    print("  Finding comparable ALQUILER properties for each VENTA...")
    
    # PRE-INDEX: Group alquiler by Distrito for O(1) lookup instead of full scan
    print("  [OPT] Pre-indexing ALQUILER data by Distrito...")
    alquiler_by_distrito = {distrito: group for distrito, group in df_alquiler.groupby('Distrito')}
    
    renta_estimada_list = []
    renta_p05_list = []
    renta_p95_list = []
    renta_rango_list = []
    precision_list = []
    comparables_list = []
    n_strict = 0
    n_relaxed = 0
    n_fallback = 0
    
    # Progress tracking
    total_props = len(df_venta)
    progress_interval = max(1, total_props // 10)  # Log every 10%
    
    for i, (idx, row) in enumerate(df_venta.iterrows()):
        # Progress logging
        if i > 0 and i % progress_interval == 0:
            print(f"    Progress: {i}/{total_props} ({100*i//total_props}%)")
        
        # Try strict criteria first (using pre-indexed lookup)
        comps = find_comparables(row, df_alquiler, strict=True, alquiler_index=alquiler_by_distrito)
        
        if len(comps) < 3:
            # Fall back to relaxed criteria
            comps = find_comparables(row, df_alquiler, strict=False, alquiler_index=alquiler_by_distrito)
            if len(comps) >= 3:
                n_relaxed += 1
        else:
            n_strict += 1
        
        if len(comps) > 0:
            # Calculate rent from comparables
            mean_val = comps['precio_m2'].mean()
            
            if config.get('is_room_mode'):
                # mean_val is Mean Room Price (since precio_m2 was aliased to price)
                n_rooms = row.get('habs', 3)
                if pd.isna(n_rooms) or n_rooms == 0: n_rooms = 3
                comp_renta = mean_val * n_rooms
            else:
                 # Standard: mean_val is Price/m2
                 comp_renta = mean_val * row['m2 construidos']
            
            # Round to 10€
            comp_renta = int(10 * round(comp_renta / 10))
            
            # =================================================================
            # STATISTICAL MARGIN OF ERROR (SEM-based)
            # =================================================================
            n_comps = len(comps)
            
            if n_comps >= 2:
                std_price_m2 = comps['precio_m2'].std()
                
                # Standard Error of the Mean (SEM)
                sem = std_price_m2 / math.sqrt(n_comps)
                
                # Margin of Error (%) at 95% confidence (z=1.96)
                if mean_val > 0:
                    margin_pct = (1.96 * sem / mean_val) * 100
                else:
                    margin_pct = 20.0  # Default max if mean is 0
                
                # Cap margin between 5% and 20%
                margin_pct = max(5.0, min(20.0, margin_pct))
            else:
                # Single comparable: use default high margin
                margin_pct = 15.0
            
            # =================================================================
            # PRECISION FROM MARGIN (inverse relationship)
            # =================================================================
            # margin < 5%  -> precision 85-100%
            # margin 5-10% -> precision 65-85%
            # margin 10-20% -> precision 40-65%
            # margin > 20% -> precision 15-40%
            
            # Get base precision from comparable similarity
            base_precision = calculate_aggregate_precision(comps)
            
            # Calculate margin-based precision (linear mapping)
            if margin_pct <= 5:
                margin_precision = 100 - (margin_pct * 3)  # 5% -> 85%
            elif margin_pct <= 10:
                margin_precision = 85 - ((margin_pct - 5) * 4)  # 10% -> 65%
            elif margin_pct <= 20:
                margin_precision = 65 - ((margin_pct - 10) * 2.5)  # 20% -> 40%
            else:
                margin_precision = 40 - ((margin_pct - 20) * 1.25)  # 25% -> 33.75%
            
            margin_precision = max(15.0, min(100.0, margin_precision))
            
            # Final precision = weighted average of similarity and margin precision
            precision = (base_precision * 0.4) + (margin_precision * 0.6)
            precision = round(max(10.0, min(100.0, precision)), 1)
            
            # =================================================================
            # DYNAMIC RANGE based on margin
            # =================================================================
            margin_factor = margin_pct / 100
            comp_p05 = int(10 * round((comp_renta * (1 - margin_factor)) / 10))
            comp_p95 = int(10 * round((comp_renta * (1 + margin_factor)) / 10))
            
            # Store references (top 10 by similarity)
            ref_cols = []
            for col in ['URL', 'titulo', 'Titulo', 'habs', 'banos', 'm2 construidos', 
                       'Distrito', 'garaje', 'Garaje', 'terraza', 'Terraza', 'price', 
                       'precio_m2', 'precision']:
                if col in comps.columns:
                    ref_cols.append(col)
            
            refs_df = comps.head(10)[ref_cols].copy()
            rename_map = {'Titulo': 'titulo', 'Garaje': 'garaje', 'Terraza': 'terraza'}
            refs_df = refs_df.rename(columns={k: v for k, v in rename_map.items() if k in refs_df.columns})
            refs = refs_df.to_dict('records')
        else:
            # No comparables found, fall back to zone median
            n_fallback += 1
            
            if config.get('is_room_mode'):
                # Room mode: use median room price * num rooms
                median_room_price = row.get('mediana_alquiler_m2', 400)  # mediana_alquiler_m2 holds room price in room mode
                n_rooms = row.get('habs', 3)
                if pd.isna(n_rooms) or n_rooms == 0: n_rooms = 3
                comp_renta = median_room_price * n_rooms
            else:
                # Standard mode: use price/m2 * flat size
                comp_renta = row.get('mediana_alquiler_m2', 10) * row['m2 construidos']
            
            comp_renta = int(10 * round(comp_renta / 10))
            comp_p05 = int(comp_renta * 0.80)  # ±20% for fallback (max uncertainty)
            comp_p95 = int(comp_renta * 1.20)
            precision = 15.0  # Low precision for fallback
            refs = []
        
        # ALWAYS store comparable-based values (SEM range is from real data)
        renta_estimada_list.append(comp_renta)
        renta_p05_list.append(comp_p05)
        renta_p95_list.append(comp_p95)
        renta_rango_list.append(f"{comp_p05}€ - {comp_p95}€")
        
        precision_list.append(round(precision, 1))
        comparables_list.append(refs)
    
    # Apply comparable-based results (ALWAYS use real data for range)
    df_venta['renta_estimada'] = renta_estimada_list
    df_venta['renta_p05'] = renta_p05_list
    df_venta['renta_p95'] = renta_p95_list
    df_venta['renta_rango'] = renta_rango_list
    df_venta['precision'] = precision_list
    df_venta['comparables'] = comparables_list
    
    print(f"  -> {n_strict} props with strict matches")
    print(f"  -> {n_relaxed} props with relaxed matches")
    print(f"  -> {n_fallback} props fell back to zone median")
    print(f"  -> Mean precision: {df_venta['precision'].mean():.1f}%")
    
    # Gross yield
    print("  Calculating gross yield...")
    df_venta['yield_bruta'] = (12 * df_venta['renta_estimada']) / df_venta['price']
    
    # Net yield
    print("  Calculating net yield...")
    gastos = config['gastos_recurrentes']
    vacancia = config['vacancia']
    costes = config['costes_compra']
    
    df_venta['renta_neta_anual'] = 12 * df_venta['renta_estimada'] * (1 - gastos) * (1 - vacancia)
    df_venta['base_invertida'] = df_venta['price'] * (1 + costes)
    df_venta['yield_neta'] = df_venta['renta_neta_anual'] / df_venta['base_invertida']
    
    print(f"  RESULT: Yield bruta media = {df_venta['yield_bruta'].mean()*100:.2f}%")
    print(f"          Yield neta media = {df_venta['yield_neta'].mean()*100:.2f}%")
    print(f"          Renta media estimada = {df_venta['renta_estimada'].mean():.0f}€/mes")
    
    save_checkpoint(config, 'yields', df_venta)
    return df_venta


# =============================================================================
# PHASE 5: SCORING
# =============================================================================
def phase_score(config, df_venta, zona_stats, use_cache=True):
    """
    Calculate opportunity scores.
    
    Returns: (df_venta_scored, zona_stats_updated)
    """
    print("\n" + "=" * 60)
    print("PHASE 5: SCORING")
    print("=" * 60)
    
    if use_cache:
        cached = load_checkpoint(config, 'score')
        if cached:
            return cached
    
    pesos = config['pesos_score']
    
    def normalize_0_100(series):
        min_val, max_val = series.min(), series.max()
        if max_val == min_val:
            return pd.Series(50, index=series.index)
        return ((series - min_val) / (max_val - min_val)) * 100
    
    print("  Calculating score components...")
    df_venta['score_descuento'] = normalize_0_100(df_venta['descuento_vs_mercado_pct'].fillna(0).clip(lower=0))
    df_venta['score_yield'] = normalize_0_100(df_venta['yield_bruta'].fillna(0).clip(lower=0, upper=0.5))
    df_venta['score_rebajado'] = df_venta['rebajado'].astype(int) * 100
    df_venta['score_zscore'] = normalize_0_100(-df_venta['z_score'].fillna(0))
    df_venta['score_comps'] = normalize_0_100(df_venta['n_alquiler'].fillna(0))
    
    print("  Calculating final score...")
    df_venta['score'] = (
        pesos['w1_descuento'] * df_venta['score_descuento'] +
        pesos['w2_yield'] * df_venta['score_yield'] +
        pesos['w3_rebajado'] * df_venta['score_rebajado'] +
        pesos['w4_zscore'] * df_venta['score_zscore'] +
        pesos['w5_comps'] * df_venta['score_comps']
    )
    
    df_venta['oportunidad'] = df_venta['below_market_combined']
    
    # Update zona_stats
    print("  Updating zone statistics...")
    zona_yield = df_venta.groupby('Distrito').agg({
        'yield_bruta': 'median',
        'yield_neta': 'median',
        'below_market_combined': 'sum',
        'score': 'mean'
    }).reset_index()
    zona_yield.columns = ['Distrito', 'yield_bruta_zona', 'yield_neta_zona', 'n_oportunidades', 'score_medio']
    
    zona_stats = zona_stats.merge(zona_yield, on='Distrito', how='left')
    zona_stats['pct_bajo_mercado'] = (zona_stats['n_oportunidades'] / zona_stats['n_venta']) * 100
    zona_stats['score_zona'] = (
        0.5 * normalize_0_100(zona_stats['yield_bruta_zona']) +
        0.5 * normalize_0_100(zona_stats['pct_bajo_mercado'])
    )
    
    n_opps = df_venta['oportunidad'].sum()
    print(f"  RESULT: {n_opps} opportunities, max score = {df_venta['score'].max():.1f}")
    
    result = (df_venta, zona_stats)
    save_checkpoint(config, 'score', result)
    return result





# =============================================================================
# PHASE 7: EXPORT
# =============================================================================
def phase_export(config, df_venta, zona_stats, log_calidad):
    """
    Export results to Excel and print to screen.
    
    Returns: output_file path
    """
    print("\n" + "=" * 60)
    print("PHASE 6: EXPORT")
    print("=" * 60)
    
    output_file = config['output_file']
    pesos = config['pesos_score']
    
    # Get opportunities sorted by score (best first)
    opps = df_venta[df_venta['oportunidad']].sort_values('score', ascending=False).copy()
    
    # Create clean output dataframe with standardized columns
    # Find Titulo column (may be named differently)
    titulo_col = next((c for c in opps.columns if c.lower() in ['titulo', 'title', 'nombre']), None)
    
    # 1. Opportunities (Sheet: oportunidades)
    # Create Propiedad as clean text (hyperlinks added later by xlsxwriter)
    if titulo_col:
        opps['Propiedad_text'] = opps[titulo_col]
    else:
        opps['Propiedad_text'] = opps['Distrito'] + ' - ' + opps['m2 construidos'].astype(int).astype(str) + 'm²'
    
    # Helper for safe column access
    def safe_col(df, col, default_val, dtype):
        if col in df.columns:
            # fillna(default_val) works on Series (even if boolean/object)
            return df[col].fillna(default_val).astype(dtype)
        # Verify length matches index
        return pd.Series(default_val, index=df.index, dtype=dtype)

    # Keep URL for UI and for xlsxwriter hyperlinks
    opps_output = pd.DataFrame({
        'Propiedad': opps['Propiedad_text'],
        'Distrito': opps['Distrito'],
        'm2': opps['m2 construidos'].astype(int),
        'Precio': opps['price'].astype(int),
        'habs': safe_col(opps, 'habs', 0, int),
        'banos': safe_col(opps, 'banos', 0, int),
        'garaje': safe_col(opps, 'Garaje', False, bool).apply(lambda x: 'Sí' if x else 'No'),
        'terraza': safe_col(opps, 'Terraza', False, bool).apply(lambda x: 'Sí' if x else 'No'),
        'Renta_estimada/mes': opps['renta_estimada'].round(0).astype(int),
        'Renta_Rango': opps.get('renta_rango', opps['renta_estimada'].apply(lambda x: f"{int(x)}€" if pd.notnull(x) else "-")),
        'Rentabilidad_Bruta_%': opps['yield_bruta'], # Decimal for % formatting
        'Rentabilidad_Neta_%': opps['yield_neta'], # Decimal
        'Precision': opps.get('precision', 0).astype(float).round(1),
        'Descuento_%': opps['descuento_vs_mercado_pct'] / 100, # Convert 32.7 to 0.327
        'Puntuación': opps['score'].round(1),
        'URL': opps['URL'], # Keep URL for UI and hyperlinks
        'comparables': opps['comparables'] # Keep for UI API
    })
    
    # Add Referencia 1-10 columns (URLs of top 10 comparables)
    for i in range(1, 11):
        col_name = f'Referencia {i}'
        opps_output[col_name] = opps_output['comparables'].apply(
            lambda refs: refs[i-1]['URL'] if refs and len(refs) >= i else ''
        )
    
    # =========================================================================
    # SCREEN OUTPUT
    # =========================================================================
    print("\n" + "=" * 100)
    print("OPORTUNIDADES DE INVERSION (ordenadas por Puntuación)")
    print("=" * 100)
    
    # Show top 20 on screen
    print("\nTOP 20 MEJORES OPORTUNIDADES:\n")
    print(f"{'Distrito':<25} {'m2':>5} {'Habs':>5} {'Precio':>10} {'Renta':>8} {'Rentab%':>7} {'Desc%':>6} {'Punt.':>6}")
    print("-" * 105)
    
    for idx, row in opps_output.head(20).iterrows():
        distrito = row['Distrito'][:24]
        # Get URL from original opps DF using index
        url = opps.loc[idx, 'URL']
        # Values are decimals, multiply by 100 for screen
        print(f"{distrito:<25} {row['m2']:>5} {row['habs']:>5} {row['Precio']:>10,} {row['Renta_estimada/mes']:>8,} {(row['Rentabilidad_Bruta_%']*100):>7.1f} {(row['Descuento_%']*100):>6.1f} {row['Puntuación']:>6.1f}")
        print(f"  -> {url}")
    
    print(f"\n... y {len(opps_output) - 20} oportunidades mas en el Excel.\n")
    
    # =========================================================================
    # EXCEL OUTPUT
    # =========================================================================
    print("  Preparing Excel sheets...")
    
    # Sheet 2: Distritos resumen (formerly Zonas resumen)
    # Rename columns and format
    zonas_excel = zona_stats.sort_values('yield_bruta_zona', ascending=False)[[
        'Distrito', 'n_venta', 'n_alquiler', 'mediana_venta_m2', 'p25_venta_m2',
        'mediana_alquiler_m2', 'yield_bruta_zona', 'yield_neta_zona', 
        'pct_bajo_mercado', 'n_oportunidades'
    ]].copy()
    
    # Yields are decimals in backend (0.05), so kept as is for % formatting
    # pct_bajo_mercado is 0-100 usually? Check formula: (n/N)*100.
    # We should convert to 0-1 for Excel % formatting if we apply % format to it.
    zonas_excel['yield_bruta_zona'] = zonas_excel['yield_bruta_zona'] # already decimal
    zonas_excel['pct_bajo_mercado'] = zonas_excel['pct_bajo_mercado'] / 100 # Convert 20.5 -> 0.205
    
    zonas_excel.columns = [
        'Distrito', 'prop_venta', 'prop_alq', 'Mediana_Venta_m2', 'P25_Venta_m2',
        'Mediana_Alquiler_m2', 'Rentabilidad_Bruta_%', 'Rentabilidad_Neta_%', 
        'Pct_Bajo_Mercado', 'Num oportunidades'
    ]
    
    # Sheet 3: Supuestos y parametros
    supuestos = pd.DataFrame([
        {'Parametro': 'gastos_recurrentes', 'Valor': f"{config['gastos_recurrentes']*100:.0f}%", 'Descripcion': 'Gastos sobre renta (comunidad, IBI, seguros)'},
        {'Parametro': 'vacancia', 'Valor': f"{config['vacancia']*100:.0f}%", 'Descripcion': 'Vacancia estimada anual'},
        {'Parametro': 'costes_compra', 'Valor': f"{config['costes_compra']*100:.0f}%", 'Descripcion': 'Costes de compra (impuestos, notaria)'},
        {'Parametro': 'umbral_residual_ml', 'Valor': f"{config['umbral_residual']*100:.0f}%", 'Descripcion': 'Umbral para modelo ML'},
        {'Parametro': 'w1_descuento', 'Valor': pesos['w1_descuento'], 'Descripcion': 'Peso: descuento vs mercado'},
        {'Parametro': 'w2_yield', 'Valor': pesos['w2_yield'], 'Descripcion': 'Peso: yield bruta'},
        {'Parametro': 'w3_rebajado', 'Valor': pesos['w3_rebajado'], 'Descripcion': 'Peso: ya rebajado'},
        {'Parametro': 'w4_zscore', 'Valor': pesos['w4_zscore'], 'Descripcion': 'Peso: z-score'},
        {'Parametro': 'w5_comps', 'Valor': pesos['w5_comps'], 'Descripcion': 'Peso: comparables'},
    ])
    
    # Sheet 4: Log calidad
    log_df = pd.DataFrame(log_calidad)
    
    # --- Save JSON for UI FIRST (with full data including comparables) ---
    import json
    json_file = output_file.replace('.xlsx', '.json')
    try:
        json_data = opps_output.to_dict(orient='records')
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, default=str)
        print(f"  Saved UI data to: {json_file}")
    except Exception as e:
        print(f"  [WARN] Could not save JSON: {e}")
    
    # Write Excel with xlsxwriter
    print(f"  Writing Excel to {output_file}...")
    try:
        with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
            workbook = writer.book
            
            # Formats
            currency_fmt = workbook.add_format({'num_format': '#,##0 €'})
            currency_no_dec_fmt = workbook.add_format({'num_format': '#,##0 €'})
            pct_fmt = workbook.add_format({'num_format': '0.00%'})
            int_fmt = workbook.add_format({'num_format': '0'})
            link_fmt = workbook.add_format({'font_color': 'blue', 'underline': 1})
            header_fmt = workbook.add_format({'bold': True})
            
            # --- Sheet 1: Oportunidades ---
            # Remove URL and comparables columns for Excel (URL embedded in hyperlinks)
            excel_cols = [c for c in opps_output.columns if c not in ['URL', 'comparables']]
            opps_for_excel = opps_output[excel_cols].copy()
            
            # Write DataFrame WITHOUT column A (Propiedad), then write hyperlinks
            # First, drop Propiedad column and write other data
            opps_without_propiedad = opps_for_excel.drop(columns=['Propiedad'])
            opps_without_propiedad.to_excel(writer, sheet_name='oportunidades', index=False, startcol=1)
            ws_opps = writer.sheets['oportunidades']
            
            # Write header for Propiedad with same format as other headers (bold)
            ws_opps.write(0, 0, 'Propiedad', header_fmt)
            
            # Write hyperlinks for Propiedad column (column A) using Excel formula
            for row_num, (idx, row) in enumerate(opps_output.iterrows(), start=1):
                url = row['URL']
                # Escape double quotes in text for the formula formula
                text = str(row['Propiedad']).replace('"', '""')
                formula = f'=HYPERLINK("{url}", "{text}")'
                ws_opps.write_formula(row_num, 0, formula, link_fmt)
            
            # Column mapping for opps_output (after dropping Propiedad for startcol=1):
            # A: Propiedad (links) - column 0
            # B: Distrito - column 1
            # C: m2 - column 2 (integer)
            # D: Precio - column 3 (currency)
            # E: habs - column 4 (integer, NOT currency)
            # F: banos - column 5 (integer, NOT percentage)
            # G: garaje - column 6 (text Sí/No)
            # H: terraza - column 7 (text Sí/No)
            # I: Renta_estimada/mes - column 8 (currency no decimals)
            # J: Renta_Rango - column 9 (text)
            # K: Rentabilidad_Bruta_% - column 10 (percentage)
            # L: Rentabilidad_Neta_% - column 11 (percentage)
            # M: Precision - column 12 (decimal/number)
            # N: Descuento_% - column 13 (percentage)
            # O: Puntuación - column 14 (number)
            # P+: Referencias 1-10 - columns 15+ (text/links)
            
            # Apply formats to columns
            ws_opps.set_column('A:A', 50)                    # Propiedad width
            ws_opps.set_column('B:B', 20)                    # Distrito
            ws_opps.set_column('C:C', 8, int_fmt)            # m2 (integer)
            ws_opps.set_column('D:D', 15, currency_no_dec_fmt)  # Precio (currency)
            ws_opps.set_column('E:E', 8, int_fmt)            # habs (integer)
            ws_opps.set_column('F:F', 8, int_fmt)            # banos (integer)
            ws_opps.set_column('G:H', 10)                    # garaje, terraza (text)
            ws_opps.set_column('I:I', 15, currency_no_dec_fmt)  # Renta_estimada/mes (currency)
            ws_opps.set_column('J:J', 15)                    # Renta_Rango (text)
            ws_opps.set_column('K:L', 12, pct_fmt)           # Rentabilidad_Bruta_%, Rentabilidad_Neta_% (percentage)
            ws_opps.set_column('M:M', 10)                    # Precision
            ws_opps.set_column('N:N', 12, pct_fmt)           # Descuento_% (percentage)
            ws_opps.set_column('O:O', 10)
            
            # --- Sheet 2: Distritos ---
            zonas_excel.to_excel(writer, sheet_name='distritos_resumen', index=False)
            ws_zonas = writer.sheets['distritos_resumen']
            
            # Apply formats
            ws_zonas.set_column('D:F', 18, currency_fmt) 
            ws_zonas.set_column('G:I', 15, pct_fmt)
            


            # --- Others ---
            supuestos.to_excel(writer, sheet_name='parametros', index=False)
            log_df.to_excel(writer, sheet_name='log_calidad', index=False)
            
        print(f"    - distritos_resumen: {len(zonas_excel)} distritos")
        
    except Exception as e:
        print(f"  [ERROR] Could not write Excel (xlsxwriter error?): {e}")
        # Fallback to standard pandas writer if xlsxwriter fails
        with pd.ExcelWriter(output_file) as writer:
            opps_output.to_excel(writer, sheet_name='oportunidades', index=False)
            zonas_excel.to_excel(writer, sheet_name='distritos_resumen', index=False)
            supuestos.to_excel(writer, sheet_name='parametros', index=False)
            log_df.to_excel(writer, sheet_name='log_calidad', index=False)

    print(f"\n  [OK] Excel saved: {output_file}")
    
    # Save JSON for UI - DYNAMIC NAME
    # App expects: resultado_*.json (sorted by time)
    # output_file is likely 'resultado_Mad-sur... .xlsx'
    json_file = output_file.replace('.xlsx', '.json')
    
    # We need to save records in a format suitable for the UI table
    # transform dataframe to list of dicts
    records = opps_output.to_dict(orient='records')
    
    try:
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(records, f, ensure_ascii=False, indent=2)
            
        print(f"  [OK] JSON saved: {json_file}")
    except Exception as e:
        print(f"  [WARN] Could not save JSON: {e}")

    return output_file


# =============================================================================
# MAIN PIPELINE
# =============================================================================
def run_pipeline(config, resume_from=None, force=False):
    """
    Run the full analysis pipeline.
    
    Args:
        config: Configuration dict
        resume_from: Phase name to resume from (None = full run)
        force: If True, ignore cache and rerun all
    """
    print("=" * 60)
    print("REAL ESTATE INVESTMENT ANALYSIS PIPELINE")
    print("=" * 60)
    print(f"  Input VENTA: {config['venta_file']}")
    print(f"  Input ALQUILER: {config['alquiler_file']}")
    print(f"  Output: {config['output_file']}")
    print(f"  Resume from: {resume_from or 'start'}")
    print(f"  Force rerun: {force}")
    
    if force:
        clear_cache(config)
    
    # Determine which phases to skip
    if resume_from:
        start_idx = PHASES.index(resume_from)
        skip_phases = set(PHASES[:start_idx])
    else:
        skip_phases = set()
    
    use_cache = not force
    
    # Phase 1: Load
    if 'load' in skip_phases:
        data = load_checkpoint(config, 'load')
        df_venta, df_alquiler = data
        print(f"\n[SKIP] Phase 1: Load (using cache)")
    else:
        df_venta, df_alquiler = phase_load(config, use_cache=False)
    
    # Phase 2: Clean
    if 'clean' in skip_phases:
        data = load_checkpoint(config, 'clean')
        df_venta, df_alquiler, log_calidad = data
        print(f"\n[SKIP] Phase 2: Clean (using cache)")
    else:
        df_venta, df_alquiler, log_calidad = phase_clean(config, df_venta, df_alquiler, use_cache=use_cache)
    
    # Phase 3: Market
    if 'market' in skip_phases:
        data = load_checkpoint(config, 'market')
        df_venta, zona_stats = data
        print(f"\n[SKIP] Phase 3: Market (using cache)")
    else:
        df_venta, zona_stats = phase_market(config, df_venta, df_alquiler, use_cache=use_cache)
    
    # Phase 4: Yields
    if 'yields' in skip_phases:
        df_venta = load_checkpoint(config, 'yields')
        print(f"\n[SKIP] Phase 4: Yields (using cache)")
    else:
        df_venta = phase_yields(config, df_venta, df_alquiler, zona_stats, use_cache=use_cache)
    
    # Phase 5: Score
    if 'score' in skip_phases:
        data = load_checkpoint(config, 'score')
        df_venta, zona_stats = data
        print(f"\n[SKIP] Phase 5: Score (using cache)")
    else:
        df_venta, zona_stats = phase_score(config, df_venta, zona_stats, use_cache=use_cache)
    
    # Phase 6: Export (always run)
    output_file = phase_export(config, df_venta, zona_stats, log_calidad)
    
    # Summary
    print("\n" + "=" * 60)
    print("PIPELINE COMPLETE")
    print("=" * 60)
    n_opps = df_venta['oportunidad'].sum()
    print(f"  Opportunities: {n_opps}")
    print(f"  Max score: {df_venta['score'].max():.1f}")
    print(f"  Output: {output_file}")
    
    return df_venta, zona_stats


# =============================================================================
# CLI ENTRY POINT
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description='Real Estate Investment Analysis',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python analysis.py                              # Auto-detect input files
  python analysis.py --venta X.xlsx --alquiler Y.xlsx
  python analysis.py --resume-from market         # Resume from a phase
  python analysis.py --force                      # Ignore cache
        """
    )
    parser.add_argument('--venta', type=str, default=None,
                        help='Path to VENTA Excel file (auto-detected if not specified)')
    parser.add_argument('--alquiler', type=str, default=None,
                        help='Path to ALQUILER Excel file (auto-detected if not specified)')
    parser.add_argument('--output', type=str, default=DEFAULT_CONFIG['output_file'],
                        help='Path to output Excel file')
    parser.add_argument('--resume-from', type=str, choices=PHASES[:-1],
                        help='Resume from a specific phase')
    parser.add_argument('--force', action='store_true',
                        help='Force full rerun, ignore cache')
    parser.add_argument('--clear-cache', action='store_true',
                        help='Clear cache and exit')
    parser.add_argument('--api-key', type=str, default=None,
                        help='Google API Key for Gemini')
    parser.add_argument('--model', type=str, default=None,
                        help='Gemini model name')
    
    args = parser.parse_args()
    
    # Build config
    config = DEFAULT_CONFIG.copy()

    # Update config with args
    if args.api_key:
        config['google_api_key'] = args.api_key
    
    if args.model:
        config['llm_model'] = args.model
    
    # Auto-detect input files if not specified
    if args.venta is None or args.alquiler is None:
        detected_venta, detected_alquiler = find_input_files()
        
        if args.venta is None:
            config['venta_file'] = detected_venta
        else:
            config['venta_file'] = args.venta
            
        if args.alquiler is None:
            config['alquiler_file'] = detected_alquiler
        else:
            config['alquiler_file'] = args.alquiler
    else:
        config['venta_file'] = args.venta
        config['alquiler_file'] = args.alquiler
    
    config['output_file'] = args.output
    
    # Handle clear cache
    if args.clear_cache:
        clear_cache(config)
        print("Cache cleared.")
        return

    # Validate input files exist
    if config['venta_file'] is None:
        print("=" * 60)
        print("ERROR: No VENTA file found or specified")
        # ... (error msg) ...
        sys.exit(1)
    
    if config['alquiler_file'] is None:
        print("=" * 60)
        print("ERROR: No ALQUILER file found or specified")
        # ... (error msg) ...
        sys.exit(1)
        
    if not Path(config['venta_file']).exists():
        print(f"ERROR: VENTA file not found: {config['venta_file']}")
        sys.exit(1)
    
    if not Path(config['alquiler_file']).exists():
        print(f"ERROR: ALQUILER file not found: {config['alquiler_file']}")
        sys.exit(1)
    
    # ---> GET USER FILTERS <---
    # Only ask if NOT resuming from late stages (unless resuming implies full start, but simple check is enough)
    # Actually, filters affect 'clean' phase. If resuming from 'market' or later, filters are skipped effectively
    # unless we force rerun of clean. 
    # If users just runs "python analysis.py", ask.
    
    try:
        user_filters = get_user_filters()
        config['filters'] = user_filters
    except KeyboardInterrupt:
        print("\n\nOperacion cancelada por el usuario.")
        sys.exit(0)
    
    # Run pipeline
    run_pipeline(config, resume_from=args.resume_from, force=args.force)



if __name__ == '__main__':
    main()
