
import os
import pandas as pd
import json
from datetime import datetime
from supabase import create_client, Client

# =============================================================================
# CONFIGURATION
# =============================================================================
# In a production environment, use environment variables. 
# For this portable tool, we use the provided constants.
SUPABASE_URL = "https://pztmyzdcjklrpvbnvdxc.supabase.co"
SUPABASE_KEY = "sb_publishable_aQw098XivpsrQOFdAyXo0g_jG-bjdiW"

# Map DataFrame columns to DB columns (snake_case)
COL_MAPPING = {
    "Titulo": "titulo",
    "price": "price",
    "old price": "old_price",
    "price change %": "price_change_pct",
    "Ubicacion": "ubicacion",
    "actualizado hace": "actualizado_hace",
    "m2 construidos": "m2_construidos",
    "m2 utiles": "m2_utiles",
    "precio por m2": "precio_m2",
    "Num plantas": "num_plantas",
    "habs": "habs",
    "banos": "banos",
    "Terraza": "terraza",
    "Garaje": "garaje",
    "Armarios": "armarios",
    "Trastero": "trastero",
    "Calefaccion": "calefaccion",
    "tipo": "tipo",
    "parcela": "parcela",
    "ascensor": "ascensor",
    "orientacion": "orientacion",
    "altura": "altura",
    "construido en": "construido_en",
    "jardin": "jardin",
    "piscina": "piscina",
    "aire acond": "aire_acond",
    "Calle": "calle",
    "Barrio": "barrio",
    "Distrito": "distrito",
    "Zona": "zona",
    "Ciudad": "ciudad",
    "Provincia": "provincia",
    "Consumo 1": "consumo_1",
    "Consumo 2": "consumo_2",
    "Emisiones 1": "emisiones_1",
    "Emisiones 2": "emisiones_2",
    "estado": "estado",
    "gastos comunidad": "gastos_comunidad",
    "okupado": "okupado",
    "Copropiedad": "copropiedad",
    "con inquilino": "con_inquilino",
    "nuda propiedad": "nuda_propiedad",
    "ces. remate": "ces_remate",
    "tipo anunciante": "tipo_anunciante",
    "nombre anunciante": "nombre_anunciante",
    "Descripcion": "descripcion",
    "Fecha Scraping": "fecha_scraping",
    "URL": "url",
    "Anuncio activo": "anuncio_activo",
    "Baja anuncio": "baja_anuncio",
    "Comunidad Autonoma": "comunidad_autonoma"
}

class DatabaseManager:
    def __init__(self, db_path=None):
        # db_path is kept for backward compatibility signature but ignored
        try:
            self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
            self.connected = True
        except Exception as e:
            print(f"  [DB] Error connecting to Supabase: {e}")
            self.connected = False

    def save_listings_from_df(self, df: pd.DataFrame, source_file: str = None):
        """
        Save a DataFrame of listings to Supabase.
        Upserts based on URL (PK).
        """
        if not self.connected or df.empty:
            return

        # Prepare DataFrame
        df = df.copy()
        
        # Rename columns to match DB
        df = df.rename(columns=COL_MAPPING)
        
        # Add metadata columns if missing
        if 'source_file' not in df.columns and source_file:
            df['source_file'] = source_file
        if 'ingestion_date' not in df.columns:
            df['ingestion_date'] = datetime.now().isoformat()

        # Handle boolean conversion
        bool_cols = [
            'terraza', 'garaje', 'armarios', 'trastero', 'ascensor', 'jardin', 'piscina', 'aire_acond',
            'okupado', 'copropiedad', 'con_inquilino', 'nuda_propiedad', 'ces_remate', 'anuncio_activo', 'baja_anuncio'
        ]
        
        for col in bool_cols:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: True if str(x).lower() in ['true', '1', 'si', 'sí', 'yes'] else False)

        # Handle Integer conversion (Fix for 2.0 vs 2 issue)
        int_cols = ['num_plantas', 'habs', 'banos', 'construido_en']
        for col in int_cols:
            if col in df.columns:
                # Use a safe converter
                def safe_int(x):
                    try:
                        if pd.isna(x) or x == '' or str(x).lower() == 'nan':
                            return None
                        s = str(x).replace(',', '.')
                        return int(float(s))
                    except:
                        return None
                
                df[col] = df[col].apply(safe_int)

        # Handle numeric conversion (NaN to None for JSON serialization)
        # Supabase API expects standard JSON types
        df = df.replace({float('nan'): None})

        # -------------------------------------------------------------------------
        # NEW: Auto-populate "operation" (VENTA/ALQUILER) and "province"
        # -------------------------------------------------------------------------
        
        # 1. Operation Type - STRICT LOGIC
        # Logic: 
        # - Must contain 'alquiler'/'rent' -> 'ALQUILER'
        # - Must contain 'venta'/'sale' -> 'VENTA'
        # - Else -> Ignore/Drop
        
        if 'operation' not in df.columns:
            df['operation'] = None
            
            # Helper to determine operation from string
            def get_op(s):
                if not isinstance(s, str): return None
                s = s.lower()
                if 'rent' in s or 'alquiler' in s:
                    return 'ALQUILER'
                if 'sale' in s or 'venta' in s:
                    return 'VENTA'
                return None

            # Attempt to set from source_file argument
            if source_file:
                op = get_op(source_file)
                if op:
                    df['operation'] = op
            
            # If still needed, check per-row source_file column
            mask_still_none = df['operation'].isnull()
            if mask_still_none.any() and 'source_file' in df.columns:
                df.loc[mask_still_none, 'operation'] = df.loc[mask_still_none, 'source_file'].apply(get_op)

        # FILTER: Drop rows where operation could not be determined
        initial_count = len(df)
        df = df.dropna(subset=['operation'])
        dropped_count = initial_count - len(df)
        if dropped_count > 0:
            print(f"  [DB] Skipped {dropped_count} rows because operation type (venta/alquiler) could not be determined from filename.")
        
        if df.empty:
            print("  [DB] No valid rows to insert after operation filtering.")
            return

        # 2. Province (Use 'Provincia' column from Excel, which is mapped to 'provincia' in DF)
        if 'province' not in df.columns:
            if 'provincia' in df.columns:
                # Use the Excel provided province
                df['province'] = df['provincia']
            elif 'ubicacion' in df.columns:
                # Fallback to parsing location if 'Provincia' column missing in Excel
                print("  [DB] 'Provincia' column missing. Falling back to parsing 'Ubicacion'.")
                def extract_province(loc):
                    if not loc or not isinstance(loc, str): 
                        return None
                    parts = loc.split(',')
                    return parts[-1].strip()
                df['province'] = df['ubicacion'].apply(extract_province)

        if 'url' in df.columns:
            df = df.dropna(subset=['url'])
            before_dedup = len(df)
            df = df.drop_duplicates(subset=['url'], keep='last')
            if len(df) < before_dedup:
                print(f"  [DB] Dropped {before_dedup - len(df)} duplicate URLs within the batch.")

        # Columns to keep: strictly those in our schema mapping + metadata
        valid_cols = set(COL_MAPPING.values()) | {'source_file', 'ingestion_date', 'operation', 'province'}
        available_cols = [c for c in df.columns if c in valid_cols]
        
        # Filter DF
        df = df[available_cols]

        # Convert to records (list of dicts)
        records = df.to_dict(orient='records')
        
        # NUKE APPROACH: Manually sanitize integer fields in the dicts
        # This bypasses any pandas weirdness with object/int/float types
        int_fields = ['num_plantas', 'habs', 'banos', 'construido_en']
        for record in records:
            for field in int_fields:
                if field in record and record[field] is not None:
                    try:
                        # Handle strings like '2.0', numbers like 2.0, or '2,0'
                        val_str = str(record[field]).replace(',', '.')
                        val_float = float(val_str)
                        record[field] = int(val_float)
                    except:
                        record[field] = None

        # Batch insert (Supabase has limits on request size, so chunk it)
        CHUNK_SIZE = 100
        for i in range(0, len(records), CHUNK_SIZE):
            chunk = records[i:i + CHUNK_SIZE]
            try:
                # Upsert is safer for "INSERT OR REPLACE" logic
                self.client.table('listings').upsert(chunk).execute()
                print(f"  [DB] Saved/Updated {len(chunk)} listings to Supabase.")
            except Exception as e:
                print(f"  [DB] Error saving batch {i//CHUNK_SIZE + 1}: {e}")

    def delete_all_listings(self):
        """
        Delete ALL data from the listings table.
        USED WITH CAUTION.
        """
        if not self.connected:
            return False
        try:
            # count = self.client.table('listings').select("*", count='exact').execute().count
            # Supabase delete requires a filter. To delete all, we filter where id is distinct from 0 (if id exists)
            # or just 'url' is not null.
            self.client.table('listings').delete().neq('url', 'NON_EXISTENT_URL_PLACEHOLDER').execute()
            print("  [DB] All listings deleted successfully.")
            return True
        except Exception as e:
            print(f"  [DB] Error deleting data: {e}")
            return False

    def get_historical_data(self, provincia: str, operation_type: str = None) -> pd.DataFrame:
        """
        Retrieve listings from Supabase filters by province and operation.
        """
        if not self.connected:
            return pd.DataFrame()

        try:
            # Build query
            query = self.client.table('listings').select("*")
            
            # Fuzzy match for province is harder via standard API without text search ext, 
            # but we can use 'ilike' filter
            query = query.ilike('provincia', f"%{provincia}%")
            
            if operation_type:
                query = query.ilike('source_file', f"%{operation_type}%")
            
            # Execute with limit (fetch e.g. last 10000?) - Supabase applies default limit usually
            # Getting ALL history might require pagination if it's huge. 
            # For now, let's grab a reasonable chunk, or loop.
            response = query.execute()
            data = response.data
            
            if not data:
                return pd.DataFrame()
                
            df = pd.DataFrame(data)
            
            # Reverse map columns
            reverse_mapping = {v: k for k, v in COL_MAPPING.items()}
            df = df.rename(columns=reverse_mapping)
            
            return df
            
        except Exception as e:
            print(f"  [DB] Error retrieving history: {e}")
            return pd.DataFrame()

    def get_all_districts(self, provincia: str) -> list:
        if not self.connected:
            return []
        try:
            # Not strict distinct query, but we can fetch districts and uniq in python
            response = self.client.table('listings').select('distrito').ilike('provincia', f"%{provincia}%").execute()
            districts = sorted(list(set(row['distrito'] for row in response.data if row['distrito'])))
            return districts
        except Exception:
            return []
