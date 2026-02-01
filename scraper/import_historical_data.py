
import os
import glob
import pandas as pd
from database_manager import DatabaseManager
from idealista_scraper.excel_writer import RENAME_COMPAT

SALIDAS_DIR = os.path.join(os.path.dirname(__file__), 'salidas')
# Also check base dir for legacy files or direct outputs
BASE_DIR = os.path.dirname(__file__)

def import_all():
    db = DatabaseManager()
    print("Starting import of historical data...")
    
    # helper to clean df before insert
    def clean_df(df):
        # Apply standard renames
        df = df.rename(columns={k:v for k,v in RENAME_COMPAT.items() if k in df.columns})
        return df

    files = glob.glob(os.path.join(SALIDAS_DIR, "*.xlsx")) + glob.glob(os.path.join(BASE_DIR, "resultado_*.xlsx"))
    
    unique_files = set(os.path.abspath(f) for f in files)
    
    for f_path in unique_files:
        print(f"Processing: {os.path.basename(f_path)}")
        try:
            xl = pd.ExcelFile(f_path)
            all_dfs = []
            for sheet in xl.sheet_names:
                df = pd.read_excel(xl, sheet_name=sheet)
                if 'URL' in df.columns or 'url' in df.columns:
                    # Enrich with source info
                    df = clean_df(df)
                    # If Provincia is missing, try to guess from filename?
                    # Filename format: idealista_City_Type.xlsx or resultado_...
                    # But usually "Provincia" column exists in recent scrapes.
                    
                    if 'Provincia' not in df.columns:
                        # Try extraction
                        params = os.path.basename(f_path).split('_')
                        if len(params) > 1 and params[0] == 'idealista':
                             # heuristic: idealista_Madrid_Venta...
                             df['Provincia'] = params[1]
                    
                    all_dfs.append(df)
            
            if all_dfs:
                full_df = pd.concat(all_dfs, ignore_index=True)
                db.save_listings_from_df(full_df, source_file=os.path.basename(f_path))
                
        except Exception as e:
            print(f"Error processing {f_path}: {e}")

    print("Import complete.")

if __name__ == "__main__":
    import_all()
