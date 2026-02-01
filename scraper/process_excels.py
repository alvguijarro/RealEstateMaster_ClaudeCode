import os
import glob
import pandas as pd
from database_manager import DatabaseManager

SALIDAS_DIR = os.path.join(os.path.dirname(__file__), 'salidas')

def process_all_excels():
    print(f"Scanning directory: {SALIDAS_DIR}")
    
    # Get all xlsx files
    files = glob.glob(os.path.join(SALIDAS_DIR, "*.xlsx"))
    
    # Initialize DB
    db = DatabaseManager()
    if not db.connected:
        print("Failed to connect to database. Aborting.")
        return

    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Skip temp files
        if filename.startswith('~$'):
            continue
            
        print(f"\nProcessing: {filename} ...")
        
        try:
            # Read Excel (ALL sheets)
            # sheet_name=None returns a dict {sheet_name: DataFrame}
            xls = pd.read_excel(file_path, sheet_name=None)
            
            for sheet_name, df in xls.items():
                print(f"  > Sheet: {sheet_name} ({len(df)} rows)")
                # Save to DB
                # Pass the filename; database_manager handles logic
                db.save_listings_from_df(df, source_file=filename)
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")

if __name__ == "__main__":
    process_all_excels()
