
import os
import json
import glob
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

# =============================================================================
# CONFIGURATION
# =============================================================================
KEY_FILE = 'service-account.json'
JSON_FILE_PATTERN = 'resultado_*.json'
EXCEL_FILE_PATTERN = 'resultado_*.xlsx'
DATASET_NAME = 'real_estate'
TABLE_NAME = 'oportunidades'

def get_project_id(key_path):
    with open(key_path, 'r') as f:
        data = json.load(f)
        return data.get('project_id')

def find_latest_file():
    # Try JSON first (cleaner data types)
    files = glob.glob(JSON_FILE_PATTERN)
    if not files:
        # Fallback to Excel
        files = glob.glob(EXCEL_FILE_PATTERN)
    
    if not files:
        return None
    
    # Return most recent
    return max(files, key=os.path.getmtime)

def migrate(source_file=None):
    print("=" * 60)
    print("BIGQUERY MIGRATION JOB")
    print("=" * 60)
    
    # 1. Check Key File
    if not os.path.exists(KEY_FILE):
        print(f"ERROR: Key file '{KEY_FILE}' not found!")
        print("Please download it from Google Cloud Console and place it here.")
        return

    try:
        project_id = get_project_id(KEY_FILE)
        print(f"  Project ID: {project_id}")
    except Exception as e:
        print(f"ERROR: Could not read project_id from key file: {e}")
        return

    # 2. Authenticate
    print("  Authenticating...")
    credentials = service_account.Credentials.from_service_account_file(KEY_FILE)
    
    # 3. Load Data
    if source_file is None:
        source_file = find_latest_file()
    
    if not source_file:
        print("ERROR: No source file found/provided.")
        print("Run analysis.py first to generate data.")
        return
    
    print(f"  Loading data from: {source_file}")
    if source_file.endswith('.json'):
        df = pd.read_json(source_file)
    else:
        df = pd.read_excel(source_file, sheet_name='oportunidades')

    # 4. Prepare Data for BigQuery
    print("  Preparing data schema...")
    # Convert lists/dicts to strings (JSON) because BQ doesn't handle nested structs from pandas easily
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if it looks like a list/dict object
            sample = df[col].iloc[0] if len(df) > 0 else None
            if isinstance(sample, (list, dict)):
                df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if x else None)
    
    # Drop internal columns not meant for export
    if '_isExpired' in df.columns:
        df = df.drop(columns=['_isExpired'])
    
    # Cast numeric columns to INTEGER (no decimals)
    int_columns = ['price', 'old price', 'habs', 'banos', 'm2 construidos', 'm2 utiles', 
                   'precio por m2', 'parcela', 'construido en', 'Num plantas']
    for col in int_columns:
        if col in df.columns:
            # Convert to nullable Int64 (handles NaN properly)
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')
    
    # Rename columns to standard SQL friendly (lowercase, underscores)
    # Our columns are already mostly clean, but let's be safe
    df.columns = [c.lower().replace(' ', '_').replace('/', '_per_').replace('%', 'pct') for c in df.columns]
    
    # Add ingestion timestamp
    df['ingestion_time'] = pd.Timestamp.now()

    # 5. Upload
    full_table_id = f"{project_id}.{DATASET_NAME}.{TABLE_NAME}"
    print(f"  Uploading {len(df)} rows to {full_table_id}...")
    
    try:
        pandas_gbq.to_gbq(
            df,
            full_table_id,
            project_id=project_id,
            credentials=credentials,
            if_exists='append', # Add to existing data
            api_method='load_csv', # Efficient for batch
        )
        print("\nSUCCESS: Migration complete!")
        print(f"Data available in BigQuery: SELECT * FROM `{full_table_id}` LIMIT 10")
        
    except Exception as e:
        print(f"\nERROR during upload: {e}")
        print("Tip: Ensure the BigQuery API is enabled and the dataset exists (or pandas-gbq expects permission to create it).")

if __name__ == "__main__":
    migrate()
