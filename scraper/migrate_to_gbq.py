
import os
import json
import glob
import pandas as pd
from google.oauth2 import service_account
import pandas_gbq

# =============================================================================
# CONFIGURATION
# =============================================================================
# Determine absolute path to the key file (in the same dir as this script)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_FILE = os.path.join(SCRIPT_DIR, 'service-account.json')
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
    
    # 3. Load Data from SUPABASE (DatabaseManager)
    print("  Loading data from Supabase...")
    try:
        # Import here to avoid issues if run from wrong dir
        import sys
        if os.path.dirname(__file__) not in sys.path:
            sys.path.append(os.path.dirname(__file__))
        from database_manager import DatabaseManager
        
        db = DatabaseManager()
        # Fetch ALL data (empty string matches all provinces)
        df = db.get_historical_data("", operation_type=None)
    except Exception as e:
        print(f"ERROR: Could not fetch data from Supabase: {e}")
        return

    if df.empty:
        print("ERROR: Supabase returned empty data.")
        return
    
    print(f"  Loaded {len(df)} rows from Supabase.")

    # 4. Prepare Data for BigQuery
    print("  Preparing data schema...")
    # Convert lists/dicts to strings (JSON) because BQ doesn't handle nested structs from pandas easily
    for col in df.columns:
        if df[col].dtype == 'object':
            # Check if it looks like a list/dict object
            sample = df[col].iloc[0] if len(df) > 0 else None
            # Aggressive string conversion to avoid BQ errors
            df[col] = df[col].astype(str)
            df[col] = df[col].replace({'nan': None, 'None': None, '<NA>': None})
    
    # Drop internal columns not meant for export
    if '_isExpired' in df.columns:
        df = df.drop(columns=['_isExpired'])
    
    # Cast numeric columns to INTEGER (no decimals) where appropriate
    # (Checking against typical known int columns)
    int_columns = ['price', 'old price', 'habs', 'banos', 'm2 construidos', 'm2 utiles', 
                   'precio por m2', 'parcela', 'construido en', 'Num plantas']
    
    # Clean column names for matching
    df.columns = [c.strip() for c in df.columns]
    
    for col in int_columns:
        # Find matching col (case insensitive)
        match = next((c for c in df.columns if c.lower() == col.lower()), None)
        if match:
             # Convert to nullable Int64 (handles NaN properly)
             df[match] = pd.to_numeric(df[match], errors='coerce').astype('Int64')
    
    # Rename columns to standard SQL friendly
    # 1. Lowercase
    # 2. Replace known separators with _
    # 3. Remove any other non-alphanumeric (except underscore)
    import re
    def clean_col(c):
        c = c.lower().strip()
        c = c.replace(' ', '_').replace('/', '_per_').replace('%', 'pct').replace('.', '_')
        # Remove any remaining invalid chars (keep a-z, 0-9, _)
        c = re.sub(r'[^a-z0-9_]', '', c)
        # BQ columns cannot start with number
        if c and c[0].isdigit():
            c = 'num_' + c
        return c

    df.columns = [clean_col(c) for c in df.columns]
    
    # Ensure ingestion_time exists
    if 'ingestion_time' not in df.columns:
        df['ingestion_time'] = pd.Timestamp.now()
    else:
        df['ingestion_time'] = pd.to_datetime(df['ingestion_time']).fillna(pd.Timestamp.now())

    # 5. Upload to BigQuery (REPLACE mode as requested)
    full_table_id = f"{project_id}.{DATASET_NAME}.{TABLE_NAME}"
    print(f"  Uploading {len(df)} rows to {full_table_id} (REPLACING existing table)...")
    
    # Define table configuration for optimization
    table_config = {
        'time_partitioning': {
            'type': 'DAY',
            'field': 'ingestion_time',  # Partition by this column
        },
        'clustering_fields': ['provincia', 'source_file']  # Cluster by these columns
    }
    
    try:
        pandas_gbq.to_gbq(
            df,
            full_table_id,
            project_id=project_id,
            credentials=credentials,
            if_exists='replace',
            api_method='load_csv',
            table_schema=None, # Auto-detect schema
            # Pass optimization config via table_config if supported by wrapper or default to basic upload
            # Note: pandas_gbq 'table_config' param helps here? 
            # Actually standard pandas_gbq doesn't support generic table_config dict easily in simple usage without google-cloud-bigquery client for explicit creation.
            # However, we can use the 'time_partitioning' argument if available or pre-create table.
            # Let's use the 'table_schema' to enforce specific types if needed, but for partitioning, 
            # pandas_gbq < 0.18 might vary. 
            # Simplest way for robust partitioning is ensuring the column is timestamp.
        )
        
        # NOTE: pandas-gbq basic implementation might not fully expose clustering controls in to_gbq
        # We will use the Google Cloud BigQuery client directly for better control if to_gbq limits us,
        # but to keep it simple with what we have:
        # We will re-run this using the 'google.cloud.bigquery' client we already imported (service_account) 
        # to modifying the table definition OR just rely on standard upload for now if complexity is high.
        
        # BUT, wait! We can pass 'time_partitioning' argument in some versions.
        # Let's try standard upload first, and if we really need partitioning/clustering, we should 
        # use the client library directly to create the table first.
        
        # Let's switch to using the CLIENT library for the upload to ensure we get Partitioning + Clustering
        from google.cloud import bigquery
        from google.api_core.exceptions import NotFound
        
        client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        
        # Explicitly delete table if exists to force schema/partitioning update
        # (pandas_gbq 'replace' doesn't always handle partitioning changes well)
        try:
            client.delete_table(full_table_id)
            print(f"  Deleted existing table {full_table_id} to ensure clean creation.")
        except NotFound:
            pass # Table didn't exist, proceed
        
        job_config = bigquery.LoadJobConfig(
            write_disposition="WRITE_EMPTY", # New table (since we deleted it)
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="ingestion_time"
            ),
            clustering_fields=["provincia", "source_file"],
            autodetect=True
        )
        
        job = client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )
        
        job.result() # Wait for completion
        
        print("\nSUCCESS: Migration complete!")
        print(f"Data synchronized: Supabase -> BigQuery ({len(df)} rows)")
        print("Table optimization: Partitioned by Day, Clustered by Province")
        
    except Exception as e:
        print(f"\nERROR during upload: {e}")
        # print("Tip: Ensure the BigQuery API is enabled.") # context already clear



if __name__ == "__main__":
    migrate()
