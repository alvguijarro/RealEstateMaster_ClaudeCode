import os
import json
import logging
from google.cloud import bigquery
from google.oauth2 import service_account

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path=None):
        """
        Initialize Database Manager.
        Note: Supabase functionality has been removed.
        BigQuery is the primary cloud storage.
        """
        self.db_path = db_path
        self.bq_client = self.init_bigquery_client()

    def init_bigquery_client(self):
        """Initialize Google BigQuery client using service account."""
        try:
            # Look for service account in scraper directory
            current_dir = os.path.dirname(os.path.abspath(__file__))
            sa_path = os.path.join(current_dir, 'service-account.json')
            
            if os.path.exists(sa_path):
                credentials = service_account.Credentials.from_service_account_file(sa_path)
                return bigquery.Client(credentials=credentials, project=credentials.project_id)
            else:
                logger.warning("BigQuery: service-account.json not found. BigQuery exports will fail.")
                return None
        except Exception as e:
            logger.error(f"Error initializing BigQuery client: {e}")
            return None

    def save_to_bigquery(self, dataset_id, table_id, df):
        """Upload a pandas DataFrame to BigQuery using an UPSERT (Delete + Insert) strategy."""
        if not self.bq_client:
            logger.error("BigQuery client not initialized.")
            return False

        if df.empty:
            logger.info("DataFrame is empty, skipping BigQuery upload.")
            return True

        try:
            # Add upload timestamp to track when data was pushed
            from datetime import datetime
            df['upload_timestamp'] = datetime.now()
            
            project_id = self.bq_client.project
            full_table_id = f"{project_id}.{dataset_id}.{table_id}"
            staging_table_id = f"{full_table_id}_staging"
            
            # 1. Upload data to a staging table (Overwrite mode for safety)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            
            logger.info(f"Uploading {len(df)} rows to staging table: {staging_table_id}")
            load_job = self.bq_client.load_table_from_dataframe(df, staging_table_id, job_config=job_config)
            load_job.result()
            
            # 2. DELETE existing rows in main table that match URLs in staging
            # BigQuery MERGE is also an option, but DELETE + INSERT is robust for schema evolution
            dml_statement = f"""
            DELETE FROM `{full_table_id}`
            WHERE URL IN (SELECT URL FROM `{staging_table_id}`)
            """
            logger.info(f"Deduplicating main table: {full_table_id}")
            query_job = self.bq_client.query(dml_statement)
            query_job.result()
            
            # 3. INSERT all rows from staging to main table
            insert_statement = f"""
            INSERT INTO `{full_table_id}`
            SELECT * FROM `{staging_table_id}`
            """
            logger.info(f"Inserting new/updated rows into {full_table_id}")
            insert_job = self.bq_client.query(insert_statement)
            insert_job.result()
            
            # 4. Cleanup: Drop staging table
            self.bq_client.delete_table(staging_table_id, not_found_ok=True)
            
            logger.info(f"Successfully UPSERTED {len(df)} rows to BigQuery table {full_table_id}")
            return True
        except Exception as e:
            logger.error(f"Error during BigQuery UPSERT: {e}")
            return False

    def save_listings_from_df(self, df, source_file="unknown"):
        """Wrapper for saving listings. Now automatically pushes to BigQuery if client is active."""
        logger.info(f"DatabaseManager: Processing {len(df)} listings from {source_file}")
        
        # Check if we should auto-upload to BigQuery
        # We'll use a default dataset/table from environment or constants
        dataset_id = "real_estate"
        table_id = "properties_raw"
        
        if self.bq_client:
            logger.info("DatabaseManager: Auto-uploading to BigQuery...")
            return self.save_to_bigquery(dataset_id, table_id, df)
        
        return True
