"""
Data Ingestion Module - Load CSV data to Staging Tables
Performs bulk loading with transaction management for atomicity
"""

import pandas as pd
import psycopg2
from psycopg2 import sql
from io import StringIO
import logging
from pathlib import Path
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection details
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_db'
DB_USER = 'admin'
DB_PASSWORD = 'password'


class DataIngestion:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.ingestion_results = {
            'ingestion_timestamp': datetime.now().isoformat(),
            'tables_loaded': {},
            'total_execution_time_seconds': 0,
            'status': 'pending'
        }
    
    def connect(self):
        """Connect to database"""
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                database=DB_NAME,
                user=DB_USER,
                password=DB_PASSWORD
            )
            self.cursor = self.conn.cursor()
            logger.info(f"✓ Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")
            return True
        except psycopg2.Error as e:
            logger.error(f"✗ Connection failed: {str(e)}")
            return False
    
    def truncate_staging_tables(self):
        """Truncate all staging tables (idempotent operation)"""
        try:
            logger.info("Truncating staging tables...")
            tables = [
                'staging.transaction_items',  # Delete this first (has FK)
                'staging.transactions',        # Then this
                'staging.customers',
                'staging.products'
            ]
            
            for table in tables:
                self.cursor.execute(f"TRUNCATE TABLE {table} CASCADE;")
            
            self.conn.commit()
            logger.info("✓ Staging tables truncated")
            return True
        except psycopg2.Error as e:
            logger.error(f"✗ Truncate failed: {str(e)}")
            self.conn.rollback()
            return False
    
    def load_csv_to_staging(self, csv_file, table_name):
        """
        Load CSV file to staging table using COPY method (fastest)
        
        Args:
            csv_file: Path to CSV file
            table_name: Target staging table name
        
        Returns:
            Number of rows loaded
        """
        try:
            logger.info(f"Loading {csv_file} to {table_name}...")
            
            # Read CSV to get row count
            df = pd.read_csv(csv_file)
            num_rows = len(df)
            
            # Prepare data for COPY command
            buffer = StringIO()
            df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)
            
            # Use PostgreSQL COPY for bulk loading (Very fast!)
            self.cursor.copy_from(
                buffer,
                table_name,
                sep=',',
                columns=[col.lower() for col in df.columns]
            )
            
            self.conn.commit()
            logger.info(f"✓ Loaded {num_rows} rows to {table_name}")
            
            return {
                'table': table_name,
                'rows_loaded': num_rows,
                'status': 'success',
                'error_message': None
            }
            
        except Exception as e:
            logger.error(f"✗ Failed to load {csv_file}: {str(e)}")
            self.conn.rollback()
            return {
                'table': table_name,
                'rows_loaded': 0,
                'status': 'failed',
                'error_message': str(e)
            }
    
    def validate_staging_load(self):
        """Validate that data was loaded correctly"""
        try:
            logger.info("Validating staging load...")
            
            validation = {}
            tables = [
                'staging.customers',
                'staging.products',
                'staging.transactions',
                'staging.transaction_items'
            ]
            
            for table in tables:
                self.cursor.execute(f"SELECT COUNT(*) FROM {table};")
                count = self.cursor.fetchone()[0]
                validation[table] = count
                logger.info(f"  {table}: {count} rows")
            
            return validation
            
        except psycopg2.Error as e:
            logger.error(f"✗ Validation failed: {str(e)}")
            return None
    
    def ingest_all_data(self):
        """Main ingestion workflow"""
        start_time = datetime.now()
        
        logger.info("=" * 60)
        logger.info("Starting Data Ingestion to Staging")
        logger.info("=" * 60)
        
        try:
            # Step 1: Connect
            if not self.connect():
                self.ingestion_results['status'] = 'failed'
                return self.ingestion_results
            
            # Step 2: Truncate (idempotent - safe to run multiple times)
            if not self.truncate_staging_tables():
                self.ingestion_results['status'] = 'failed'
                return self.ingestion_results
            
            # Step 3: Load data from CSVs
            csv_files = [
                ('data/raw/customers.csv', 'staging.customers'),
                ('data/raw/products.csv', 'staging.products'),
                ('data/raw/transactions.csv', 'staging.transactions'),
                ('data/raw/transaction_items.csv', 'staging.transaction_items')
            ]
            
            for csv_file, table_name in csv_files:
                if Path(csv_file).exists():
                    result = self.load_csv_to_staging(csv_file, table_name)
                    self.ingestion_results['tables_loaded'][table_name] = result
                else:
                    logger.warning(f"File not found: {csv_file}")
                    self.ingestion_results['tables_loaded'][table_name] = {
                        'status': 'failed',
                        'error_message': 'File not found'
                    }
            
            # Step 4: Validate
            validation = self.validate_staging_load()
            if validation:
                self.ingestion_results['validation'] = validation
            
            # Step 5: Calculate execution time
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            self.ingestion_results['total_execution_time_seconds'] = execution_time
            
            self.ingestion_results['status'] = 'success'
            
            logger.info("=" * 60)
            logger.info("✓ Data Ingestion Complete!")
            logger.info(f"Total time: {execution_time:.2f} seconds")
            logger.info("=" * 60)
            
            return self.ingestion_results
            
        except Exception as e:
            logger.error(f"Error during ingestion: {str(e)}")
            self.ingestion_results['status'] = 'failed'
            return self.ingestion_results
        
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
    
    def save_ingestion_summary(self, output_dir='data/staging'):
        """Save ingestion summary to JSON"""
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            summary_file = f'{output_dir}/ingestion_summary.json'
            with open(summary_file, 'w') as f:
                json.dump(self.ingestion_results, f, indent=2)
            
            logger.info(f"✓ Ingestion summary saved to {summary_file}")
        except Exception as e:
            logger.error(f"Failed to save summary: {str(e)}")


def main():
    """Main execution"""
    ingestion = DataIngestion()
    results = ingestion.ingest_all_data()
    ingestion.save_ingestion_summary()
    
    return results


if __name__ == '__main__':
    main()
