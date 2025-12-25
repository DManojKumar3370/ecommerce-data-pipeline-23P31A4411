"""
Export warehouse tables to CSV for Tableau
Quick export script for CSV-based dashboard
"""

import psycopg2
import pandas as pd
from pathlib import Path
import os

# Database connection settings
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'ecommerce_db')
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASSWORD = os.getenv('DB_PASSWORD', 'mysql@123')

# Output directory
OUTPUT_DIR = Path('data/csv_exports')
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def connect_db():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("✓ Connected to PostgreSQL database")
        return conn
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        raise

def export_table(conn, schema, table_name):
    """Export single table to CSV"""
    try:
        query = f"SELECT * FROM {schema}.{table_name}"
        df = pd.read_sql(query, conn)
        
        csv_file = OUTPUT_DIR / f"{table_name}.csv"
        df.to_csv(csv_file, index=False)
        
        print(f"  ✓ {table_name}: {len(df)} rows exported")
        return True
    except Exception as e:
        print(f"  ✗ {table_name} failed: {e}")
        return False

def main():
    """Export all warehouse tables"""
    
    # Connect to database
    conn = connect_db()
    
    # List of tables to export
    tables = [
        'fact_sales',
        'dim_customers',
        'dim_products',
        'dim_date',
        'dim_payment_method',
        'agg_daily_sales',
        'agg_product_performance',
        'agg_customer_metrics'
    ]
    
    print("\n" + "="*60)
    print("📊 EXPORTING WAREHOUSE TABLES TO CSV")
    print("="*60 + "\n")
    
    successful = 0
    for table in tables:
        if export_table(conn, 'warehouse', table):
            successful += 1
    
    conn.close()
    
    print("\n" + "="*60)
    print(f"✓ Export completed: {successful}/{len(tables)} tables")
    print(f"📁 CSV files created in: {OUTPUT_DIR.absolute()}")
    print("="*60 + "\n")

if __name__ == '__main__':
    main()
