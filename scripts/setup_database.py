"""
Setup Database Schemas using Python (No psql needed!)
"""

import psycopg2
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Database connection details
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'ecommerce_db'
DB_USER = 'admin'
DB_PASSWORD = 'password'


def connect_to_db():
    """Connect to PostgreSQL database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        logger.info(f"✓ Connected to {DB_HOST}:{DB_PORT}/{DB_NAME}")
        return conn
    except psycopg2.Error as e:
        logger.error(f"✗ Connection failed: {str(e)}")
        return None


def execute_sql_file(conn, file_path):
    """Execute SQL file"""
    try:
        with open(file_path, 'r') as f:
            sql_script = f.read()
        
        cursor = conn.cursor()
        cursor.execute(sql_script)
        conn.commit()
        cursor.close()
        
        logger.info(f"✓ Executed: {file_path}")
        return True
    except Exception as e:
        logger.error(f"✗ Failed to execute {file_path}: {str(e)}")
        conn.rollback()
        return False


def setup_schemas():
    """Setup all database schemas"""
    logger.info("=" * 60)
    logger.info("Setting up Database Schemas")
    logger.info("=" * 60)
    
    # Connect to database
    conn = connect_to_db()
    if not conn:
        return False
    
    try:
        # Execute schema files in order
        schema_files = [
            'sql/ddl/create_staging_schema.sql',
            'sql/ddl/create_production_schema.sql',
            'sql/ddl/create_warehouse_schema.sql'
        ]
        
        for schema_file in schema_files:
            if Path(schema_file).exists():
                if not execute_sql_file(conn, schema_file):
                    logger.error(f"Failed to execute {schema_file}")
                    return False
            else:
                logger.warning(f"File not found: {schema_file}")
        
        logger.info("=" * 60)
        logger.info("✓ All schemas created successfully!")
        logger.info("=" * 60)
        return True
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        return False
    finally:
        conn.close()


if __name__ == '__main__':
    setup_schemas()
