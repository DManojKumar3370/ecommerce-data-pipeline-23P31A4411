

import sqlite3
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'ecommerce.db'

def setup_sqlite():
    """Setup SQLite database"""
    logger.info("=" * 60)
    logger.info("Setting up SQLite Database")
    logger.info("=" * 60)
    
    try:
        # Connect to SQLite
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        logger.info(f"✓ Connected to {DB_FILE}")
        
        # Read and execute schema file
        with open('sql/ddl/create_schemas_sqlite.sql', 'r') as f:
            schema = f.read()
        
        cursor.executescript(schema)
        conn.commit()
        
        logger.info("✓ All tables created successfully!")
        logger.info("=" * 60)
        
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"✗ Setup failed: {str(e)}")
        return False

if __name__ == '__main__':
    setup_sqlite()
