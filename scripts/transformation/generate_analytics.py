"""
Execute analytical queries and export results
"""

import sqlite3
import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'ecommerce.db'

def execute_analytics():
    """Execute all analytical queries"""
    logger.info("=" * 60)
    logger.info("Executing Analytical Queries")
    logger.info("=" * 60)
    
    try:
        conn = sqlite3.connect(DB_FILE)
        Path('data/processed/analytics').mkdir(parents=True, exist_ok=True)
        
        # Read all queries from SQL file
        with open('sql/queries/analytical_queries.sql', 'r') as f:
            queries_text = f.read()
        
        # Split queries (each starts with --)
        queries = [q.strip() for q in queries_text.split('--') if q.strip() and 'QUERY' in q]
        
        query_count = 1
        for query_block in queries_text.split('-- QUERY'):
            if not query_block.strip():
                continue
            
            # Extract query
            lines = query_block.split('\n')
            query = '\n'.join([l for l in lines if l and not l.startswith('--')])
            
            if not query.strip():
                continue
            
            try:
                df = pd.read_sql_query(query, conn)
                
                # Save to CSV
                filename = f'data/processed/analytics/query_{query_count:02d}.csv'
                df.to_csv(filename, index=False)
                logger.info(f"✓ Query {query_count} executed: {len(df)} rows")
                query_count += 1
            except Exception as e:
                logger.warning(f"✗ Query {query_count} failed: {str(e)}")
                query_count += 1
        
        conn.close()
        logger.info("=" * 60)
        logger.info(f"✓ All {query_count-1} queries completed!")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")

if __name__ == '__main__':
    execute_analytics()
