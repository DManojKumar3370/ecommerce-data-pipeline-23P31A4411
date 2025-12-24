"""
Complete ETL Pipeline - Staging → Production → Warehouse
"""

import sqlite3
import pandas as pd
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB_FILE = 'ecommerce.db'

class CompleteETLPipeline:
    def __init__(self):
        self.conn = None
        self.cursor = None
    
    def connect(self):
        try:
            self.conn = sqlite3.connect(DB_FILE)
            self.cursor = self.conn.cursor()
            logger.info(f"✓ Connected to {DB_FILE}")
            return True
        except Exception as e:
            logger.error(f"✗ Connection failed: {str(e)}")
            return False
    
    def load_staging(self):
        """Load CSV to staging"""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 1: LOADING STAGING")
        logger.info("=" * 60)
        
        try:
            self.cursor.execute("DELETE FROM staging_transaction_items")
            self.cursor.execute("DELETE FROM staging_transactions")
            self.cursor.execute("DELETE FROM staging_customers")
            self.cursor.execute("DELETE FROM staging_products")
            
            df_customers = pd.read_csv('data/raw/customers.csv')
            df_customers.to_sql('staging_customers', self.conn, if_exists='append', index=False)
            logger.info(f"✓ Loaded {len(df_customers)} customers")
            
            df_products = pd.read_csv('data/raw/products.csv')
            df_products.to_sql('staging_products', self.conn, if_exists='append', index=False)
            logger.info(f"✓ Loaded {len(df_products)} products")
            
            df_transactions = pd.read_csv('data/raw/transactions.csv')
            df_transactions.to_sql('staging_transactions', self.conn, if_exists='append', index=False)
            logger.info(f"✓ Loaded {len(df_transactions)} transactions")
            
            df_items = pd.read_csv('data/raw/transaction_items.csv')
            df_items.to_sql('staging_transaction_items', self.conn, if_exists='append', index=False)
            logger.info(f"✓ Loaded {len(df_items)} items")
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"✗ Staging failed: {str(e)}")
            self.conn.rollback()
            return False
    
    def transform_to_production(self):
        """Transform to production"""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 2: TRANSFORMING TO PRODUCTION")
        logger.info("=" * 60)
        
        try:
            self.cursor.execute("DELETE FROM production_transaction_items")
            self.cursor.execute("DELETE FROM production_transactions")
            self.cursor.execute("DELETE FROM production_customers")
            self.cursor.execute("DELETE FROM production_products")
            
            # Customers
            self.cursor.execute("""
            INSERT INTO production_customers 
            (customer_id, first_name, last_name, email, phone, registration_date, city, state, country, age_group)
            SELECT DISTINCT customer_id, first_name, last_name, email, phone, registration_date, city, state, country, age_group
            FROM staging_customers WHERE customer_id IS NOT NULL AND email IS NOT NULL
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} customers")
            
            # Products
            self.cursor.execute("""
            INSERT INTO production_products 
            (product_id, product_name, category, sub_category, price, cost, brand, stock_quantity, supplier_id)
            SELECT DISTINCT product_id, product_name, category, sub_category, price, cost, brand, stock_quantity, supplier_id
            FROM staging_products WHERE product_id IS NOT NULL AND price > 0
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} products")
            
            # Transactions
            self.cursor.execute("""
            INSERT INTO production_transactions 
            (transaction_id, customer_id, transaction_date, transaction_time, payment_method, shipping_address, total_amount)
            SELECT DISTINCT transaction_id, customer_id, transaction_date, transaction_time, payment_method, shipping_address, total_amount
            FROM staging_transactions WHERE transaction_id IS NOT NULL AND customer_id IS NOT NULL
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} transactions")
            
            # Items
            self.cursor.execute("""
            INSERT INTO production_transaction_items 
            (item_id, transaction_id, product_id, quantity, unit_price, discount_percentage, line_total)
            SELECT DISTINCT item_id, transaction_id, product_id, quantity, unit_price, discount_percentage, line_total
            FROM staging_transaction_items WHERE item_id IS NOT NULL AND quantity > 0
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} items")
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"✗ Production failed: {str(e)}")
            self.conn.rollback()
            return False
    
    def load_warehouse(self):
        """Load warehouse"""
        logger.info("\n" + "=" * 60)
        logger.info("STAGE 3: LOADING WAREHOUSE")
        logger.info("=" * 60)
        
        try:
            # Payment Methods
            methods = ['Credit Card', 'Debit Card', 'UPI', 'Cash on Delivery', 'Net Banking']
            for method in methods:
                self.cursor.execute(
                    "INSERT OR IGNORE INTO warehouse_dim_payment_method (payment_method_name, payment_type) VALUES (?, ?)",
                    (method, 'Online' if method != 'Cash on Delivery' else 'Offline')
                )
            self.conn.commit()
            logger.info(f"✓ Loaded {len(methods)} payment methods")
            
            # Date Dimension
            start = datetime(2024, 1, 1)
            end = datetime(2024, 12, 31)
            current = start
            dates = 0
            
            while current <= end:
                key = int(current.strftime('%Y%m%d'))
                self.cursor.execute("""
                INSERT OR IGNORE INTO warehouse_dim_date 
                (date_key, full_date, year, quarter, month, day, month_name, day_name, week_of_year, is_weekend)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    key, current.strftime('%Y-%m-%d'), current.year,
                    (current.month - 1) // 3 + 1, current.month, current.day,
                    current.strftime('%B'), current.strftime('%A'),
                    current.isocalendar()[1], 1 if current.weekday() >= 5 else 0
                ))
                current += timedelta(days=1)
                dates += 1
            
            self.conn.commit()
            logger.info(f"✓ Loaded {dates} date records")
            
            # Customers to Warehouse
            self.cursor.execute("""
            INSERT INTO warehouse_dim_customers 
            (customer_id, full_name, email, city, state, country, age_group, registration_date, effective_date, is_current)
            SELECT customer_id, first_name || ' ' || last_name, email, city, state, country, age_group, 
                   registration_date, date('now'), 1 FROM production_customers
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} customers")
            
            # Products to Warehouse
            self.cursor.execute("""
            INSERT INTO warehouse_dim_products 
            (product_id, product_name, category, sub_category, brand, effective_date, is_current)
            SELECT product_id, product_name, category, sub_category, brand, date('now'), 1 FROM production_products
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} products")
            
            # Fact Sales
            self.cursor.execute("""
            INSERT INTO warehouse_fact_sales 
            (date_key, customer_key, product_key, payment_method_key, transaction_id, quantity, unit_price, line_total, profit)
            SELECT 
                CAST(REPLACE(pt.transaction_date, '-', '') AS INTEGER),
                dc.customer_key, dp.product_key, dpm.payment_method_key,
                pti.transaction_id, pti.quantity, pti.unit_price, pti.line_total,
                ROUND(pti.line_total - (pti.quantity * (SELECT cost FROM production_products WHERE product_id = pti.product_id)), 2)
            FROM production_transaction_items pti
            JOIN production_transactions pt ON pti.transaction_id = pt.transaction_id
            JOIN warehouse_dim_customers dc ON pt.customer_id = dc.customer_id
            JOIN warehouse_dim_products dp ON pti.product_id = dp.product_id
            JOIN warehouse_dim_payment_method dpm ON pt.payment_method = dpm.payment_method_name
            """)
            logger.info(f"✓ Loaded {self.cursor.rowcount} fact records")
            
            self.conn.commit()
            return True
        except Exception as e:
            logger.error(f"✗ Warehouse failed: {str(e)}")
            self.conn.rollback()
            return False
    
    def run_pipeline(self):
        """Run complete pipeline"""
        logger.info("\n╔" + "=" * 58 + "╗")
        logger.info("║  COMPLETE ETL PIPELINE - SQLite                   ║")
        logger.info("╚" + "=" * 58 + "╝")
        
        if not self.connect():
            return False
        
        try:
            if not self.load_staging():
                return False
            if not self.transform_to_production():
                return False
            if not self.load_warehouse():
                return False
            
            logger.info("\n" + "=" * 60)
            logger.info("✓ ETL PIPELINE COMPLETE!")
            logger.info("=" * 60)
            return True
            
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    pipeline = CompleteETLPipeline()
    return pipeline.run_pipeline()

if __name__ == '__main__':
    main()
