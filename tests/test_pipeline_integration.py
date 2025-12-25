"""
Integration Tests for End-to-End Pipeline
Tests complete pipeline execution from data generation to warehouse loading
"""
import pytest
import pandas as pd
import sqlite3
from pathlib import Path
import sys
import tempfile
import shutil

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_generation.generate_data import (
    generate_customers, generate_products, generate_transactions,
    generate_transaction_items, validate_referential_integrity
)


class TestPipelineIntegration:
    """Integration tests for complete pipeline"""
    
    @pytest.fixture
    def temp_project_dir(self):
        """Create temporary project directory for integration testing"""
        temp_dir = tempfile.mkdtemp()
        
        # Create data directories
        (Path(temp_dir) / "data" / "raw").mkdir(parents=True, exist_ok=True)
        (Path(temp_dir) / "data" / "staging").mkdir(parents=True, exist_ok=True)
        (Path(temp_dir) / "data" / "processed").mkdir(parents=True, exist_ok=True)
        (Path(temp_dir) / "logs").mkdir(parents=True, exist_ok=True)
        
        yield Path(temp_dir)
        
        # Cleanup
        shutil.rmtree(temp_dir)
    
    def test_complete_data_generation_pipeline(self):
        """Test complete data generation pipeline"""
        num_customers = 100
        num_products = 50
        num_transactions = 200
        
        customers_df = generate_customers(num_customers)
        products_df = generate_products(num_products)
        transactions_df = generate_transactions(num_transactions, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        # Verify generated data
        assert len(customers_df) == num_customers
        assert len(products_df) == num_products
        assert len(transactions_df) == num_transactions
        assert len(items_df) > num_transactions  # Multiple items per transaction
        
        # Verify relationships
        result = validate_referential_integrity(customers_df, products_df, transactions_df, items_df)
        assert result['orphan_records'] == 0, "Referential integrity violated"
    
    def test_csv_save_and_load_cycle(self, temp_project_dir):
        """Test saving data to CSV and loading back"""
        customers_df = generate_customers(50)
        products_df = generate_products(25)
        
        raw_dir = temp_project_dir / "data" / "raw"
        
        # Save to CSV
        customers_df.to_csv(raw_dir / "customers.csv", index=False)
        products_df.to_csv(raw_dir / "products.csv", index=False)
        
        # Load back
        loaded_customers = pd.read_csv(raw_dir / "customers.csv")
        loaded_products = pd.read_csv(raw_dir / "products.csv")
        
        # Verify
        assert len(loaded_customers) == len(customers_df)
        assert len(loaded_products) == len(products_df)
        assert list(loaded_customers.columns) == list(customers_df.columns)
    
    def test_database_staging_layer_integration(self, temp_project_dir):
        """Test database staging layer integration"""
        db_path = temp_project_dir / "test_ecommerce.db"
        conn = sqlite3.connect(str(db_path))
        
        # Generate and load data
        customers_df = generate_customers(100)
        customers_df.to_sql("staging_customers", conn, if_exists='replace', index=False)
        
        conn.commit()
        
        # Verify data in database
        result = pd.read_sql("SELECT COUNT(*) as count FROM staging_customers", conn)
        assert result['count'].iloc[0] == 100, "Data not loaded correctly"
        
        # Test production layer
        result.to_sql("production_customers", conn, if_exists='replace', index=False)
        prod_result = pd.read_sql("SELECT * FROM production_customers", conn)
        assert len(prod_result) > 0, "Production layer empty"
        
        conn.close()
    
    def test_data_quality_in_generated_data(self):
        """Test that generated data meets quality standards"""
        customers_df = generate_customers(200)
        
        # No null values in key fields
        assert customers_df['customer_id'].isnull().sum() == 0
        assert customers_df['email'].isnull().sum() == 0
        
        # Unique emails and IDs
        assert customers_df['customer_id'].nunique() == len(customers_df)
        assert customers_df['email'].nunique() == len(customers_df)
        
        # Valid data formats
        assert all(cust_id.startswith('CUST') for cust_id in customers_df['customer_id'])
        assert '@' in customers_df['email'].iloc[0]
    
    def test_transaction_integrity(self):
        """Test transaction and item integrity"""
        customers_df = generate_customers(100)
        products_df = generate_products(50)
        transactions_df = generate_transactions(300, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        # All transaction items reference valid transactions
        valid_txns = set(transactions_df['transaction_id'])
        item_txns = set(items_df['transaction_id'])
        assert item_txns.issubset(valid_txns), "Items reference non-existent transactions"
        
        # All items reference valid products
        valid_prods = set(products_df['product_id'])
        item_prods = set(items_df['product_id'])
        assert item_prods.issubset(valid_prods), "Items reference non-existent products"
        
        # Line totals are calculated correctly
        for idx, row in items_df.iterrows():
            expected = row['quantity'] * row['unit_price'] * (1 - row['discount_percentage']/100)
            assert abs(row['line_total'] - expected) < 0.01, f"Incorrect line_total at row {idx}"


class TestPipelineRobustness:
    """Test pipeline robustness and error handling"""
    
    def test_large_dataset_handling(self):
        """Test handling of large datasets"""
        customers_df = generate_customers(1000)
        products_df = generate_products(500)
        transactions_df = generate_transactions(5000, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        assert len(customers_df) == 1000
        assert len(products_df) == 500
        assert len(transactions_df) == 5000
        assert len(items_df) > 5000
    
    def test_empty_dataset_handling(self):
        """Test handling of empty datasets"""
        customers_df = generate_customers(0)
        assert len(customers_df) == 0
    
    def test_data_consistency_across_generations(self):
        """Test data consistency across multiple generations"""
        gen1_customers = generate_customers(100)
        gen2_customers = generate_customers(100)
        
        # Both should have same number of records
        assert len(gen1_customers) == len(gen2_customers)
        # Both should have required columns
        assert set(gen1_customers.columns) == set(gen2_customers.columns)
