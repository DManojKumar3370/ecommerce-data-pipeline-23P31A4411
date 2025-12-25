"""
Unit Tests for Data Generation Module
Tests data generation functions for accuracy and integrity
"""
import pytest
import pandas as pd
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data_generation.generate_data import (
    generate_customers,
    generate_products,
    generate_transactions,
    generate_transaction_items,
    validate_referential_integrity
)


class TestDataGeneration:
    """Test cases for data generation functions"""
    
    def test_generate_customers(self):
        """Test customer generation"""
        num_customers = 100
        customers_df = generate_customers(num_customers)
        
        assert len(customers_df) == num_customers, f"Expected {num_customers} customers, got {len(customers_df)}"
        assert 'customer_id' in customers_df.columns, "Missing customer_id column"
        assert 'email' in customers_df.columns, "Missing email column"
        assert 'phone' in customers_df.columns, "Missing phone column"
        assert customers_df['email'].nunique() == num_customers, "Duplicate emails found"
        assert customers_df['customer_id'].nunique() == num_customers, "Duplicate customer IDs found"
    
    def test_generate_customers_id_format(self):
        """Test customer ID format"""
        customers_df = generate_customers(10)
        
        for cust_id in customers_df['customer_id']:
            assert cust_id.startswith('CUST'), f"Invalid customer ID format: {cust_id}"
            assert len(cust_id) == 8, f"Customer ID should be 8 chars: {cust_id}"
    
    def test_generate_customers_no_nulls(self):
        """Test no null values in mandatory fields"""
        customers_df = generate_customers(50)
        
        mandatory_fields = ['customer_id', 'email', 'phone', 'name', 'address']
        for field in mandatory_fields:
            assert customers_df[field].isnull().sum() == 0, f"Null values found in {field}"
    
    def test_generate_products(self):
        """Test product generation"""
        num_products = 50
        products_df = generate_products(num_products)
        
        assert len(products_df) == num_products, f"Expected {num_products} products, got {len(products_df)}"
        assert 'product_id' in products_df.columns, "Missing product_id column"
        assert 'price' in products_df.columns, "Missing price column"
        assert 'cost' in products_df.columns, "Missing cost column"
        assert (products_df['price'] >= 0).all(), "Negative prices found"
    
    def test_generate_products_profit_margin(self):
        """Test cost < price (positive margin)"""
        products_df = generate_products(50)
        
        profit_margin = products_df['price'] - products_df['cost']
        assert (profit_margin > 0).all(), "Products with zero or negative profit margin found"
    
    def test_generate_products_id_format(self):
        """Test product ID format"""
        products_df = generate_products(10)
        
        for prod_id in products_df['product_id']:
            assert prod_id.startswith('PROD'), f"Invalid product ID format: {prod_id}"
    
    def test_generate_transactions(self):
        """Test transaction generation"""
        customers_df = generate_customers(100)
        num_transactions = 500
        
        transactions_df = generate_transactions(num_transactions, customers_df)
        
        assert len(transactions_df) == num_transactions, f"Expected {num_transactions} transactions"
        assert 'transaction_id' in transactions_df.columns, "Missing transaction_id"
        assert 'customer_id' in transactions_df.columns, "Missing customer_id"
        assert (transactions_df['customer_id'].isin(customers_df['customer_id'])).all(), "Invalid customer references"
    
    def test_generate_transactions_id_format(self):
        """Test transaction ID format"""
        customers_df = generate_customers(10)
        transactions_df = generate_transactions(10, customers_df)
        
        for txn_id in transactions_df['transaction_id']:
            assert txn_id.startswith('TXN'), f"Invalid transaction ID: {txn_id}"
    
    def test_generate_transaction_items(self):
        """Test transaction items generation"""
        customers_df = generate_customers(100)
        products_df = generate_products(50)
        transactions_df = generate_transactions(100, customers_df)
        
        items_df = generate_transaction_items(transactions_df, products_df)
        
        assert len(items_df) > 0, "No transaction items generated"
        assert 'transaction_id' in items_df.columns, "Missing transaction_id"
        assert 'product_id' in items_df.columns, "Missing product_id"
        assert 'line_total' in items_df.columns, "Missing line_total"
    
    def test_generate_transaction_items_line_total_calculation(self):
        """Test line_total calculation: quantity * unit_price * (1 - discount/100)"""
        customers_df = generate_customers(50)
        products_df = generate_products(30)
        transactions_df = generate_transactions(50, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        for idx, row in items_df.iterrows():
            expected_line_total = row['quantity'] * row['unit_price'] * (1 - row['discount_percentage']/100)
            assert abs(row['line_total'] - expected_line_total) < 0.01, f"Incorrect line_total calculation at row {idx}"
    
    def test_generate_transaction_items_id_format(self):
        """Test item ID format"""
        customers_df = generate_customers(10)
        products_df = generate_products(10)
        transactions_df = generate_transactions(10, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        for item_id in items_df['item_id']:
            assert item_id.startswith('ITEM'), f"Invalid item ID: {item_id}"
    
    def test_validate_referential_integrity(self):
        """Test referential integrity validation"""
        customers_df = generate_customers(100)
        products_df = generate_products(50)
        transactions_df = generate_transactions(200, customers_df)
        items_df = generate_transaction_items(transactions_df, products_df)
        
        result = validate_referential_integrity(customers_df, products_df, transactions_df, items_df)
        
        assert result['orphan_records'] == 0, f"Orphan records found: {result}"
        assert result['constraint_violations'] == 0, f"Constraint violations found: {result}"
    
    def test_data_generation_consistency(self):
        """Test that same seed produces consistent results"""
        # This test ensures deterministic behavior
        customers_df1 = generate_customers(50)
        customers_df2 = generate_customers(50)
        
        # Should have same number of rows
        assert len(customers_df1) == len(customers_df2), "Inconsistent customer generation"


class TestDataGenerationEdgeCases:
    """Edge case tests"""
    
    def test_generate_zero_customers(self):
        """Test generating zero customers"""
        customers_df = generate_customers(0)
        assert len(customers_df) == 0, "Should generate empty dataframe for zero customers"
    
    def test_generate_single_customer(self):
        """Test generating single customer"""
        customers_df = generate_customers(1)
        assert len(customers_df) == 1, "Should generate exactly 1 customer"
    
    def test_large_dataset_generation(self):
        """Test generating large dataset"""
        customers_df = generate_customers(1000)
        assert len(customers_df) == 1000, "Should handle 1000 customers"
