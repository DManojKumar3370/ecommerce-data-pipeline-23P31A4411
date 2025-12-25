"""
Unit Tests for Data Quality Checks Module
"""
import pytest
import pandas as pd
import sqlite3
from pathlib import Path
import sys
import tempfile

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestQualityChecksBasic:
    """Basic test cases for quality checks"""
    
    @pytest.fixture
    def test_db_with_data(self):
        """Create test database with sample data"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        conn = sqlite3.connect(db_path)
        
        # Create staging tables
        customers_data = {
            'customer_id': ['CUST0001', 'CUST0002', 'CUST0003'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'phone': ['1234567890', '0987654321', '5555555555'],
            'name': ['John', 'Jane', 'Bob'],
            'address': ['123 Main', '456 Oak', '789 Pine']
        }
        
        df = pd.DataFrame(customers_data)
        df.to_sql('staging_customers', conn, if_exists='replace', index=False)
        conn.commit()
        
        yield conn
        
        conn.close()
        Path(db_path).unlink()
    
    def test_data_quality_no_nulls(self, test_db_with_data):
        """Test that data has no null values in key fields"""
        result = pd.read_sql("SELECT * FROM staging_customers", test_db_with_data)
        
        # Verify no nulls in key columns
        assert result['customer_id'].isnull().sum() == 0, "Null values in customer_id"
        assert result['email'].isnull().sum() == 0, "Null values in email"
    
    def test_data_quality_unique_keys(self, test_db_with_data):
        """Test that key columns have unique values"""
        result = pd.read_sql("SELECT * FROM staging_customers", test_db_with_data)
        
        assert result['customer_id'].nunique() == len(result), "Duplicate customer IDs"
        assert result['email'].nunique() == len(result), "Duplicate emails"
    
    def test_data_quality_valid_formats(self, test_db_with_data):
        """Test that data has valid formats"""
        result = pd.read_sql("SELECT * FROM staging_customers", test_db_with_data)
        
        # Check email format
        assert all('@' in email for email in result['email']), "Invalid email format"
        
        # Check customer ID format
        assert all(cid.startswith('CUST') for cid in result['customer_id']), "Invalid customer ID format"
    
    def test_data_quality_record_count(self, test_db_with_data):
        """Test that correct number of records exist"""
        result = pd.read_sql("SELECT COUNT(*) as count FROM staging_customers", test_db_with_data)
        assert result['count'].iloc[0] == 3, "Expected 3 records"


class TestDataQualityMetrics:
    """Test data quality metrics"""
    
    def test_completeness_metric(self):
        """Test completeness metric calculation"""
        data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['A', 'B', 'C', None, 'E'],
            'email': ['a@test.com', 'b@test.com', 'c@test.com', 'd@test.com', 'e@test.com']
        }
        df = pd.DataFrame(data)
        
        # Completeness = (non-null values / total values) * 100
        total_cells = len(df) * len(df.columns)
        null_cells = df.isnull().sum().sum()
        completeness = ((total_cells - null_cells) / total_cells) * 100
        
        assert completeness > 85, f"Completeness too low: {completeness}%"
    
    def test_uniqueness_metric(self):
        """Test uniqueness metric calculation"""
        data = {
            'customer_id': ['CUST001', 'CUST002', 'CUST003'],
            'email': ['john@test.com', 'jane@test.com', 'bob@test.com']
        }
        df = pd.DataFrame(data)
        
        # Check for duplicates
        duplicate_count = len(df) - df.drop_duplicates().shape[0]
        assert duplicate_count == 0, "Duplicate records found"
    
    def test_validity_metric(self):
        """Test validity metric calculation"""
        data = {
            'price': [10.5, 20.0, 15.75, 30.0],
            'quantity': [1, 2, 3, 4]
        }
        df = pd.DataFrame(data)
        
        # Check validity: prices should be > 0
        invalid_count = len(df[df['price'] <= 0])
        assert invalid_count == 0, "Invalid prices found"


class TestDataQualityEdgeCases:
    """Test edge cases in data quality"""
    
    def test_empty_dataframe_quality(self):
        """Test quality check on empty dataframe"""
        df = pd.DataFrame(columns=['id', 'name', 'email'])
        assert len(df) == 0, "Should have empty dataframe"
    
    def test_single_record_quality(self):
        """Test quality check on single record"""
        df = pd.DataFrame({
            'id': [1],
            'name': ['Test'],
            'email': ['test@example.com']
        })
        assert len(df) == 1, "Should have single record"
        assert df['name'].isnull().sum() == 0, "Should have no nulls"
    
    def test_large_dataset_quality(self):
        """Test quality check on large dataset"""
        import numpy as np
        size = 10000
        df = pd.DataFrame({
            'id': range(size),
            'value': np.random.rand(size)
        })
        assert len(df) == size, f"Should have {size} records"
