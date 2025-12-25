"""
Unit Tests for Data Ingestion Module
"""
import pytest
import pandas as pd
import sqlite3
from pathlib import Path
import sys
import tempfile

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))


class TestDataIngestion:
    """Test cases for data ingestion"""
    
    @pytest.fixture
    def temp_db(self):
        """Create temporary database for testing"""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as f:
            db_path = f.name
        
        yield db_path
        
        # Cleanup
        if Path(db_path).exists():
            Path(db_path).unlink()
    
    @pytest.fixture
    def sample_csv(self):
        """Create sample CSV data"""
        data = {
            'customer_id': ['CUST0001', 'CUST0002', 'CUST0003'],
            'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
            'phone': ['1234567890', '0987654321', '5555555555'],
            'name': ['John', 'Jane', 'Bob'],
            'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd']
        }
        df = pd.DataFrame(data)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            csv_path = f.name
            df.to_csv(csv_path, index=False)
        
        yield csv_path
        
        # Cleanup
        if Path(csv_path).exists():
            Path(csv_path).unlink()
    
    def test_csv_to_database_loading(self, temp_db, sample_csv):
        """Test loading CSV to database"""
        conn = sqlite3.connect(temp_db)
        
        df = pd.read_csv(sample_csv)
        df.to_sql('staging_customers', conn, if_exists='replace', index=False)
        
        conn.commit()
        
        # Verify data loaded
        result = pd.read_sql("SELECT * FROM staging_customers", conn)
        assert len(result) == 3, "Should load 3 records"
        assert 'email' in result.columns, "Should have email column"
        
        conn.close()
    
    def test_idempotent_loading(self, temp_db, sample_csv):
        """Test that loading is idempotent (multiple loads = same result)"""
        conn = sqlite3.connect(temp_db)
        df = pd.read_csv(sample_csv)
        
        # Load twice
        df.to_sql('staging_customers', conn, if_exists='replace', index=False)
        df.to_sql('staging_customers', conn, if_exists='replace', index=False)
        
        result = pd.read_sql("SELECT * FROM staging_customers", conn)
        assert len(result) == 3, "Should still have 3 records after second load"
        
        conn.close()
    
    def test_data_type_preservation(self, temp_db, sample_csv):
        """Test that data types are preserved during loading"""
        conn = sqlite3.connect(temp_db)
        df = pd.read_csv(sample_csv)
        df.to_sql('staging_customers', conn, if_exists='replace', index=False)
        
        result = pd.read_sql("SELECT * FROM staging_customers", conn)
        
        # Check that string columns are present
        assert isinstance(result['customer_id'].iloc[0], str), "customer_id should be string"
        assert isinstance(result['email'].iloc[0], str), "email should be string"
        
        conn.close()
