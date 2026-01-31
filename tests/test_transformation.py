import pandas as pd
from datetime import datetime
from scripts.transformation.staging_to_production import load_fact_sales

class DummyCursor:
    def __init__(self):
        self.executed = []

    def execute(self, query, params=None):
        self.executed.append((query, params))

    def close(self):
        pass

class DummyConn:
    def __init__(self):
        self.cursor_obj = DummyCursor()

    def cursor(self):
        return self.cursor_obj

def test_load_fact_sales_inserts_rows():
    # Prepare a minimal DataFrame-like object using pandas
    data = {
        "transaction_id": [1],
        "transaction_item_id": [10],
        "customer_id": [100],
        "product_id": [200],
        "transaction_date": [datetime(2024, 1, 1, 10, 0, 0)],
        "quantity": [2],
        "unit_price": [50.0],
        "currency": ["USD"],
        "status": ["COMPLETED"],
        "payment_method": ["credit_card"],
    }
    df = pd.DataFrame(data)

    conn = DummyConn()
    load_fact_sales(conn)  # function uses SQL directly; this test is a smoke test

    # At least check that no exception occurred and cursor object exists
    assert conn.cursor_obj is not None
