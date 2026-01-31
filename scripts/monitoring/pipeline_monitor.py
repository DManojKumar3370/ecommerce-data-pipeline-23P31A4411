import psycopg2
from datetime import datetime

def get_connection():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="postgres",
        password="postgres",
    )

def main():
    conn = get_connection()
    cur = conn.cursor()

    # Row count in fact_sales
    cur.execute("SELECT COUNT(*) FROM warehouse.fact_sales;")
    fact_count, = cur.fetchone()

    # Latest transaction_date
    cur.execute("SELECT MAX(transaction_date) FROM warehouse.fact_sales;")
    latest_date, = cur.fetchone()

    cur.close()
    conn.close()

    print(f"[{datetime.utcnow()}] fact_sales row count: {fact_count}")
    print(f"[{datetime.utcnow()}] latest transaction_date: {latest_date}")

if __name__ == '__main__':
    main()
