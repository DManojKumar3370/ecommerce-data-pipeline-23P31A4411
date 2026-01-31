import logging
from datetime import datetime
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_connection():
    # TODO: replace with values from your config / .env if needed
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="postgres",
        password="postgres",
    )


def upsert_dim_customers(conn):
    """
    Simple SCD Type 2 upsert from staging.customers -> warehouse.dim_customers
    Assumes dim_customers has columns:
    - customer_key (serial PK)
    - customer_id
    - first_name, last_name, email, country
    - effective_from, effective_to, is_current
    """
    cur = conn.cursor()

    # Close existing current records if anything changed
    cur.execute(
        """
        UPDATE warehouse.dim_customers d
        SET effective_to = NOW(), is_current = FALSE
        FROM staging.customers s
        WHERE d.customer_id = s.customer_id
          AND d.is_current = TRUE
          AND (
              d.first_name <> s.first_name OR
              d.last_name  <> s.last_name  OR
              d.email      <> s.email      OR
              d.country    <> s.country
          );
        """
    )

    # Insert new current records (for new or changed customers)
    cur.execute(
        """
        INSERT INTO warehouse.dim_customers
            (customer_id, first_name, last_name, email, country,
             effective_from, effective_to, is_current)
        SELECT
            s.customer_id,
            s.first_name,
            s.last_name,
            s.email,
            s.country,
            NOW(),
            '9999-12-31',
            TRUE
        FROM staging.customers s
        LEFT JOIN warehouse.dim_customers d
          ON d.customer_id = s.customer_id
         AND d.is_current = TRUE
        WHERE d.customer_id IS NULL
           OR d.first_name <> s.first_name
           OR d.last_name  <> s.last_name
           OR d.email      <> s.email
           OR d.country    <> s.country;
        """
    )

    cur.close()


def upsert_dim_products(conn):
    """
    Simple SCD Type 2 upsert from staging.products -> warehouse.dim_products
    Assumes dim_products has columns:
    - product_key (serial PK)
    - product_id
    - product_name, category, unit_price
    - effective_from, effective_to, is_current
    """
    cur = conn.cursor()

    cur.execute(
        """
        UPDATE warehouse.dim_products d
        SET effective_to = NOW(), is_current = FALSE
        FROM staging.products s
        WHERE d.product_id = s.product_id
          AND d.is_current = TRUE
          AND (
              d.product_name <> s.product_name OR
              d.category     <> s.category     OR
              d.unit_price   <> s.unit_price
          );
        """
    )

    cur.execute(
        """
        INSERT INTO warehouse.dim_products
            (product_id, product_name, category, unit_price,
             effective_from, effective_to, is_current)
        SELECT
            s.product_id,
            s.product_name,
            s.category,
            s.unit_price,
            NOW(),
            '9999-12-31',
            TRUE
        FROM staging.products s
        LEFT JOIN warehouse.dim_products d
          ON d.product_id = s.product_id
         AND d.is_current = TRUE
        WHERE d.product_id   IS NULL
           OR d.product_name <> s.product_name
           OR d.category     <> s.category
           OR d.unit_price   <> s.unit_price;
        """
    )

    cur.close()


def load_fact_sales(conn):
    """
    Load fact_sales from staging.transactions + staging.transaction_items.
    Assumes fact_sales has columns at least:
    - transaction_id
    - transaction_item_id
    - customer_id
    - product_id
    - transaction_date
    - quantity
    - unit_price
    - total_amount
    - currency
    - status
    - payment_method
    """
    cur = conn.cursor()

    cur.execute(
        """
        INSERT INTO warehouse.fact_sales (
            transaction_id,
            transaction_item_id,
            customer_id,
            product_id,
            transaction_date,
            quantity,
            unit_price,
            total_amount,
            currency,
            status,
            payment_method
        )
        SELECT
            t.transaction_id,
            ti.transaction_item_id,
            t.customer_id,
            ti.product_id,
            t.transaction_date,
            ti.quantity,
            ti.unit_price,
            ti.quantity * ti.unit_price AS total_amount,
            ti.currency,
            t.status,
            t.payment_method
        FROM staging.transaction_items ti
        JOIN staging.transactions t
          ON ti.transaction_id = t.transaction_id
        ON CONFLICT (transaction_item_id) DO NOTHING;
        """
    )

    cur.close()


def main():
    logger.info("Starting transformation from staging to warehouse...")
    conn = get_connection()
    try:
        upsert_dim_customers(conn)
        upsert_dim_products(conn)
        load_fact_sales(conn)
        conn.commit()
        logger.info("Transformation completed successfully.")
    except Exception as e:
        conn.rollback()
        logger.exception("Error during transformation")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
