-- Row counts
SELECT 'warehouse.dim_customers' AS table_name, COUNT(*) AS row_count
FROM warehouse.dim_customers;

SELECT 'warehouse.dim_products' AS table_name, COUNT(*) AS row_count
FROM warehouse.dim_products;

SELECT 'warehouse.fact_sales' AS table_name, COUNT(*) AS row_count
FROM warehouse.fact_sales;

-- Freshness
SELECT MAX(transaction_date) AS latest_transaction_date
FROM warehouse.fact_sales;
