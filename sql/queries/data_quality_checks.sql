-- Row counts
SELECT 'staging.customers' AS table_name, COUNT(*) AS row_count FROM staging.customers;
SELECT 'staging.products' AS table_name, COUNT(*) AS row_count FROM staging.products;
SELECT 'staging.transactions' AS table_name, COUNT(*) AS row_count FROM staging.transactions;
SELECT 'staging.transaction_items' AS table_name, COUNT(*) AS row_count FROM staging.transaction_items;

-- Completeness (nulls in key columns)
SELECT 'customers.customer_id' AS column_name, COUNT(*) AS null_count
FROM staging.customers
WHERE customer_id IS NULL;

SELECT 'transactions.transaction_id' AS column_name, COUNT(*) AS null_count
FROM staging.transactions
WHERE transaction_id IS NULL;

-- Range checks (check_data_ranges)
SELECT 'transaction_items.quantity' AS check_name, COUNT(*) AS invalid_count
FROM staging.transaction_items
WHERE quantity <= 0;

SELECT 'transaction_items.unit_price' AS check_name, COUNT(*) AS invalid_count
FROM staging.transaction_items
WHERE unit_price <= 0;

-- Referential integrity
SELECT COUNT(*) AS orphan_transactions
FROM staging.transaction_items ti
LEFT JOIN staging.transactions t ON ti.transaction_id = t.transaction_id
WHERE t.transaction_id IS NULL;

SELECT COUNT(*) AS orphan_products
FROM staging.transaction_items ti
LEFT JOIN staging.products p ON ti.product_id = p.product_id
WHERE p.product_id IS NULL;
