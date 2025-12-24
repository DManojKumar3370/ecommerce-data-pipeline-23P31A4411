CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id VARCHAR(20) PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    registration_date DATE,
    city VARCHAR(100),
    state VARCHAR(100),
    country VARCHAR(100),
    age_group VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    sub_category VARCHAR(100),
    price DECIMAL(12, 2),
    cost DECIMAL(12, 2),
    brand VARCHAR(100),
    stock_quantity INTEGER,
    supplier_id VARCHAR(20),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id VARCHAR(20) PRIMARY KEY,
    customer_id VARCHAR(20),
    transaction_date DATE,
    transaction_time TIME,
    payment_method VARCHAR(50),
    shipping_address VARCHAR(500),
    total_amount DECIMAL(12, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    item_id VARCHAR(20) PRIMARY KEY,
    transaction_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INTEGER,
    unit_price DECIMAL(12, 2),
    discount_percentage DECIMAL(5, 2),
    line_total DECIMAL(12, 2),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_staging_transactions_customer ON staging.transactions(customer_id);
CREATE INDEX IF NOT EXISTS idx_staging_items_transaction ON staging.transaction_items(transaction_id);
CREATE INDEX IF NOT EXISTS idx_staging_items_product ON staging.transaction_items(product_id);
