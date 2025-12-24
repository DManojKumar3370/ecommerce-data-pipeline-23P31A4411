-- SQLite Schemas

-- DROP EXISTING TABLES (for re-runs)
DROP TABLE IF EXISTS staging_transaction_items;
DROP TABLE IF EXISTS staging_transactions;
DROP TABLE IF EXISTS staging_customers;
DROP TABLE IF EXISTS staging_products;
DROP TABLE IF EXISTS production_transaction_items;
DROP TABLE IF EXISTS production_transactions;
DROP TABLE IF EXISTS production_customers;
DROP TABLE IF EXISTS production_products;

-- STAGING TABLES
CREATE TABLE staging_customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    phone TEXT,
    registration_date TEXT,
    city TEXT,
    state TEXT,
    country TEXT,
    age_group TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    sub_category TEXT,
    price REAL,
    cost REAL,
    brand TEXT,
    stock_quantity INTEGER,
    supplier_id TEXT,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging_transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT,
    transaction_date TEXT,
    transaction_time TEXT,
    payment_method TEXT,
    shipping_address TEXT,
    total_amount REAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE staging_transaction_items (
    item_id TEXT PRIMARY KEY,
    transaction_id TEXT,
    product_id TEXT,
    quantity INTEGER,
    unit_price REAL,
    discount_percentage REAL,
    line_total REAL,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- PRODUCTION TABLES
CREATE TABLE production_customers (
    customer_id TEXT PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    phone TEXT,
    registration_date TEXT NOT NULL,
    city TEXT,
    state TEXT,
    country TEXT,
    age_group TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production_products (
    product_id TEXT PRIMARY KEY,
    product_name TEXT NOT NULL,
    category TEXT NOT NULL,
    sub_category TEXT,
    price REAL NOT NULL CHECK (price > 0),
    cost REAL NOT NULL CHECK (cost > 0),
    brand TEXT,
    stock_quantity INTEGER CHECK (stock_quantity >= 0),
    supplier_id TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE production_transactions (
    transaction_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    transaction_date TEXT NOT NULL,
    transaction_time TEXT,
    payment_method TEXT NOT NULL,
    shipping_address TEXT,
    total_amount REAL NOT NULL CHECK (total_amount >= 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (customer_id) REFERENCES production_customers(customer_id)
);

CREATE TABLE production_transaction_items (
    item_id TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL,
    product_id TEXT NOT NULL,
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    unit_price REAL NOT NULL CHECK (unit_price > 0),
    discount_percentage REAL CHECK (discount_percentage >= 0 AND discount_percentage <= 100),
    line_total REAL NOT NULL CHECK (line_total > 0),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (transaction_id) REFERENCES production_transactions(transaction_id),
    FOREIGN KEY (product_id) REFERENCES production_products(product_id)
);
