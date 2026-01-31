CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.customers (
    customer_id      BIGINT PRIMARY KEY,
    first_name       TEXT,
    last_name        TEXT,
    email            TEXT,
    signup_date      DATE,
    country          TEXT
);

CREATE TABLE IF NOT EXISTS staging.products (
    product_id   BIGINT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    unit_price   NUMERIC(10,2),
    cost_price   NUMERIC(10,2),
    is_active    BOOLEAN
);

CREATE TABLE IF NOT EXISTS staging.transactions (
    transaction_id   BIGINT PRIMARY KEY,
    customer_id      BIGINT,
    transaction_date TIMESTAMP,
    payment_method   TEXT,
    status           TEXT
);

CREATE TABLE IF NOT EXISTS staging.transaction_items (
    transaction_item_id BIGINT PRIMARY KEY,
    transaction_id      BIGINT,
    product_id          BIGINT,
    quantity            INTEGER,
    unit_price          NUMERIC(10,2),
    currency            TEXT
);
