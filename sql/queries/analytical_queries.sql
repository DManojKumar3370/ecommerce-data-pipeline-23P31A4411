-- ============================================================
-- ANALYTICAL QUERIES - Business Intelligence Layer
-- All queries use warehouse schema for analytics
-- ============================================================

-- QUERY 1: Top 10 Products by Revenue
-- Identifies best-selling products
SELECT 
    dp.product_name,
    dp.category,
    COUNT(DISTINCT fs.sales_key) as times_sold,
    ROUND(SUM(fs.line_total), 2) as total_revenue,
    ROUND(SUM(fs.quantity), 0) as units_sold,
    ROUND(AVG(fs.unit_price), 2) as avg_price
FROM warehouse_fact_sales fs
JOIN warehouse_dim_products dp ON fs.product_key = dp.product_key AND dp.is_current = 1
GROUP BY dp.product_key, dp.product_name, dp.category
ORDER BY total_revenue DESC
LIMIT 10;

-- QUERY 2: Monthly Sales Trend
-- Revenue patterns over time
SELECT 
    dd.year,
    dd.month,
    dd.month_name,
    COUNT(DISTINCT fs.sales_key) as total_transactions,
    ROUND(SUM(fs.line_total), 2) as total_revenue,
    ROUND(SUM(fs.profit), 2) as total_profit,
    COUNT(DISTINCT fs.customer_key) as unique_customers,
    ROUND(AVG(fs.line_total), 2) as avg_transaction_value
FROM warehouse_fact_sales fs
JOIN warehouse_dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.year, dd.month, dd.month_name
ORDER BY dd.year, dd.month;

-- QUERY 3: Customer Segmentation by Spending
-- Segment customers into spending tiers
SELECT 
    CASE 
        WHEN total_spent < 1000 THEN 'Budget (<$1K)'
        WHEN total_spent < 5000 THEN 'Mid-Range ($1K-$5K)'
        WHEN total_spent < 10000 THEN 'Premium ($5K-$10K)'
        ELSE 'VIP ($10K+)'
    END as spending_segment,
    COUNT(DISTINCT customer_key) as customer_count,
    ROUND(SUM(total_spent), 2) as segment_revenue,
    ROUND(AVG(total_spent), 2) as avg_customer_value
FROM (
    SELECT 
        fs.customer_key,
        SUM(fs.line_total) as total_spent
    FROM warehouse_fact_sales fs
    GROUP BY fs.customer_key
)
GROUP BY spending_segment
ORDER BY CASE 
    WHEN spending_segment = 'Budget (<$1K)' THEN 1
    WHEN spending_segment = 'Mid-Range ($1K-$5K)' THEN 2
    WHEN spending_segment = 'Premium ($5K-$10K)' THEN 3
    ELSE 4
END;

-- QUERY 4: Product Category Performance
-- Compare categories by revenue and profit
SELECT 
    dp.category,
    COUNT(DISTINCT dp.product_key) as product_count,
    ROUND(SUM(fs.line_total), 2) as category_revenue,
    ROUND(SUM(fs.profit), 2) as category_profit,
    ROUND(SUM(fs.quantity), 0) as units_sold,
    ROUND((SUM(fs.profit) / SUM(fs.line_total) * 100), 2) as profit_margin_pct
FROM warehouse_fact_sales fs
JOIN warehouse_dim_products dp ON fs.product_key = dp.product_key AND dp.is_current = 1
GROUP BY dp.category
ORDER BY category_revenue DESC;

-- QUERY 5: Payment Method Distribution
-- Analyze customer payment preferences
SELECT 
    dpm.payment_method_name,
    COUNT(DISTINCT fs.sales_key) as transaction_count,
    ROUND(SUM(fs.line_total), 2) as total_revenue,
    ROUND(COUNT(DISTINCT fs.sales_key) * 100.0 / (SELECT COUNT(DISTINCT sales_key) FROM warehouse_fact_sales), 2) as pct_transactions,
    ROUND(SUM(fs.line_total) * 100.0 / (SELECT SUM(line_total) FROM warehouse_fact_sales), 2) as pct_revenue
FROM warehouse_fact_sales fs
JOIN warehouse_dim_payment_method dpm ON fs.payment_method_key = dpm.payment_method_key
GROUP BY dpm.payment_method_name, dpm.payment_type
ORDER BY total_revenue DESC;

-- QUERY 6: Geographic Performance
-- Sales by state (location analysis)
SELECT 
    dc.state,
    COUNT(DISTINCT dc.customer_key) as unique_customers,
    COUNT(DISTINCT fs.sales_key) as transaction_count,
    ROUND(SUM(fs.line_total), 2) as state_revenue,
    ROUND(SUM(fs.line_total) / COUNT(DISTINCT dc.customer_key), 2) as revenue_per_customer
FROM warehouse_fact_sales fs
JOIN warehouse_dim_customers dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
GROUP BY dc.state
ORDER BY state_revenue DESC;

-- QUERY 7: Customer Lifetime Value (Top Customers)
-- Identify most valuable customers
SELECT 
    dc.full_name,
    dc.email,
    COUNT(DISTINCT fs.sales_key) as transaction_count,
    ROUND(SUM(fs.line_total), 2) as total_spent,
    ROUND(AVG(fs.line_total), 2) as avg_order_value,
    CAST(JULIANDAY('now') - JULIANDAY(dc.registration_date) AS INTEGER) as days_since_registration
FROM warehouse_fact_sales fs
JOIN warehouse_dim_customers dc ON fs.customer_key = dc.customer_key AND dc.is_current = 1
GROUP BY dc.customer_key, dc.full_name, dc.email, dc.registration_date
ORDER BY total_spent DESC
LIMIT 20;

-- QUERY 8: Product Profitability Deep Dive
-- Which products are most profitable
SELECT 
    dp.product_name,
    dp.category,
    ROUND(SUM(fs.line_total), 2) as product_revenue,
    ROUND(SUM(fs.profit), 2) as product_profit,
    ROUND((SUM(fs.profit) / SUM(fs.line_total) * 100), 2) as profit_margin_pct,
    ROUND(SUM(fs.quantity), 0) as units_sold,
    ROUND(AVG(fs.unit_price), 2) as avg_selling_price
FROM warehouse_fact_sales fs
JOIN warehouse_dim_products dp ON fs.product_key = dp.product_key AND dp.is_current = 1
GROUP BY dp.product_key, dp.product_name, dp.category
ORDER BY product_profit DESC
LIMIT 15;

-- QUERY 9: Day of Week Sales Pattern
-- Understand weekly buying patterns
SELECT 
    dd.day_name,
    COUNT(DISTINCT fs.sales_key) as transaction_count,
    ROUND(AVG(fs.line_total), 2) as avg_transaction_value,
    ROUND(SUM(fs.line_total), 2) as total_day_revenue,
    COUNT(DISTINCT dd.date_key) as occurrences_in_2024
FROM warehouse_fact_sales fs
JOIN warehouse_dim_date dd ON fs.date_key = dd.date_key
GROUP BY dd.day_name
ORDER BY CASE 
    WHEN dd.day_name = 'Monday' THEN 1
    WHEN dd.day_name = 'Tuesday' THEN 2
    WHEN dd.day_name = 'Wednesday' THEN 3
    WHEN dd.day_name = 'Thursday' THEN 4
    WHEN dd.day_name = 'Friday' THEN 5
    WHEN dd.day_name = 'Saturday' THEN 6
    ELSE 7
END;

-- QUERY 10: Discount Impact Analysis
-- How discounts affect sales volume and revenue
SELECT 
    CASE 
        WHEN fs.line_total = fs.unit_price * fs.quantity THEN '0% (No Discount)'
        WHEN fs.line_total > fs.unit_price * fs.quantity * 0.9 THEN '1-10%'
        WHEN fs.line_total > fs.unit_price * fs.quantity * 0.75 THEN '11-25%'
        WHEN fs.line_total > fs.unit_price * fs.quantity * 0.5 THEN '26-50%'
        ELSE '50%+'
    END as discount_range,
    COUNT(DISTINCT fs.sales_key) as quantity_sold,
    ROUND(SUM(fs.line_total), 2) as total_revenue,
    ROUND(AVG(fs.line_total), 2) as avg_line_total,
    ROUND(SUM(fs.profit), 2) as total_profit
FROM warehouse_fact_sales fs
GROUP BY discount_range
ORDER BY CASE 
    WHEN discount_range = '0% (No Discount)' THEN 1
    WHEN discount_range = '1-10%' THEN 2
    WHEN discount_range = '11-25%' THEN 3
    WHEN discount_range = '26-50%' THEN 4
    ELSE 5
END;
