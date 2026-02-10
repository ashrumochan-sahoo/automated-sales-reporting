-- ============================================
-- Sales Analytics Queries
-- Author: Ashrumochan Sahoo
-- Database: SQLite
-- Purpose: Business intelligence reporting
-- ============================================

-- ============================================
-- QUERY 1: Regional Sales Performance
-- Business Question: Which regions drive
-- the most revenue and profit?
-- ============================================

SELECT
    c.region,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    SUM(f.quantity)                     AS total_units_sold,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(AVG(f.sales_amount), 2)       AS avg_order_value,
    ROUND(
        SUM(f.profit) * 100.0 /
        NULLIF(SUM(f.sales_amount), 0)
    , 2)                                AS profit_margin_pct
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region
ORDER BY total_revenue DESC;

-- ============================================
-- QUERY 2: Top 10 Products by Revenue
-- Business Question: What are our
-- best performing products?
-- ============================================

SELECT
    p.product_name,
    p.category,
    p.sub_category,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(AVG(f.discount) * 100, 2)     AS avg_discount_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY
    p.product_name,
    p.category,
    p.sub_category
ORDER BY total_revenue DESC
LIMIT 10;

-- ============================================
-- QUERY 3: Monthly Sales Trend
-- Business Question: How are sales
-- trending month over month?
-- ============================================

SELECT
    d.year,
    d.month,
    d.month_name,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.sales_amount), 2)       AS monthly_revenue,
    ROUND(SUM(f.profit), 2)             AS monthly_profit,
    ROUND(AVG(f.sales_amount), 2)       AS avg_transaction_size
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY
    d.year,
    d.month,
    d.month_name
ORDER BY
    d.year,
    d.month;

-- ============================================
-- QUERY 4: Customer Segment Analysis
-- Business Question: Which customer segments
-- are most valuable to the business?
-- ============================================

SELECT
    c.segment,
    COUNT(DISTINCT c.customer_key)      AS total_customers,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(
        SUM(f.sales_amount) * 1.0 /
        COUNT(DISTINCT c.customer_key)
    , 2)                                AS revenue_per_customer,
    ROUND(
        COUNT(DISTINCT f.order_id) * 1.0 /
        COUNT(DISTINCT c.customer_key)
    , 2)                                AS orders_per_customer
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.segment
ORDER BY total_revenue DESC;

-- ============================================
-- QUERY 5: Discount Impact Analysis
-- Business Question: How do discounts
-- affect our profitability?
-- ============================================

SELECT
    CASE
        WHEN f.discount = 0         THEN '0 - No Discount'
        WHEN f.discount <= 0.10     THEN '1 - Up to 10%'
        WHEN f.discount <= 0.20     THEN '2 - Up to 20%'
        WHEN f.discount <= 0.30     THEN '3 - Up to 30%'
        ELSE                             '4 - Above 30%'
    END                                 AS discount_bucket,
    COUNT(*)                            AS transaction_count,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(
        SUM(f.profit) * 100.0 /
        NULLIF(SUM(f.sales_amount), 0)
    , 2)                                AS profit_margin_pct,
    ROUND(AVG(f.sales_amount), 2)       AS avg_sale_amount
FROM fact_sales f
GROUP BY discount_bucket
ORDER BY discount_bucket;

-- ============================================
-- QUERY 6: Product Category Performance
-- Business Question: Which categories
-- drive revenue vs profit?
-- ============================================

SELECT
    p.category,
    COUNT(DISTINCT p.product_key)       AS product_count,
    SUM(f.quantity)                     AS units_sold,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(
        SUM(f.profit) * 100.0 /
        NULLIF(SUM(f.sales_amount), 0)
    , 2)                                AS profit_margin_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category
ORDER BY total_revenue DESC;

-- ============================================
-- QUERY 7: Shipping Mode Analysis
-- Business Question: Does shipping speed
-- affect order value and profitability?
-- ============================================

SELECT
    s.ship_mode,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(AVG(f.sales_amount), 2)       AS avg_order_value,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(
        AVG(
            JULIANDAY(sd.full_date) -
            JULIANDAY(od.full_date)
        )
    , 1)                                AS avg_delivery_days
FROM fact_sales f
JOIN dim_shipping s  ON f.shipping_key  = s.shipping_key
JOIN dim_date od     ON f.order_date_key = od.date_key
JOIN dim_date sd     ON f.ship_date_key  = sd.date_key
GROUP BY s.ship_mode
ORDER BY total_revenue DESC;

-- ============================================
-- QUERY 8: State wise Sales Performance
-- Business Question: Which states are
-- our top markets?
-- ============================================

SELECT
    c.state,
    c.region,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.sales_amount), 2)       AS total_revenue,
    ROUND(SUM(f.profit), 2)             AS total_profit,
    ROUND(
        SUM(f.profit) * 100.0 /
        NULLIF(SUM(f.sales_amount), 0)
    , 2)                                AS profit_margin_pct
FROM fact_sales f
JOIN dim_customer c ON f.customer_key = c.customer_key
GROUP BY
    c.state,
    c.region
ORDER BY total_revenue DESC
LIMIT 15;

-- ============================================
-- QUERY 9: Quarterly Revenue Summary
-- Business Question: How does revenue
-- compare quarter over quarter?
-- ============================================

SELECT
    d.year,
    d.quarter,
    COUNT(DISTINCT f.order_id)          AS total_orders,
    ROUND(SUM(f.sales_amount), 2)       AS quarterly_revenue,
    ROUND(SUM(f.profit), 2)             AS quarterly_profit,
    ROUND(
        SUM(f.profit) * 100.0 /
        NULLIF(SUM(f.sales_amount), 0)
    , 2)                                AS profit_margin_pct
FROM fact_sales f
JOIN dim_date d ON f.order_date_key = d.date_key
GROUP BY
    d.year,
    d.quarter
ORDER BY
    d.year,
    d.quarter;

-- ============================================
-- QUERY 10: Pipeline Validation Query
-- Purpose: Verify data loaded correctly
-- Run this AFTER pipeline execution
-- ============================================

SELECT
    'staging_raw_sales'     AS table_name,
    COUNT(*)                AS row_count
FROM staging_raw_sales

UNION ALL

SELECT
    'dim_customer'          AS table_name,
    COUNT(*)                AS row_count
FROM dim_customer

UNION ALL

SELECT
    'dim_product'           AS table_name,
    COUNT(*)                AS row_count
FROM dim_product

UNION ALL

SELECT
    'dim_date'              AS table_name,
    COUNT(*)                AS row_count
FROM dim_date

UNION ALL

SELECT
    'dim_shipping'          AS table_name,
    COUNT(*)                AS row_count
FROM dim_shipping

UNION ALL

SELECT
    'fact_sales'            AS table_name,
    COUNT(*)                AS row_count
FROM fact_sales;