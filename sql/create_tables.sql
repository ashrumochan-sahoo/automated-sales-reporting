-- ============================================
-- Sales Analytics Database Schema
-- Author: Your Name
-- Database: SQLite
-- Design: Star Schema
-- ============================================

-- ============================================
-- DROP EXISTING TABLES (Clean Setup)
-- Order matters: fact table first, then dims
-- ============================================

DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_shipping;
DROP TABLE IF EXISTS staging_raw_sales;

-- ============================================
-- STAGING TABLE
-- Purpose: Raw data landing zone
-- This is where CSV data lands FIRST
-- before any transformation
-- ============================================

CREATE TABLE staging_raw_sales (
    order_id        TEXT,
    order_date      TEXT,
    ship_date       TEXT,
    ship_mode       TEXT,
    customer_id     TEXT,
    customer_name   TEXT,
    segment         TEXT,
    country         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    region          TEXT,
    product_id      TEXT,
    category        TEXT,
    sub_category    TEXT,
    product_name    TEXT,
    sales           REAL,
    quantity        INTEGER,
    discount        REAL,
    profit          REAL,
    load_timestamp  TEXT DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- DIMENSION TABLE: dim_date
-- Purpose: All date-related attributes
-- Enables time-based analysis
-- ============================================

CREATE TABLE dim_date (
    date_key        INTEGER PRIMARY KEY,
    full_date       TEXT NOT NULL,
    year            INTEGER NOT NULL,
    quarter         INTEGER NOT NULL,
    month           INTEGER NOT NULL,
    month_name      TEXT,
    day             INTEGER NOT NULL,
    day_of_week     INTEGER NOT NULL,
    day_name        TEXT,
    week_of_year    INTEGER,
    is_weekend      INTEGER  -- 0 = Weekday, 1 = Weekend
);

-- ============================================
-- DIMENSION TABLE: dim_customer
-- Purpose: Customer attributes and geography
-- Enables customer segmentation analysis
-- ============================================

CREATE TABLE dim_customer (
    customer_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    customer_id     TEXT UNIQUE NOT NULL,
    customer_name   TEXT,
    segment         TEXT,
    country         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    region          TEXT,
    first_order_date TEXT,
    last_order_date  TEXT
);

-- ============================================
-- DIMENSION TABLE: dim_product
-- Purpose: Product hierarchy and categories
-- Enables product performance analysis
-- ============================================

CREATE TABLE dim_product (
    product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
    product_id      TEXT UNIQUE NOT NULL,
    product_name    TEXT,
    category        TEXT,
    sub_category    TEXT
);

-- ============================================
-- DIMENSION TABLE: dim_shipping
-- Purpose: Shipping mode attributes
-- Enables logistics analysis
-- ============================================

CREATE TABLE dim_shipping (
    shipping_key    INTEGER PRIMARY KEY AUTOINCREMENT,
    ship_mode       TEXT UNIQUE NOT NULL
);

-- ============================================
-- FACT TABLE: fact_sales
-- Purpose: Core transactional sales data
-- Central table of our star schema
-- Contains measures (sales, profit, quantity)
-- and foreign keys to all dimensions
-- ============================================

CREATE TABLE fact_sales (
    sales_key           INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id            TEXT NOT NULL,
    order_date_key      INTEGER NOT NULL,
    ship_date_key       INTEGER NOT NULL,
    customer_key        INTEGER NOT NULL,
    product_key         INTEGER NOT NULL,
    shipping_key        INTEGER NOT NULL,
    quantity            INTEGER NOT NULL,
    sales_amount        REAL NOT NULL,
    discount            REAL,
    profit              REAL NOT NULL,

    -- Foreign Keys
    FOREIGN KEY (order_date_key)    REFERENCES dim_date(date_key),
    FOREIGN KEY (ship_date_key)     REFERENCES dim_date(date_key),
    FOREIGN KEY (customer_key)      REFERENCES dim_customer(customer_key),
    FOREIGN KEY (product_key)       REFERENCES dim_product(product_key),
    FOREIGN KEY (shipping_key)      REFERENCES dim_shipping(shipping_key)
);

-- ============================================
-- INDEXES
-- Purpose: Speed up frequently used queries
-- ============================================

-- Fact table indexes
CREATE INDEX idx_fact_order_date    ON fact_sales(order_date_key);
CREATE INDEX idx_fact_customer      ON fact_sales(customer_key);
CREATE INDEX idx_fact_product       ON fact_sales(product_key);
CREATE INDEX idx_fact_order_id      ON fact_sales(order_id);

-- Dimension indexes
CREATE INDEX idx_customer_segment   ON dim_customer(segment);
CREATE INDEX idx_customer_region    ON dim_customer(region);
CREATE INDEX idx_product_category   ON dim_product(category);
CREATE INDEX idx_product_subcategory ON dim_product(sub_category);
CREATE INDEX idx_date_year_month    ON dim_date(year, month);

-- ============================================
-- VERIFICATION
-- ============================================

SELECT 'Schema created successfully!' AS status;