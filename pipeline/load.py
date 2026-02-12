"""
Data Loading Module
Author: Ashrumochan Sahoo
Purpose: Load transformed sales data into
         SQLite star schema database
"""

import logging
import sqlite3
import pandas as pd
from sqlalchemy import create_engine, text
from pipeline.config import (
    CONNECTION_STRING,
    DB_PATH,
    SQL_DIR
)

# Get logger
logger = logging.getLogger(__name__)


# ============================================
# FUNCTION 1: get_engine
# Purpose: Create SQLAlchemy engine
# for database connection
# ============================================

def get_engine():
    """
    Creates and returns SQLAlchemy engine
    for SQLite database connection.
    """
    engine = create_engine(
        CONNECTION_STRING,
        echo=False  # Set True to see SQL queries
    )
    logger.info(f"✅ Database engine created: {DB_PATH}")
    return engine


# ============================================
# FUNCTION 2: create_schema
# Purpose: Create all tables from SQL file
# ============================================

def create_schema(engine):
    """
    Creates database schema using
    direct SQLite connection.
    More reliable than SQLAlchemy for
    executing multi-statement SQL files.
    """
    logger.info("Creating database schema...")

    import sqlite3

    # Connect directly with sqlite3
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enable foreign keys
    cursor.execute("PRAGMA foreign_keys = ON")

    # Drop tables in correct order (fact first, then dims)
    drop_statements = [
        "DROP TABLE IF EXISTS fact_sales",
        "DROP TABLE IF EXISTS dim_date",
        "DROP TABLE IF EXISTS dim_customer",
        "DROP TABLE IF EXISTS dim_product",
        "DROP TABLE IF EXISTS dim_shipping",
        "DROP TABLE IF EXISTS staging_raw_sales"
    ]

    for stmt in drop_statements:
        cursor.execute(stmt)

    # Create staging table
    cursor.execute("""
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
        )
    """)

    # Create dim_date
    cursor.execute("""
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
            is_weekend      INTEGER
        )
    """)

    # Create dim_customer
    cursor.execute("""
        CREATE TABLE dim_customer (
            customer_key        INTEGER PRIMARY KEY AUTOINCREMENT,
            customer_id         TEXT UNIQUE NOT NULL,
            customer_name       TEXT,
            segment             TEXT,
            country             TEXT,
            city                TEXT,
            state               TEXT,
            postal_code         TEXT,
            region              TEXT,
            first_order_date    TEXT,
            last_order_date     TEXT
        )
    """)

    # Create dim_product
    cursor.execute("""
        CREATE TABLE dim_product (
            product_key     INTEGER PRIMARY KEY AUTOINCREMENT,
            product_id      TEXT UNIQUE NOT NULL,
            product_name    TEXT,
            category        TEXT,
            sub_category    TEXT
        )
    """)

    # Create dim_shipping
    cursor.execute("""
        CREATE TABLE dim_shipping (
            shipping_key    INTEGER PRIMARY KEY AUTOINCREMENT,
            ship_mode       TEXT UNIQUE NOT NULL
        )
    """)

    # Create fact_sales
    cursor.execute("""
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
            FOREIGN KEY (order_date_key)  REFERENCES dim_date(date_key),
            FOREIGN KEY (ship_date_key)   REFERENCES dim_date(date_key),
            FOREIGN KEY (customer_key)    REFERENCES dim_customer(customer_key),
            FOREIGN KEY (product_key)     REFERENCES dim_product(product_key),
            FOREIGN KEY (shipping_key)    REFERENCES dim_shipping(shipping_key)
        )
    """)

    # Create indexes
    indexes = [
        "CREATE INDEX idx_fact_order_date  ON fact_sales(order_date_key)",
        "CREATE INDEX idx_fact_customer    ON fact_sales(customer_key)",
        "CREATE INDEX idx_fact_product     ON fact_sales(product_key)",
        "CREATE INDEX idx_fact_order_id    ON fact_sales(order_id)",
        "CREATE INDEX idx_customer_segment ON dim_customer(segment)",
        "CREATE INDEX idx_customer_region  ON dim_customer(region)",
        "CREATE INDEX idx_product_category ON dim_product(category)",
        "CREATE INDEX idx_product_subcat   ON dim_product(sub_category)",
        "CREATE INDEX idx_date_year_month  ON dim_date(year, month)"
    ]

    for idx in indexes:
        cursor.execute(idx)

    conn.commit()
    conn.close()

    logger.info("✅ Database schema created successfully")
    logger.info("   Tables: staging, dim_date, dim_customer,")
    logger.info("           dim_product, dim_shipping, fact_sales")
    logger.info("   Indexes: 9 performance indexes created")

# ============================================
# FUNCTION 3: load_staging
# Purpose: Load raw data into staging table
# First stop for ALL data
# ============================================

def load_staging(df: pd.DataFrame, engine):
    """
    Loads transformed DataFrame into
    staging_raw_sales table.
    """
    logger.info("Loading data into staging table...")

    # Select only original columns for staging
    staging_cols = [
        'order_id', 'order_date', 'ship_date',
        'ship_mode', 'customer_id', 'customer_name',
        'segment', 'country', 'city', 'state',
        'postal_code', 'region', 'product_id',
        'category', 'sub_category', 'product_name',
        'sales', 'quantity', 'discount', 'profit'
    ]

    staging_df = df[staging_cols].copy()

    # Convert dates to string for SQLite
    staging_df['order_date'] = staging_df['order_date'].dt.strftime('%Y-%m-%d')
    staging_df['ship_date']  = staging_df['ship_date'].dt.strftime('%Y-%m-%d')

    # Load to staging table
    staging_df.to_sql(
        'staging_raw_sales',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ Staging table loaded: {len(staging_df):,} rows")
    return len(staging_df)


# ============================================
# FUNCTION 4: load_dim_date
# Purpose: Populate date dimension table
# Creates one row per unique date
# ============================================

def load_dim_date(df: pd.DataFrame, engine):
    """
    Populates dim_date table with all
    unique dates from order and ship dates.
    """
    logger.info("Loading dim_date...")

    # Collect all unique dates
    order_dates = df['order_date'].dropna()
    ship_dates  = df['ship_date'].dropna()
    all_dates   = pd.concat([order_dates, ship_dates]).unique()
    all_dates   = pd.to_datetime(all_dates)
    all_dates   = sorted(all_dates)

    # Build date dimension rows
    date_rows = []
    for date in all_dates:
        date_rows.append({
            'date_key'     : int(date.strftime('%Y%m%d')),
            'full_date'    : date.strftime('%Y-%m-%d'),
            'year'         : date.year,
            'quarter'      : date.quarter,
            'month'        : date.month,
            'month_name'   : date.strftime('%B'),
            'day'          : date.day,
            'day_of_week'  : date.dayofweek,
            'day_name'     : date.strftime('%A'),
            'week_of_year' : int(date.isocalendar()[1]),
            'is_weekend'   : 1 if date.dayofweek >= 5 else 0
        })

    dim_date_df = pd.DataFrame(date_rows)

    dim_date_df.to_sql(
        'dim_date',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ dim_date loaded: {len(dim_date_df):,} rows")
    return len(dim_date_df)


# ============================================
# FUNCTION 5: load_dim_customer
# Purpose: Populate customer dimension
# One row per unique customer
# ============================================

def load_dim_customer(df: pd.DataFrame, engine):
    """
    Populates dim_customer table with
    unique customer records.
    """
    logger.info("Loading dim_customer...")

    # Get first and last order date per customer
    customer_dates = df.groupby('customer_id').agg(
        first_order_date=('order_date', 'min'),
        last_order_date=('order_date', 'max')
    ).reset_index()

    # Get unique customers with their attributes
    customer_cols = [
        'customer_id', 'customer_name', 'segment',
        'country', 'city', 'state',
        'postal_code', 'region'
    ]

    dim_customer = df[customer_cols].drop_duplicates(
        subset=['customer_id']
    ).copy()

    # Merge with date info
    dim_customer = dim_customer.merge(
        customer_dates,
        on='customer_id',
        how='left'
    )

    # Convert dates to string
    dim_customer['first_order_date'] = pd.to_datetime(
        dim_customer['first_order_date']
    ).dt.strftime('%Y-%m-%d')

    dim_customer['last_order_date'] = pd.to_datetime(
        dim_customer['last_order_date']
    ).dt.strftime('%Y-%m-%d')

    dim_customer.to_sql(
        'dim_customer',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ dim_customer loaded: {len(dim_customer):,} rows")
    return len(dim_customer)


# ============================================
# FUNCTION 6: load_dim_product
# Purpose: Populate product dimension
# One row per unique product
# ============================================

def load_dim_product(df: pd.DataFrame, engine):
    """
    Populates dim_product table with
    unique product records.
    """
    logger.info("Loading dim_product...")

    product_cols = [
        'product_id', 'product_name',
        'category', 'sub_category'
    ]

    dim_product = df[product_cols].drop_duplicates(
        subset=['product_id']
    ).copy()

    dim_product.to_sql(
        'dim_product',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ dim_product loaded: {len(dim_product):,} rows")
    return len(dim_product)


# ============================================
# FUNCTION 7: load_dim_shipping
# Purpose: Populate shipping dimension
# One row per unique shipping mode
# ============================================

def load_dim_shipping(df: pd.DataFrame, engine):
    """
    Populates dim_shipping table with
    unique shipping modes.
    """
    logger.info("Loading dim_shipping...")

    dim_shipping = df[['ship_mode']].drop_duplicates().copy()

    dim_shipping.to_sql(
        'dim_shipping',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ dim_shipping loaded: {len(dim_shipping):,} rows")
    return len(dim_shipping)


# ============================================
# FUNCTION 8: load_fact_sales
# Purpose: Populate fact table
# Core of the star schema
# ============================================

def load_fact_sales(df: pd.DataFrame, engine):
    """
    Populates fact_sales table by joining
    transformed data with dimension keys.
    """
    logger.info("Loading fact_sales...")

    with engine.connect() as conn:

        # Get dimension keys
        customers = pd.read_sql(
            "SELECT customer_key, customer_id FROM dim_customer",
            conn
        )
        products = pd.read_sql(
            "SELECT product_key, product_id FROM dim_product",
            conn
        )
        shipping = pd.read_sql(
            "SELECT shipping_key, ship_mode FROM dim_shipping",
            conn
        )

    # Convert dates to date_key format (YYYYMMDD)
    df['order_date_key'] = pd.to_datetime(
        df['order_date']
    ).dt.strftime('%Y%m%d').astype(int)

    df['ship_date_key'] = pd.to_datetime(
        df['ship_date']
    ).dt.strftime('%Y%m%d').astype(int)

    # Merge with dimension keys
    fact = df.merge(customers, on='customer_id', how='left')
    fact = fact.merge(products,  on='product_id',  how='left')
    fact = fact.merge(shipping,  on='ship_mode',   how='left')

    # Select only fact table columns
    fact_cols = [
        'order_id',
        'order_date_key',
        'ship_date_key',
        'customer_key',
        'product_key',
        'shipping_key',
        'quantity',
        'sales',
        'discount',
        'profit'
    ]

    fact_df = fact[fact_cols].copy()
    fact_df = fact_df.rename(columns={'sales': 'sales_amount'})

    fact_df.to_sql(
        'fact_sales',
        engine,
        if_exists='append',
        index=False
    )

    logger.info(f"✅ fact_sales loaded: {len(fact_df):,} rows")
    return len(fact_df)


# ============================================
# FUNCTION 9: validate_load
# Purpose: Verify row counts after loading
# Reconcile source vs destination
# ============================================

def validate_load(engine, source_rows: int):
    """
    Validates data was loaded correctly
    by checking row counts in all tables.
    """
    logger.info("Validating loaded data...")

    with engine.connect() as conn:
        tables = {
            'staging_raw_sales' : 'SELECT COUNT(*) FROM staging_raw_sales',
            'dim_date'          : 'SELECT COUNT(*) FROM dim_date',
            'dim_customer'      : 'SELECT COUNT(*) FROM dim_customer',
            'dim_product'       : 'SELECT COUNT(*) FROM dim_product',
            'dim_shipping'      : 'SELECT COUNT(*) FROM dim_shipping',
            'fact_sales'        : 'SELECT COUNT(*) FROM fact_sales'
        }

        counts = {}
        for table, query in tables.items():
            count = conn.execute(text(query)).scalar()
            counts[table] = count
            logger.info(f"   {table:<25}: {count:,} rows")

    # Validate fact table matches source
    if counts['fact_sales'] != source_rows:
        logger.error(
            f"❌ Row count mismatch! "
            f"Source: {source_rows:,}, "
            f"Fact: {counts['fact_sales']:,}"
        )
        raise ValueError("Load validation failed - row count mismatch!")

    logger.info(f"✅ Load validation passed!")
    logger.info(
        f"   Source rows  : {source_rows:,} "
        f"= Fact rows: {counts['fact_sales']:,}"
    )
    return counts


# ============================================
# MAIN FUNCTION: load_data
# Purpose: Orchestrates all loading steps
# ============================================

def load_data(df: pd.DataFrame):
    """
    Main loading function.
    Orchestrates all loading steps in sequence.

    Args:
        df: Transformed DataFrame from transform phase

    Returns:
        Dictionary of row counts per table
    """

    logger.info("=" * 50)
    logger.info("LOAD PHASE STARTED")
    logger.info("=" * 50)
    logger.info(f"Input rows: {len(df):,}")

    # Step 1: Create database engine
    engine = get_engine()

    # Step 2: Create schema (tables)
    create_schema(engine)

    # Step 3: Load staging table
    load_staging(df, engine)

    # Step 4: Load dimension tables
    load_dim_date(df, engine)
    load_dim_customer(df, engine)
    load_dim_product(df, engine)
    load_dim_shipping(df, engine)

    # Step 5: Load fact table
    load_fact_sales(df, engine)

    # Step 6: Validate everything loaded correctly
    counts = validate_load(engine, len(df))

    logger.info("LOAD PHASE COMPLETED")
    logger.info("=" * 50)

    return counts
