"""
Data Transformation Module
Author: Ashrumochan Sahoo
Purpose: Clean, standardize and transform raw sales data
         into structured format ready for loading
"""

import logging
import pandas as pd
from pathlib import Path
from pipeline.config import PROCESSED_DATA_PATH

# Get logger
logger = logging.getLogger(__name__)


# ============================================
# FUNCTION 1: clean_column_names
# Purpose: Standardize column names
# Remove spaces, lowercase everything
# ============================================

def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardizes column names:
    - Lowercase all names
    - Replace spaces with underscores
    - Remove special characters
    """
    logger.info("Cleaning column names...")

    df.columns = (
        df.columns
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
        .str.replace('/', '_')
    )

    logger.info(f"✅ Columns standardized: {list(df.columns)}")
    return df


# ============================================
# FUNCTION 2: remove_duplicates
# Purpose: Remove duplicate rows
# We found 1 duplicate in extract phase
# ============================================

def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes duplicate rows from DataFrame.
    Logs how many duplicates were removed.
    """
    logger.info("Checking for duplicates...")

    before = len(df)
    df = df.drop_duplicates().copy()
    after = len(df)
    removed = before - after

    if removed > 0:
        logger.warning(f"⚠️  Removed {removed} duplicate rows")
    else:
        logger.info(f"✅ No duplicates found")

    logger.info(f"   Rows before: {before:,}")
    logger.info(f"   Rows after : {after:,}")

    return df


# ============================================
# FUNCTION 3: fix_data_types
# Purpose: Convert columns to correct types
# Dates as datetime, numbers as float/int
# ============================================

def fix_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converts columns to appropriate data types:
    - Order Date, Ship Date → datetime
    - Sales, Profit, Discount → float
    - Quantity → integer
    """
    logger.info("Fixing data types...")

    # Convert date columns
    df['order_date'] = pd.to_datetime(
        df['order_date'],
        format='%Y-%m-%d',
        errors='coerce'
    )

    df['ship_date'] = pd.to_datetime(
        df['ship_date'],
        format='%Y-%m-%d',
        errors='coerce'
    )

    # Convert numeric columns
    df['sales']    = pd.to_numeric(df['sales'],    errors='coerce')
    df['profit']   = pd.to_numeric(df['profit'],   errors='coerce')
    df['discount'] = pd.to_numeric(df['discount'], errors='coerce')
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')

    # Check for any conversion failures (NaT or NaN)
    date_nulls = df['order_date'].isnull().sum()
    if date_nulls > 0:
        logger.warning(f"⚠️  {date_nulls} order dates failed to parse")
    else:
        logger.info(f"✅ All dates converted successfully")

    logger.info(f"✅ Data types fixed")
    logger.info(f"   order_date : {df['order_date'].dtype}")
    logger.info(f"   ship_date  : {df['ship_date'].dtype}")
    logger.info(f"   sales      : {df['sales'].dtype}")
    logger.info(f"   quantity   : {df['quantity'].dtype}")

    return df


# ============================================
# FUNCTION 4: add_derived_columns
# Purpose: Create new useful columns
# from existing data
# ============================================

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates derived columns for analytics:
    - profit_margin: profit as % of sales
    - delivery_days: days between order and ship
    - order_year, order_month, order_quarter
    - order_month_name, order_day_name
    """
    logger.info("Adding derived columns...")

    # Profit margin percentage
    df['profit_margin'] = round(
        df['profit'] / df['sales'].replace(0, pd.NA) * 100,
        2
    )

    # Delivery days (how long to ship)
    df['delivery_days'] = (
        df['ship_date'] - df['order_date']
    ).dt.days

    # Date components for time analysis
    df['order_year']       = df['order_date'].dt.year
    df['order_month']      = df['order_date'].dt.month
    df['order_quarter']    = df['order_date'].dt.quarter
    df['order_month_name'] = df['order_date'].dt.strftime('%B')
    df['order_day_name']   = df['order_date'].dt.strftime('%A')
    df['order_week']       = df['order_date'].dt.isocalendar().week.astype(int)
    df['is_weekend']       = df['order_date'].dt.dayofweek.isin([5, 6]).astype(int)

    logger.info(f"✅ Derived columns added:")
    logger.info(f"   profit_margin  : profit as % of sales")
    logger.info(f"   delivery_days  : days from order to ship")
    logger.info(f"   order_year     : extracted year")
    logger.info(f"   order_month    : extracted month number")
    logger.info(f"   order_quarter  : Q1/Q2/Q3/Q4")
    logger.info(f"   order_month_name: January, February...")
    logger.info(f"   order_day_name : Monday, Tuesday...")
    logger.info(f"   is_weekend     : 0=Weekday, 1=Weekend")

    return df


# ============================================
# FUNCTION 5: handle_nulls
# Purpose: Handle any remaining null values
# ============================================

def handle_nulls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Handles null values in the DataFrame:
    - Fills numeric nulls with 0
    - Fills text nulls with 'Unknown'
    - Logs what was filled
    """
    logger.info("Handling null values...")

    null_before = df.isnull().sum().sum()

    if null_before == 0:
        logger.info(f"✅ No null values to handle")
        return df

    # Fill nulls based on column type
    for col in df.columns:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            if df[col].dtype in ['float64', 'int64', 'Int64']:
                df[col] = df[col].fillna(0)
                logger.warning(
                    f"⚠️  Filled {null_count} nulls in '{col}' with 0"
                )
            else:
                df[col] = df[col].fillna('Unknown')
                logger.warning(
                    f"⚠️  Filled {null_count} nulls in '{col}' with 'Unknown'"
                )

    null_after = df.isnull().sum().sum()
    logger.info(f"✅ Null handling complete")
    logger.info(f"   Nulls before : {null_before}")
    logger.info(f"   Nulls after  : {null_after}")

    return df


# ============================================
# FUNCTION 6: validate_data
# Purpose: Final data quality checks
# before loading to database
# ============================================

def validate_data(df: pd.DataFrame) -> bool:
    """
    Validates transformed data quality.
    Returns True if valid, raises error if not.
    """
    logger.info("Validating transformed data...")

    errors = []

    # Check 1: No null values in critical columns
    critical_cols = [
        'order_id', 'order_date', 'customer_id',
        'product_id', 'sales', 'quantity', 'profit'
    ]
    for col in critical_cols:
        null_count = df[col].isnull().sum()
        if null_count > 0:
            errors.append(
                f"Critical column '{col}' has {null_count} null values"
            )

    # Check 2: No negative quantities
    neg_qty = (df['quantity'] < 0).sum()
    if neg_qty > 0:
        errors.append(f"{neg_qty} rows have negative quantity")

    # Check 3: No negative sales
    neg_sales = (df['sales'] < 0).sum()
    if neg_sales > 0:
        errors.append(f"{neg_sales} rows have negative sales")

    # Check 4: Delivery days should be >= 0
    neg_delivery = (df['delivery_days'] < 0).sum()
    if neg_delivery > 0:
        logger.warning(
            f"⚠️  {neg_delivery} rows have negative delivery days"
        )

    # Check 5: Discount between 0 and 1
    bad_discount = ((df['discount'] < 0) | (df['discount'] > 1)).sum()
    if bad_discount > 0:
        errors.append(f"{bad_discount} rows have invalid discount values")

    # Raise error if critical issues found
    if errors:
        for error in errors:
            logger.error(f"❌ Validation failed: {error}")
        raise ValueError(f"Data validation failed: {errors}")

    logger.info(f"✅ Data validation passed!")
    logger.info(f"   All critical columns have no nulls")
    logger.info(f"   All quantities are positive")
    logger.info(f"   All sales values are positive")
    logger.info(f"   All discounts are between 0 and 1")

    return True


# ============================================
# MAIN FUNCTION: transform_data
# Purpose: Orchestrates all transformations
# Calls all functions above in order
# ============================================

def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Main transformation function.
    Runs all transformation steps in sequence.

    Args:
        df: Raw DataFrame from extract phase

    Returns:
        Cleaned and transformed DataFrame
    """

    logger.info("=" * 50)
    logger.info("TRANSFORM PHASE STARTED")
    logger.info("=" * 50)
    logger.info(f"Input rows: {len(df):,}")

    # Step 1: Clean column names
    df = clean_column_names(df)

    # Step 2: Remove duplicates
    df = remove_duplicates(df)

    # Step 3: Fix data types
    df = fix_data_types(df)

    # Step 4: Add derived columns
    df = add_derived_columns(df)

    # Step 5: Handle nulls
    df = handle_nulls(df)

    # Step 6: Validate data
    validate_data(df)

    # Step 7: Save transformed data
    transformed_path = PROCESSED_DATA_PATH / "transformed_sales.csv"
    df.to_csv(transformed_path, index=False)
    logger.info(f"✅ Transformed data saved to: {transformed_path}")

    logger.info(f"Output rows : {len(df):,}")
    logger.info(f"Output cols : {len(df.columns):,}")
    logger.info("TRANSFORM PHASE COMPLETED")
    logger.info("=" * 50)

    return df


# ============================================
# FUNCTION 7: get_transform_summary
# Purpose: Return metrics about transformed data
# ============================================

def get_transform_summary(df: pd.DataFrame) -> dict:
    """
    Returns summary metrics of transformed data.
    """
    summary = {
        "total_rows"        : len(df),
        "total_columns"     : len(df.columns),
        "null_values"       : df.isnull().sum().sum(),
        "unique_orders"     : df['order_id'].nunique(),
        "unique_customers"  : df['customer_id'].nunique(),
        "unique_products"   : df['product_id'].nunique(),
        "date_min"          : str(df['order_date'].min()),
        "date_max"          : str(df['order_date'].max()),
        "avg_delivery_days" : round(df['delivery_days'].mean(), 1),
        "avg_profit_margin" : round(df['profit_margin'].mean(), 2),
        "total_sales"       : round(df['sales'].sum(), 2),
        "total_profit"      : round(df['profit'].sum(), 2)
    }
    return summary
