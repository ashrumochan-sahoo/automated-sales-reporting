"""
Data Extraction Module
Author: Ashrumochan Sahoo
Purpose: Extract raw sales data from CSV source
         and perform initial validation
"""

import logging
import pandas as pd
from pathlib import Path
from pipeline.config import RAW_DATA_PATH, PROCESSED_DATA_PATH

# Get logger
logger = logging.getLogger(__name__)


# ============================================
# FUNCTION 1: extract_data
# Purpose: Read CSV file into a DataFrame
# ============================================

def extract_data(file_path: Path = RAW_DATA_PATH) -> pd.DataFrame:
    """
    Extracts raw sales data from CSV file.

    Args:
        file_path: Path to the CSV file

    Returns:
        DataFrame containing raw sales data

    Raises:
        FileNotFoundError: If CSV file doesn't exist
        ValueError: If CSV file is empty
    """

    logger.info("=" * 50)
    logger.info("EXTRACT PHASE STARTED")
    logger.info("=" * 50)
    logger.info(f"Reading data from: {file_path}")

    # ----------------------------------------
    # CHECK 1: File exists
    # ----------------------------------------
    if not file_path.exists():
        raise FileNotFoundError(
            f"❌ Data file not found: {file_path}\n"
            f"   Please place sales_data.csv in data/raw/"
        )

    # ----------------------------------------
    # Read CSV into DataFrame
    # ----------------------------------------
    try:
        df = pd.read_csv(
            file_path,
            dtype={
                'Order ID'    : str,
                'Customer ID' : str,
                'Product ID'  : str,
                'Postal Code' : str
            }
        )
    except Exception as e:
        logger.error(f"❌ Failed to read CSV: {e}")
        raise

    # ----------------------------------------
    # CHECK 2: File is not empty
    # ----------------------------------------
    if df.empty:
        raise ValueError(
            f"❌ Data file is empty: {file_path}"
        )

    # ----------------------------------------
    # CHECK 3: Required columns exist
    # ----------------------------------------
    required_columns = [
        'Order ID', 'Order Date', 'Ship Date',
        'Ship Mode', 'Customer ID', 'Customer Name',
        'Segment', 'Country', 'City', 'State',
        'Postal Code', 'Region', 'Product ID',
        'Category', 'Sub-Category', 'Product Name',
        'Sales', 'Quantity', 'Discount', 'Profit'
    ]

    missing_columns = [
        col for col in required_columns
        if col not in df.columns
    ]

    if missing_columns:
        raise ValueError(
            f"❌ Missing required columns: {missing_columns}"
        )

    # ----------------------------------------
    # LOG EXTRACTION METRICS
    # ----------------------------------------
    logger.info(f"✅ Data extracted successfully!")
    logger.info(f"   Rows extracted    : {len(df):,}")
    logger.info(f"   Columns found     : {len(df.columns)}")
    logger.info(f"   Date range        : "
                f"{df['Order Date'].min()} to "
                f"{df['Order Date'].max()}")
    logger.info(f"   Memory usage      : "
                f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")

    # ----------------------------------------
    # LOG NULL VALUE SUMMARY
    # ----------------------------------------
    null_counts = df.isnull().sum()
    null_columns = null_counts[null_counts > 0]

    if len(null_columns) > 0:
        logger.warning(f"⚠️  Null values found:")
        for col, count in null_columns.items():
            logger.warning(f"   {col}: {count} nulls")
    else:
        logger.info(f"✅ No null values found in dataset")

    # ----------------------------------------
    # SAVE RAW COPY to processed folder
    # for reference/auditing
    # ----------------------------------------
    raw_backup_path = PROCESSED_DATA_PATH / "raw_backup.csv"
    df.to_csv(raw_backup_path, index=False)
    logger.info(f"✅ Raw backup saved to: {raw_backup_path}")

    logger.info("EXTRACT PHASE COMPLETED")
    logger.info("=" * 50)

    return df


# ============================================
# FUNCTION 2: get_extract_summary
# Purpose: Return key metrics about extracted data
# Used for validation after extraction
# ============================================

def get_extract_summary(df: pd.DataFrame) -> dict:
    """
    Returns a summary of extracted data metrics.

    Args:
        df: Extracted DataFrame

    Returns:
        Dictionary containing extraction metrics
    """

    summary = {
        "total_rows"       : len(df),
        "total_columns"    : len(df.columns),
        "null_values"      : df.isnull().sum().sum(),
        "duplicate_rows"   : df.duplicated().sum(),
        "date_min"         : df['Order Date'].min(),
        "date_max"         : df['Order Date'].max(),
        "unique_orders"    : df['Order ID'].nunique(),
        "unique_customers" : df['Customer ID'].nunique(),
        "unique_products"  : df['Product ID'].nunique(),
        "total_sales"      : round(df['Sales'].sum(), 2),
        "total_profit"     : round(df['Profit'].sum(), 2)
    }

    return summary
