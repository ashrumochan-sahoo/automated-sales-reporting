"""
Unit Tests for ETL Pipeline
Author: Ashrumochan Sahoo
Purpose: Validate each pipeline component
         works correctly with sample data
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest
import pandas as pd
import sqlite3
from pathlib import Path
from unittest.mock import patch


# ============================================
# TEST FIXTURES
# Purpose: Reusable sample data for tests
# ============================================

@pytest.fixture
def sample_raw_df():
    """
    Creates a small sample DataFrame
    mimicking raw Superstore CSV data.
    Used across multiple tests.
    """
    return pd.DataFrame({
        'Order ID'      : ['CA-2019-001', 'CA-2019-002', 'CA-2019-003', 'CA-2019-001'],
        'Order Date'    : ['2019-01-03', '2019-01-04', '2019-01-05', '2019-01-03'],
        'Ship Date'     : ['2019-01-07', '2019-01-08', '2019-01-09', '2019-01-07'],
        'Ship Mode'     : ['Standard Class', 'First Class', 'Second Class', 'Standard Class'],
        'Customer ID'   : ['CUST-001', 'CUST-002', 'CUST-003', 'CUST-001'],
        'Customer Name' : ['John Doe', 'Jane Smith', 'Bob Johnson', 'John Doe'],
        'Segment'       : ['Consumer', 'Corporate', 'Home Office', 'Consumer'],
        'Country'       : ['United States'] * 4,
        'City'          : ['New York', 'Los Angeles', 'Chicago', 'New York'],
        'State'         : ['New York', 'California', 'Illinois', 'New York'],
        'Postal Code'   : ['10001', '90001', '60601', '10001'],
        'Region'        : ['East', 'West', 'Central', 'East'],
        'Product ID'    : ['PROD-001', 'PROD-002', 'PROD-003', 'PROD-001'],
        'Category'      : ['Technology', 'Furniture', 'Office Supplies', 'Technology'],
        'Sub-Category'  : ['Phones', 'Chairs', 'Paper', 'Phones'],
        'Product Name'  : ['Phone A', 'Chair B', 'Paper C', 'Phone A'],
        'Sales'         : [100.0, 200.0, 50.0, 100.0],
        'Quantity'      : [1, 2, 3, 1],
        'Discount'      : [0.0, 0.1, 0.2, 0.0],
        'Profit'        : [20.0, 40.0, 10.0, 20.0]
    })


@pytest.fixture
def sample_transformed_df(sample_raw_df):
    """
    Returns transformed version of sample data.
    """
    from pipeline.transform import transform_data
    return transform_data(sample_raw_df)


# ============================================
# TESTS: EXTRACT MODULE
# ============================================

class TestExtract:
    """Tests for pipeline/extract.py"""

    def test_get_extract_summary_returns_dict(self, sample_raw_df):
        from pipeline.extract import get_extract_summary
        summary = get_extract_summary(sample_raw_df)
        assert isinstance(summary, dict)
        assert 'total_rows' in summary
        assert 'total_columns' in summary
        assert 'null_values' in summary
        assert 'unique_orders' in summary
        assert 'total_sales' in summary

    def test_extract_summary_correct_row_count(self, sample_raw_df):
        from pipeline.extract import get_extract_summary
        summary = get_extract_summary(sample_raw_df)
        assert summary['total_rows'] == 4

    def test_extract_summary_correct_sales(self, sample_raw_df):
        from pipeline.extract import get_extract_summary
        summary = get_extract_summary(sample_raw_df)
        assert summary['total_sales'] == 450.0

    def test_extract_file_not_found(self):
        from pipeline.extract import extract_data
        with pytest.raises(FileNotFoundError):
            extract_data(Path("nonexistent/file.csv"))


# ============================================
# TESTS: TRANSFORM MODULE
# ============================================

class TestTransform:
    """Tests for pipeline/transform.py"""

    def test_clean_column_names(self, sample_raw_df):
        from pipeline.transform import clean_column_names
        df = clean_column_names(sample_raw_df.copy())
        for col in df.columns:
            assert ' ' not in col
        assert 'order_id' in df.columns
        assert 'customer_name' in df.columns
        assert 'sub_category' in df.columns

    def test_remove_duplicates(self, sample_raw_df):
        from pipeline.transform import clean_column_names, remove_duplicates
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        assert len(df) == 3

    def test_fix_data_types_dates(self, sample_raw_df):
        from pipeline.transform import (
            clean_column_names,
            remove_duplicates,
            fix_data_types
        )
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        df = fix_data_types(df)
        assert str(df['order_date'].dtype) == 'datetime64[ns]'
        assert str(df['ship_date'].dtype) == 'datetime64[ns]'

    def test_add_derived_columns(self, sample_raw_df):
        from pipeline.transform import (
            clean_column_names,
            remove_duplicates,
            fix_data_types,
            add_derived_columns
        )
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        df = fix_data_types(df)
        df = add_derived_columns(df)
        assert 'profit_margin' in df.columns
        assert 'delivery_days' in df.columns
        assert 'order_year' in df.columns
        assert 'order_month' in df.columns
        assert 'order_quarter' in df.columns
        assert 'is_weekend' in df.columns

    def test_delivery_days_positive(self, sample_raw_df):
        from pipeline.transform import (
            clean_column_names,
            remove_duplicates,
            fix_data_types,
            add_derived_columns
        )
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        df = fix_data_types(df)
        df = add_derived_columns(df)
        assert (df['delivery_days'] >= 0).all()

    def test_profit_margin_calculated(self, sample_raw_df):
        from pipeline.transform import (
            clean_column_names,
            remove_duplicates,
            fix_data_types,
            add_derived_columns
        )
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        df = fix_data_types(df)
        df = add_derived_columns(df)
        first_row_margin = df.iloc[0]['profit_margin']
        assert first_row_margin == 20.0

    def test_validate_data_passes(self, sample_raw_df):
        from pipeline.transform import (
            clean_column_names,
            remove_duplicates,
            fix_data_types,
            add_derived_columns,
            validate_data
        )
        df = clean_column_names(sample_raw_df.copy())
        df = remove_duplicates(df)
        df = fix_data_types(df)
        df = add_derived_columns(df)
        result = validate_data(df)
        assert result == True

    def test_transform_data_reduces_duplicates(self, sample_raw_df):
        from pipeline.transform import transform_data
        df = transform_data(sample_raw_df)
        assert len(df) == 3

    def test_transform_adds_columns(self, sample_raw_df):
        from pipeline.transform import transform_data
        df = transform_data(sample_raw_df)
        assert len(df.columns) > 20

    def test_no_nulls_after_transform(self, sample_raw_df):
        from pipeline.transform import transform_data
        df = transform_data(sample_raw_df)
        assert df.isnull().sum().sum() == 0


# ============================================
# TESTS: CONFIG MODULE
# ============================================

class TestConfig:
    """Tests for pipeline/config.py"""

    def test_connection_string_format(self):
        from pipeline.config import CONNECTION_STRING
        assert CONNECTION_STRING.startswith('sqlite:///')

    def test_raw_data_path_exists(self):
        from pipeline.config import RAW_DATA_PATH
        assert RAW_DATA_PATH is not None
        assert str(RAW_DATA_PATH).endswith('sales_data.csv')

    def test_base_dir_is_project_root(self):
        from pipeline.config import BASE_DIR
        assert (BASE_DIR / 'pipeline').exists()
        assert (BASE_DIR / 'data').exists()
        assert (BASE_DIR / 'sql').exists()