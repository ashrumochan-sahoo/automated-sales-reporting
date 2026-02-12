"""
ETL Pipeline Orchestration Module
Author: Ashrumochan Sahoo
Purpose: Main entry point for the automated
         sales reporting ETL pipeline.
         Orchestrates Extract, Transform, Load phases.
"""

import logging
import time
from datetime import datetime
from pipeline.config import setup_logging, validate_config
from pipeline.extract import extract_data, get_extract_summary
from pipeline.transform import transform_data, get_transform_summary
from pipeline.load import load_data


# ============================================
# MAIN PIPELINE FUNCTION
# Purpose: Orchestrates all ETL phases
# ============================================

def run_pipeline():
    """
    Runs the complete ETL pipeline:
    1. Extract  - Read raw CSV data
    2. Transform - Clean and standardize
    3. Load     - Load into SQLite database

    Returns:
        dict: Pipeline execution summary
    """

    # Setup logging first
    logger = setup_logging()

    # Track start time
    pipeline_start = time.time()
    start_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    logger.info("=" * 60)
    logger.info("  AUTOMATED SALES REPORTING PIPELINE")
    logger.info("=" * 60)
    logger.info(f"  Started at : {start_datetime}")
    logger.info(f"  Author     : Ashrumochan Sahoo")
    logger.info(f"  Version    : 1.0.0")
    logger.info("=" * 60)

    try:

        # ----------------------------------------
        # PRE-FLIGHT CHECK
        # Validate all configs before starting
        # ----------------------------------------
        logger.info("")
        logger.info(">>> PRE-FLIGHT CHECKS")
        validate_config()

        # ----------------------------------------
        # PHASE 1: EXTRACT
        # ----------------------------------------
        logger.info("")
        logger.info(">>> PHASE 1: EXTRACT")
        phase_start = time.time()

        df_raw = extract_data()
        extract_summary = get_extract_summary(df_raw)

        extract_time = round(time.time() - phase_start, 2)
        logger.info(f">>> PHASE 1 COMPLETED in {extract_time}s")

        # ----------------------------------------
        # PHASE 2: TRANSFORM
        # ----------------------------------------
        logger.info("")
        logger.info(">>> PHASE 2: TRANSFORM")
        phase_start = time.time()

        df_transformed = transform_data(df_raw)
        transform_summary = get_transform_summary(df_transformed)

        transform_time = round(time.time() - phase_start, 2)
        logger.info(f">>> PHASE 2 COMPLETED in {transform_time}s")

        # ----------------------------------------
        # PHASE 3: LOAD
        # ----------------------------------------
        logger.info("")
        logger.info(">>> PHASE 3: LOAD")
        phase_start = time.time()

        load_counts = load_data(df_transformed)

        load_time = round(time.time() - phase_start, 2)
        logger.info(f">>> PHASE 3 COMPLETED in {load_time}s")

        # ----------------------------------------
        # PIPELINE SUMMARY
        # ----------------------------------------
        total_time = round(time.time() - pipeline_start, 2)

        logger.info("")
        logger.info("=" * 60)
        logger.info("  PIPELINE EXECUTION SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  Status          : ✅ SUCCESS")
        logger.info(f"  Started at      : {start_datetime}")
        logger.info(f"  Total time      : {total_time}s")
        logger.info("")
        logger.info("  EXTRACT METRICS:")
        logger.info(f"    Rows extracted    : {extract_summary['total_rows']:,}")
        logger.info(f"    Columns           : {extract_summary['total_columns']}")
        logger.info(f"    Null values       : {extract_summary['null_values']}")
        logger.info(f"    Date range        : {extract_summary['date_min']} to {extract_summary['date_max']}")
        logger.info("")
        logger.info("  TRANSFORM METRICS:")
        logger.info(f"    Rows after clean  : {transform_summary['total_rows']:,}")
        logger.info(f"    Columns           : {transform_summary['total_columns']}")
        logger.info(f"    Avg delivery days : {transform_summary['avg_delivery_days']}")
        logger.info(f"    Avg profit margin : {transform_summary['avg_profit_margin']}%")
        logger.info("")
        logger.info("  LOAD METRICS:")
        for table, count in load_counts.items():
            logger.info(f"    {table:<25}: {count:,} rows")
        logger.info("")
        logger.info("  PERFORMANCE:")
        logger.info(f"    Extract time      : {extract_time}s")
        logger.info(f"    Transform time    : {transform_time}s")
        logger.info(f"    Load time         : {load_time}s")
        logger.info(f"    Total time        : {total_time}s")
        logger.info("=" * 60)

        # Return summary for testing
        return {
            "status"           : "SUCCESS",
            "total_time"       : total_time,
            "extract_summary"  : extract_summary,
            "transform_summary": transform_summary,
            "load_counts"      : load_counts
        }

    # ----------------------------------------
    # ERROR HANDLING
    # Catches any error in any phase
    # ----------------------------------------
    except FileNotFoundError as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error(f"  ❌ PIPELINE FAILED - FILE NOT FOUND")
        logger.error(f"  Error: {e}")
        logger.error("=" * 60)
        raise

    except ValueError as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error(f"  ❌ PIPELINE FAILED - VALIDATION ERROR")
        logger.error(f"  Error: {e}")
        logger.error("=" * 60)
        raise

    except Exception as e:
        logger.error("")
        logger.error("=" * 60)
        logger.error(f"  ❌ PIPELINE FAILED - UNEXPECTED ERROR")
        logger.error(f"  Error: {e}")
        logger.error("=" * 60)
        raise

    finally:
        total_time = round(time.time() - pipeline_start, 2)
        logger.info(f"  Pipeline finished in {total_time}s")


# ============================================
# ENTRY POINT
# Purpose: Allows running as script
# python3 -m pipeline.main
# OR
# python3 pipeline/main.py
# ============================================

if __name__ == "__main__":
    result = run_pipeline()
    print("\n✅ Pipeline completed successfully!")
    print(f"   Total time : {result['total_time']}s")
    print(f"   Rows loaded: {result['load_counts']['fact_sales']:,}")
    