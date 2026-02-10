"""
Configuration Management Module
Author: Ashrumochan Sahoo
Purpose: Centralized settings and configuration for ETL pipeline
"""

import os
import logging
from pathlib import Path
from dotenv import load_dotenv

# ============================================
# Load Environment Variables from .env file
# ============================================
load_dotenv()


# ============================================
# BASE DIRECTORY
# Purpose: All paths are relative to project root
# This ensures paths work regardless of where
# you run the script from
# ============================================
BASE_DIR = Path(__file__).resolve().parent.parent

# ============================================
# DATABASE CONFIGURATION
# ============================================
DB_PATH = BASE_DIR / os.getenv("DB_PATH", "data/sales_analytics.db")

# SQLAlchemy connection string for SQLite
# Format: sqlite:///absolute/path/to/database.db
CONNECTION_STRING = f"sqlite:///{DB_PATH}"

# ============================================
# FILE PATH CONFIGURATION
# ============================================
RAW_DATA_PATH      = BASE_DIR / "data" / "raw" / "sales_data.csv"
PROCESSED_DATA_PATH = BASE_DIR / "data" / "processed"
SQL_DIR            = BASE_DIR / "sql"
LOGS_DIR           = BASE_DIR / "logs"

# ============================================
# LOGGING CONFIGURATION
# ============================================
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE  = BASE_DIR / os.getenv("LOG_FILE", "logs/pipeline.log")

# ============================================
# ETL CONFIGURATION
# ============================================
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", 1000))

# ============================================
# SETUP LOGGING
# Purpose: Write logs to BOTH terminal and file
# so we can see what's happening in real time
# AND review logs later
# ============================================
def setup_logging():
    """
    Configures logging for the pipeline.
    Logs go to both console and log file.
    """

    # Create logs directory if it doesn't exist
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    # Configure logging format
    log_format = "%(asctime)s | %(levelname)s | %(module)s | %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
        format=log_format,
        datefmt=date_format,
        handlers=[
            # Handler 1: Write to log file
            logging.FileHandler(LOG_FILE),
            # Handler 2: Print to terminal
            logging.StreamHandler()
        ]
    )

    return logging.getLogger(__name__)


# ============================================
# CONFIGURATION VALIDATION
# Purpose: Check everything is in place
# BEFORE the pipeline starts running
# Fail early = easier debugging
# ============================================
def validate_config():
    """
    Validates all required configurations exist.
    Raises errors immediately if something is wrong.
    """
    logger = logging.getLogger(__name__)

    # Check raw data file exists
    if not RAW_DATA_PATH.exists():
        raise FileNotFoundError(
            f"\n❌ Raw data file not found: {RAW_DATA_PATH}"
            f"\n   Please place sales_data.csv in data/raw/ directory"
        )

    # Check processed directory exists
    PROCESSED_DATA_PATH.mkdir(parents=True, exist_ok=True)

    # Check logs directory exists
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    logger.info("✅ Configuration validated successfully")
    logger.info(f"   Database path : {DB_PATH}")
    logger.info(f"   Raw data path : {RAW_DATA_PATH}")
    logger.info(f"   Log file      : {LOG_FILE}")
    logger.info(f"   Chunk size    : {CHUNK_SIZE}")

    return True
