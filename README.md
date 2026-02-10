# Automated Sales Reporting Pipeline

A batch ETL pipeline that processes raw retail sales data and loads it into a structured SQLite database for business intelligence reporting.

## Overview

This pipeline ingests raw Superstore sales data from CSV, applies data quality checks and transformations, loads it into a star schema data warehouse, and generates SQL-based business insights.

## Architecture
```
CSV (Raw Data) → Extract → Transform → Load → SQLite Database → Analytics Queries
```

## Tech Stack

- Python 3.9
- SQLite (embedded database)
- Pandas (data transformation)
- SQLAlchemy (database connectivity)
- Pandera (data validation)
- Pytest (unit testing)

## Project Structure
```
automated-sales-reporting/
├── data/
│   ├── raw/                    # Source CSV files
│   └── processed/              # Intermediate cleaned data
├── pipeline/
│   ├── __init__.py
│   ├── config.py               # Configuration and settings
│   ├── extract.py              # Data extraction module
│   ├── transform.py            # Data transformation logic
│   ├── load.py                 # Database loading module
│   └── main.py                 # Pipeline orchestration
├── sql/
│   ├── create_tables.sql       # Star schema definition
│   └── analytics_queries.sql  # Business intelligence queries
├── scripts/
│   └── run_pipeline.sh         # Automation script
├── tests/
│   └── test_pipeline.py        # Unit tests
├── logs/                       # Pipeline execution logs
├── .env.example                # Environment config template
├── requirements.txt            # Python dependencies
└── README.md                   # Project documentation
```

## Dataset

- **Source**: Superstore Sales Dataset
- **Records**: 9,994 rows
- **Fields**: 20 columns including Order ID, Customer, Product, Sales, Profit
- **Date Range**: 2019

## Setup Instructions

### Prerequisites
- Python 3.9+
- pip

### Installation

**1. Clone the repository**
```bash
git clone <your-repo-url>
cd automated-sales-reporting
```

**2. Create and activate virtual environment**
```bash
python3 -m venv venv
source venv/bin/activate
```

**3. Install dependencies**
```bash
pip install -r requirements.txt
```

**4. Configure environment**
```bash
cp .env.example .env
```

**5. Add dataset**
```
Place sales_data.csv in data/raw/ directory
```

**6. Run the pipeline**
```bash
./scripts/run_pipeline.sh
```

## ETL Workflow

### Extract
- Reads raw CSV from data/raw/
- Validates file existence and structure
- Logs extraction metrics (row count, columns)

### Transform
- Handles missing values
- Standardizes date formats
- Creates derived columns (profit margin, delivery days)
- Validates data quality using Pandera

### Load
- Creates star schema tables in SQLite
- Populates dimension tables (date, customer, product, shipping)
- Loads fact table with foreign key references
- Validates row counts after loading

## Analytics

The pipeline generates insights for:
- Regional sales performance
- Top products by revenue and profit
- Monthly sales trends
- Customer segment analysis
- Discount impact on profitability
- Shipping mode analysis

## Running Tests
```bash
pytest tests/ -v
```

## Status
Project in active development