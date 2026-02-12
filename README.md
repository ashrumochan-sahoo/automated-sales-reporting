# 🛒 Automated Sales Reporting Pipeline

![Python](https://img.shields.io/badge/Python-3.9-blue)
![SQLite](https://img.shields.io/badge/Database-SQLite-green)
![Pandas](https://img.shields.io/badge/Pandas-2.0.3-yellow)
![Tests](https://img.shields.io/badge/Tests-17%20Passing-brightgreen)
![Status](https://img.shields.io/badge/Status-Complete-success)

A production-ready batch ETL pipeline that processes raw retail
sales data into a structured SQLite data warehouse for business
intelligence reporting and analysis.

---

## 📊 Project Overview

This pipeline demonstrates end-to-end data engineering by:
- **Extracting** raw Superstore sales data from CSV
- **Transforming** it with data quality checks and business logic
- **Loading** it into a star schema SQLite data warehouse
- **Analyzing** it with 10 SQL business intelligence queries

---

## 🏗️ Architecture
```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   CSV File  │───▶│   Extract   │───▶│  Transform  │───▶│    Load     │
│  9,994 rows │    │  validate   │    │  clean data │    │  SQLite DB  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                                │
                                                                ▼
                                                    ┌─────────────────────┐
                                                    │   Star Schema DWH   │
                                                    │  ┌───────────────┐  │
                                                    │  │  fact_sales   │  │
                                                    │  └───────┬───────┘  │
                                                    │    ┌─────┴─────┐    │
                                                    │  dims     dims      │
                                                    └─────────────────────┘
                                                                │
                                                                ▼
                                                    ┌─────────────────────┐
                                                    │  Analytics Queries  │
                                                    │  10 BI Insights     │
                                                    └─────────────────────┘
```

---

## 🛠️ Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.9 | ETL orchestration |
| SQLite | Built-in | Data warehouse |
| Pandas | 2.0.3 | Data transformation |
| SQLAlchemy | 2.0.23 | Database connectivity |
| Pandera | 0.17.2 | Data validation |
| Pytest | 7.4.3 | Unit testing |

---

## 📁 Project Structure
```
automated-sales-reporting/
├── data/
│   ├── raw/                      # Source CSV files
│   │   └── sales_data.csv        # Superstore dataset (9,994 rows)
│   └── processed/                # Intermediate outputs
│       ├── raw_backup.csv        # Raw data backup
│       └── transformed_sales.csv # Cleaned dataset
├── pipeline/
│   ├── __init__.py               # Package initializer
│   ├── config.py                 # Configuration management
│   ├── extract.py                # Data extraction module
│   ├── transform.py              # Data transformation logic
│   ├── load.py                   # Database loading module
│   └── main.py                   # Pipeline orchestration
├── sql/
│   ├── create_tables.sql         # Star schema definition
│   └── analytics_queries.sql    # 10 BI queries
├── scripts/
│   └── run_pipeline.sh           # Automation script
├── tests/
│   └── test_pipeline.py          # 17 unit tests
├── logs/
│   └── pipeline.log              # Execution logs
├── .env.example                  # Environment config template
├── pytest.ini                    # Test configuration
├── requirements.txt              # Python dependencies
└── README.md                     # Project documentation
```

---

## 📈 Dataset

| Property | Value |
|----------|-------|
| Source | Superstore Sales Dataset |
| Records | 9,994 rows |
| Columns | 20 fields |
| Date Range | 2019 - 2022 |
| Total Revenue | $2,297,200 |
| Total Profit | $286,397 |

---

## 🗄️ Database Schema (Star Schema)
```
                    ┌──────────────┐
                    │   dim_date   │
                    │  date_key PK │
                    └──────┬───────┘
                           │
┌──────────────┐    ┌──────┴───────┐    ┌──────────────┐
│ dim_customer │    │  fact_sales  │    │  dim_product │
│customer_key  │◀───│  sales_key   │───▶│  product_key │
│customer_id   │    │  order_id    │    │  product_id  │
│segment       │    │  sales_amt   │    │  category    │
│region        │    │  profit      │    │  sub_cat     │
└──────────────┘    │  quantity    │    └──────────────┘
                    │  discount    │
┌──────────────┐    └──────┬───────┘
│ dim_shipping │           │
│shipping_key  │◀──────────┘
│ship_mode     │
└──────────────┘
```

---

## 🔄 ETL Workflow

### Extract Phase
- Reads raw CSV with proper data types
- Validates file existence and structure
- Checks all 20 required columns present
- Logs extraction metrics and null values
- Creates raw backup for auditing

### Transform Phase
- Standardizes column names (lowercase + underscores)
- Removes 1 duplicate row detected
- Converts dates to datetime format
- Converts numerics to correct types
- Creates 9 derived columns:
  - `profit_margin` - profit as % of sales
  - `delivery_days` - days from order to ship
  - `order_year`, `order_month`, `order_quarter`
  - `order_month_name`, `order_day_name`
  - `order_week`, `is_weekend`
- Validates data quality (nulls, ranges, types)

### Load Phase
- Creates star schema in SQLite
- Loads staging table (all 9,993 rows)
- Populates 4 dimension tables
- Loads fact table with foreign keys
- Validates row counts post-load

---

## 📊 Analytics Queries (10 Total)

| # | Query | Business Question |
|---|-------|------------------|
| 1 | Regional Performance | Which regions drive most revenue? |
| 2 | Top 10 Products | Best performing products? |
| 3 | Monthly Trends | How are sales trending? |
| 4 | Customer Segments | Which segments are most valuable? |
| 5 | Discount Impact | How do discounts affect profit? |
| 6 | Category Performance | Which categories drive revenue? |
| 7 | Shipping Analysis | Does shipping speed affect value? |
| 8 | State Performance | Which states are top markets? |
| 9 | Quarterly Summary | Quarter over quarter comparison? |
| 10 | Pipeline Validation | Data loaded correctly? |

---

## 🚀 Setup Instructions

### Prerequisites
- Python 3.9+
- pip
- Git

### Installation

**1. Clone the repository**
```bash
git clone https://github.com/YOUR_USERNAME/automated-sales-reporting.git
cd automated-sales-reporting
```

**2. Create virtual environment**
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
```bash
# Place sales_data.csv in data/raw/
cp /path/to/sales_data.csv data/raw/sales_data.csv
```

**6. Run the pipeline**
```bash
./scripts/run_pipeline.sh
```

---

## 🧪 Running Tests
```bash
# Run all tests
pytest -v

# Run with coverage
pytest --cov=pipeline -v
```

**Test Results:**
```
17 passed in 3.2s
- TestExtract:    4 tests ✅
- TestTransform: 10 tests ✅
- TestConfig:     3 tests ✅
```

---

## 📋 Pipeline Results

After running the pipeline:

| Table | Rows | Description |
|-------|------|-------------|
| staging_raw_sales | 9,993 | Raw data landing zone |
| dim_date | 1,432 | Unique dates |
| dim_customer | 793 | Unique customers |
| dim_product | 1,862 | Unique products |
| dim_shipping | 4 | Shipping modes |
| fact_sales | 9,993 | Sales transactions |

---

## 💡 Key Findings from Analytics

- **West region** drives highest revenue
- **Technology** category has best profit margins
- **Standard Class** shipping is most used
- **Consumer segment** generates most orders
- **High discounts (>30%)** lead to negative profits

---

## 🎯 Skills Demonstrated

- ETL pipeline development
- Dimensional data modeling (Star Schema)
- Data quality and validation
- SQL analytics and business intelligence
- Python best practices (modular design)
- Unit testing with pytest
- Shell scripting for automation
- Version control with Git
- Production-ready logging

---

## 👤 Author

**Ashrumochan Sahoo**
Data Engineer | Building data pipelines for business insights

---

*Built as part of data engineering portfolio*