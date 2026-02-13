import sqlite3
import pandas as pd
import os

conn = sqlite3.connect('data/sales_analytics.db')

query = """
SELECT
    f.order_id,
    f.sales_amount,
    f.profit,
    f.quantity,
    f.discount,
    d.full_date,
    d.year        AS order_year,
    d.month       AS order_month,
    d.month_name  AS order_month_name,
    d.quarter     AS order_quarter,
    d.day_name    AS order_day_name,
    d.is_weekend,
    c.customer_id,
    c.customer_name,
    c.segment,
    c.city,
    c.state,
    c.region,
    c.country,
    p.product_id,
    p.product_name,
    p.category,
    p.sub_category,
    s.ship_mode
FROM fact_sales f
JOIN dim_date     d ON f.order_date_key = d.date_key
JOIN dim_customer c ON f.customer_key = c.customer_key
JOIN dim_product  p ON f.product_key  = p.product_key
JOIN dim_shipping s ON f.shipping_key = s.shipping_key
"""

df = pd.read_sql_query(query, conn)
conn.close()

os.makedirs('data/tableau_export', exist_ok=True)

df.to_csv('data/tableau_export/sales_dashboard_data.csv', index=False)

print('=== TABLEAU EXPORT COMPLETE ===')
print(f'Rows exported  : {len(df):,}')
print(f'Columns        : {len(df.columns)}')
print(f'Saved to       : data/tableau_export/sales_dashboard_data.csv')
print()
print('Columns available in Tableau:')
for col in df.columns:
    print(f'  - {col}')