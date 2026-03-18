# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "6f35a82d-c147-47bf-99aa-926d54140029",
# META       "default_lakehouse_name": "Gold_Layer",
# META       "default_lakehouse_workspace_id": "4c151600-1e5d-4d4a-8d8a-30942ae62e2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "6f35a82d-c147-47bf-99aa-926d54140029"
# META         },
# META         {
# META           "id": "cf862b4b-92b3-4a6c-be70-100f69205334"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
spark.sql("""
    CREATE OR REPLACE TABLE gold_category_sales AS
    SELECT
        o.transaction_date,
        p.category AS product_category,
        ROUND(SUM(o.total_amount), 7) AS category_total_sales
    FROM
        Silver_Layer.dbo.silver_orders o
    JOIN
        Silver_Layer.dbo.silver_products p ON o.product_id = p.product_id
    GROUP BY
        o.transaction_date,p.category
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM gold_category_sales LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE OR REPLACE TABLE Gold_Layer.dbo.gold_category_summary AS
    SELECT
        sp.category,
        ROUND(SUM(so.total_amount), 7) AS category_total_sales,
        COUNT(so.order_id) AS total_orders
    FROM Silver_Layer.dbo.silver_orders so
    JOIN Silver_Layer.dbo.silver_products sp ON so.product_id = sp.product_id
    GROUP BY sp.category
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM gold_category_summary LIMIT 10").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        COUNT(*) AS total_rows,
        COUNT(transaction_date) AS non_null_dates,
        COUNT(*) - COUNT(transaction_date) AS null_dates
    FROM Gold_Layer.dbo.gold_daily_sales
""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
