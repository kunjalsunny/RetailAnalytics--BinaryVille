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
    CREATE OR REPLACE TABLE gold_daily_sales AS
    SELECT 
        transaction_date,
        ROUND(SUM(total_amount), 11) AS daily_total_sales
    FROM
        Silver_Layer.dbo.silver_orders
    GROUP BY
        transaction_date
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) FROM gold_daily_sales").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT * FROM gold_daily_sales LIMIT 10").show()

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
