# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "cf862b4b-92b3-4a6c-be70-100f69205334",
# META       "default_lakehouse_name": "Silver_Layer",
# META       "default_lakehouse_workspace_id": "4c151600-1e5d-4d4a-8d8a-30942ae62e2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "cf862b4b-92b3-4a6c-be70-100f69205334"
# META         },
# META         {
# META           "id": "01c4f844-9da9-4244-bb27-609392cf0fb0"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
from pyspark.sql.functions import col, when
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import notebookutils

notebookutils.lakehouse.list()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lakehouse = notebookutils.lakehouse.get("Bronze_Layer")
bronze_path = bronze_lakehouse['properties']['abfsPath'] + "/Tables/dbo/Orders"

print(bronze_path)

bronze_df = spark.read.format("delta").load(bronze_path)
bronze_df.show(2,truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_lakehouse = notebookutils.lakehouse.get("Silver_Layer")
silver_abfs = silver_lakehouse['properties']['abfsPath']
silver_orders_path = f"{silver_abfs}/Tables/dbo/silver_orders"

print(f"Silver path: {silver_abfs}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_incremental_df = bronze_df.select(
    F.col("transaction_id").alias("order_id"),
    F.col("customer_id"),
    F.col("product_id"),
    F.when(F.col("quantity") < 0, 0)
    .otherwise(F.col("quantity")).alias("quantity"),
    F.when(F.col("total_amount").cast("double") < 0, 0)
    .otherwise(F.col("total_amount").cast("double")).alias("total_amount"),
    F.col("transaction_date").cast("date").alias("transaction_date"),
    F.when((F.col("quantity") == 0) & (F.col("total_amount") == 0), "Cancelled")
    .when((F.col("quantity") > 0) & (F.col("total_amount") > 0), "Completed")
    .otherwise("In Progress").alias("order_status"),
    F.col("payment_method"),
    F.col("store_type"),
    F.current_timestamp().alias("stock_status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_incremental_df.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(silver_orders_path)  


spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_orders
    USING DELTA
    LOCATION '{silver_orders_path}'
""")

print("Table registered")
spark.sql("SELECT COUNT(*) FROM silver_orders").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format("delta").load(silver_orders_path).limit(10).show()

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
