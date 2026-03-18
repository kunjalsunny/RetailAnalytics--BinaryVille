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
# META           "id": "01c4f844-9da9-4244-bb27-609392cf0fb0"
# META         },
# META         {
# META           "id": "cf862b4b-92b3-4a6c-be70-100f69205334"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

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


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_lakehouse = notebookutils.lakehouse.get("Bronze_Layer")
bronze_path = bronze_lakehouse['properties']['abfsPath'] + "/Tables/dbo/Product"

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

print(f"Silver path: {silver_abfs}")

silver_product_path = f"{silver_abfs}/Tables/dbo/silver_products"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# CODE FOR INCREMENTAL LOADING OF DATA FROM BRONZE TO SILVER
# spark.sql("""
# CREATE TABLE IF NOT EXISTS silver_products(
#     product_id STRING,
#     name STRING,
#     category STRING,
#     brand STRING,
#     price DOUBLE,
#     stock_quantity INT,
#     rating DOUBLE,
#     is_active BOOLEAN,
#     price_category STRING,
#     stock_status STRING,
#     last_updated TIMESTAMP
#     )
# USING DELTA
# """)

# from datetime import datetime

# silver_product_path = f"{silver_abfs}/Tables/silver_products"

# if DeltaTable.isDeltaTable(spark, silver_product_path):
#     last_processed_timestamp = spark.read.format("delta").load(silver_product_path) \
#                                     .agg(F.max("last_updated").alias("last_processed")) \
#                                     .collect()[0]['last_processed']
# else:
#     last_processed_timestamp = None

# if last_processed_timestamp is None:
#     last_processed_timestamp = datetime(1900, 1, 1, 0, 0, 0)

# bronze_abfs = "abfss://4c151600-1e5d-4d4a-8d8a-30942ae62e2c@onelake.dfs.fabric.microsoft.com/01c4f844-9da9-4244-bb27-609392cf0fb0"

# product_df = spark.read.format('delta').load(f'{bronze_abfs}/Tables/dbo/Product')
# print(f"Path being used: {bronze_abfs}/Tables/dbo/Customer")


# bronze_incremental = product_df.filter(
#     F.to_timestamp(F.col("ingestion_timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'") < F.lit(str(last_processed_timestamp)).cast("timestamp")
#     # F.col("last_updated") > F.lit(last_processed_timestamp)
# )

# bronze_incremental.createOrReplaceTempView("bronze_incremental")

# spark.sql("""
#     SELECT COUNT(*) as new_records FROM bronze_incremental
# """).show()

# product_df.select(
#     "ingestion_timestamp",
#     F.expr("LEFT(ingestion_timestamp, 26)").alias("trimmed"),
#     F.to_timestamp(F.expr("LEFT(ingestion_timestamp, 26)"), "yyyy-MM-dd HH:mm:ss.SSSSSS").alias("parsed")
# ).show(5, truncate=False)

# silver_incremental_df = bronze_incremental.select(
#     F.col("product_id"),
#     F.col("name"),
#     F.col("category"),
#     F.col("brand"),
#     F.col("price").cast("double").alias("price"),
#     F.col('stock_quantity').cast("int").alias("stock_quantity"),
#     F.when(F.col("rating").cast("double") < 0, 0)
#     .when(F.col("rating").cast("double") > 5, 5)
#     .otherwise(F.col("rating").cast("double")).alias("rating"),
#     F.col("is_active").cast("boolean").alias("is_active"),
#     F.when(F.col('price').cast("double") > 500, "Premium")
#     .when((F.col('price').cast("double")> 100) & (F.col('price').cast("double")< 500), "Standard")
#     .otherwise('Budget').alias("price_category"),
#     F.when(F.col("stock_quantity").cast("int") == 0, "Out Of Stock")
#     .when((F.col("stock_quantity").cast("int") < 100) & (F.col("stock_quantity").cast("int") > 0),"Low Stock")
#     .when((F.col("stock_quantity").cast("int") > 100) & (F.col("stock_quantity").cast("int") < 500), "Moderate Stock")
#     .otherwise("Sufficient Stock").alias("stock_status"),
#     F.current_timestamp().alias("last_updated")
# )

# silver_incremental_df.createOrReplaceTempView("silver_incremental_products")
# print(f"Records in {silver_incremental_df.count()}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_incremental_df = bronze_df.select(  
    F.col("product_id"),
    F.col("name"),
    F.col("category"),
    F.col("brand"),
    F.col("price").cast("double").alias("price"),
    F.col('stock_quantity').cast("int").alias("stock_quantity"),
    F.when(F.col("rating").cast("double") < 0, 0)
    .when(F.col("rating").cast("double") > 5, 5)
    .otherwise(F.col("rating").cast("double")).alias("rating"),
    F.col("is_active").cast("boolean").alias("is_active"),
    F.when(F.col('price').cast("double") > 500, "Premium")
    .when((F.col('price').cast("double")> 100) & (F.col('price').cast("double")< 500), "Standard")
    .otherwise('Budget').alias("price_category"),
    F.when(F.col("stock_quantity").cast("int") == 0, "Out Of Stock")
    .when((F.col("stock_quantity").cast("int") < 100) & (F.col("stock_quantity").cast("int") > 0),"Low Stock")
    .when((F.col("stock_quantity").cast("int") > 100) & (F.col("stock_quantity").cast("int") < 500), "Moderate Stock")
    .otherwise("Sufficient Stock").alias("stock_status"),
    F.current_timestamp().alias("last_updated")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS silver_products
    USING DELTA
    LOCATION '{silver_product_path}'
""")

print("Table registered")
spark.sql("SELECT COUNT(*) FROM silver_products").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_incremental_df.createOrReplaceTempView("silver_incremental_products")

if DeltaTable.isDeltaTable(spark, silver_product_path):
    spark.sql(f"""
        MERGE INTO delta.`{silver_product_path}` AS target
        USING silver_incremental_products AS source
        ON target.product_id = source.product_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
    print("MERGE complete")
else:
    silver_incremental_df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable("silver_products")
    print("Initial load complete")

spark.read.format("delta").load(silver_product_path).limit(10).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format("delta").load(silver_product_path).limit(10).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format("delta").load(silver_product_path).count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
