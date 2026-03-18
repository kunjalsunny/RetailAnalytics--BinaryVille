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
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
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
bronze_path = bronze_lakehouse['properties']['abfsPath'] + "/Tables/dbo/Customer"

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# spark.sql("DROP TABLE IF EXISTS silver_customers")

# spark.sql(f"""
#     CREATE TABLE IF NOT EXISTS silver_customers(
#         customer_id STRING,
#         name STRING,
#         email STRING,
#         country STRING,
#         customer_type STRING,
#         registration_date DATE,
#         age INT,
#         gender STRING,
#         total_purchases INT,
#         customer_segment STRING,
#         days_since_registration INT,
#         last_updated TIMESTAMP
#     )
#     LOCATION '{silver_abfs}/Tables/silver_customers'
# """)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_customer_path = f"{silver_abfs}/Tables/silver_customers"

if DeltaTable.isDeltaTable(spark, silver_customer_path):
    last_processed_timestamp = spark.read.format("delta").load(silver_customer_path) \
                                    .agg(F.max("last_updated").alias("last_processed")) \
                                    .collect()[0]['last_processed']
else:
    last_processed_timestamp = None

if last_processed_timestamp is None:
    last_processed_timestamp = "1900-01-01 00:00:00.0000000"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# last_processed_df = spark.sql("SELECT MAX(last_updated) as last_processed FROM silver_customers")
# last_processed_timestamp = last_processed_df.collect()[0]['last_processed']

# if last_processed_timestamp is None:
#     last_processed_timestamp = "1900-01-01 00:00:00.0000000"

# print(last_processed_timestamp)
# print(type(last_processed_timestamp))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_abfs = "abfss://4c151600-1e5d-4d4a-8d8a-30942ae62e2c@onelake.dfs.fabric.microsoft.com/01c4f844-9da9-4244-bb27-609392cf0fb0"

customer_df = spark.read.format("delta").load(f"{bronze_abfs}/Tables/dbo/Customer")
print(f"Path being used: {bronze_abfs}/Tables/dbo/Customer")

bronze_incremental = customer_df.filter(
    F.to_timestamp(F.expr("LEFT(ingestion_timestamp, 26)"), "yyyy-MM-dd HH:mm:ss.SSSSSS") > F.lit(str(last_processed_timestamp)).cast("timestamp")
)

bronze_incremental.createOrReplaceTempView("bronze_incremental")

spark.sql("""SELECT COUNT(*) as new_records FROM bronze_incremental""").show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    SELECT 
        age,
        CAST(age AS INT) as age_cast,
        email,
        total_purchases,
        CAST(total_purchases AS INT) as purchases_cast,
        registration_date,
        CAST(registration_date AS DATE) as reg_date_cast
    FROM bronze_incremental 
    LIMIT 5
""").show(truncate=False)

spark.sql("""
    SELECT 
        COUNT(*) as total,
        SUM(CASE WHEN CAST(age AS INT) BETWEEN 18 AND 100 THEN 1 ELSE 0 END) as pass_age,
        SUM(CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END) as pass_email,
        SUM(CASE WHEN CAST(total_purchases AS INT) >= 0 THEN 1 ELSE 0 END) as pass_purchases
    FROM bronze_incremental
""").show()

spark.sql("""
    SELECT DISTINCT age, CAST(age AS INT) as age_cast
    FROM bronze_incremental
    ORDER BY age
    LIMIT 20
""").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_incremental_df = bronze_incremental.filter(
    (F.col("age").cast("int").between(18,100)) &
    (F.col("email").isNotNull()) &
    (F.col("total_purchases").cast("int")>=0)
).select(
    F.col("customer_id"),
    F.col("name"),
    F.col("email"),
    F.col("country"),
    F.col("customer_type"),
    F.col("registration_date").cast("date").alias("registration_date"),
    F.col("age").cast("int").alias("age"),
    F.col("gender"),
    F.col("total_purchases").cast("int").alias("total_purchases"),
    F.when(F.col("total_purchases").cast("int") > 1000, "High Values")
     .when((F.col("total_purchases").cast("int") > 500) & (F.col("total_purchases").cast("int") < 1000), "Medium Value")
     .otherwise("Low Value").alias("customer_segment"),
    F.datediff(F.current_date(), F.col("registration_date").cast("date")).alias("days_since_registration"),
    F.current_timestamp().alias("last_updated")
)

silver_incremental_df.createOrReplaceTempView("silver_incremental")
silver_incremental_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if DeltaTable.isDeltaTable(spark, silver_customer_path):
    spark.sql(f"""
        MERGE INTO delta.`{silver_customer_path}` AS target
        USING silver_incremental AS source
        ON target.customer_id = source.customer_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)
else:
    silver_incremental_df.write.format("delta").mode("overwrite").save(silver_customer_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.read.format("delta").load(silver_customer_path).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql(f"CREATE TABLE IF NOT EXISTS silver_customers USING DELTA LOCATION '{silver_customer_path}'")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("select count(*) from silver_customers").show()

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
