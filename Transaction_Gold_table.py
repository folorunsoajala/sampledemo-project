# Databricks notebook source
# MAGIC %md
# MAGIC First create another container (Gold layer) on your adls and create external location on databricks 

# COMMAND ----------

# MAGIC %md
# MAGIC You need to add a storage path to your catalog creation command — something like this:

# COMMAND ----------

# Create Catalog and Schema
spark.sql("""
CREATE CATALOG IF NOT EXISTS safe_sure_catalog
MANAGED LOCATION 'abfss://gold-layer@sampledemos.dfs.core.windows.net/'
""")

# Switch to the Catalog
spark.sql("USE CATALOG safe_sure_catalog")

# Create Schema (use either DATABASE or SCHEMA — they are synonyms in Databricks)
spark.sql("CREATE SCHEMA IF NOT EXISTS gold")
spark.sql("USE gold")


# COMMAND ----------

# MAGIC %md
# MAGIC Create the agent Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG safe_sure_catalog;
# MAGIC USE SCHEMA gold;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE transaction (
# MAGIC     TransactionID STRING,
# MAGIC     PolicyNo STRING,
# MAGIC     CustomerID STRING,
# MAGIC     ProductID STRING,
# MAGIC     TransactionDate DATE,
# MAGIC     Amount DECIMAL(18,2),
# MAGIC     PaymentMethod STRING,
# MAGIC     Channel STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/Transaction.csv")

from pyspark.sql import functions as F


df_transaction_clean = (
    df
    .withColumn("TransactionDate", F.to_date(F.col("TransactionDate"), "yyyy/MM/dd"))
    .withColumn("Amount", F.regexp_replace("Amount", "[₦,]", "").cast("double"))
    .withColumn("PaymentMethod", F.initcap(F.trim(F.col("PaymentMethod"))))
    .withColumn("Channel", F.initcap(F.trim(F.col("Channel"))))
    # rename to avoid Spark confusing columns
    .select(
        F.col("TransactionID").alias("TransactionID"),
        F.col("PolicyNo").alias("PolicyNo"),
        F.col("CustomerID").alias("CustomerID"),
        F.col("ProductID").alias("ProductID"),
        F.col("TransactionDate").alias("TransactionDate"),
        F.col("Amount").alias("Amount"),
        F.col("PaymentMethod").alias("PaymentMethod"),
        F.col("Channel").alias("Channel")
    )
)

df_transaction_clean.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.transaction")






# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.transaction
