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
# MAGIC Create the renewal Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.renewal (
# MAGIC     RenewalID STRING,
# MAGIC     PolicyNo STRING,
# MAGIC     RenewalDate DATE,
# MAGIC     Amount DOUBLE,
# MAGIC     PaymentMethod STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/Renewal.csv")

from pyspark.sql import functions as F


# Cleaning and Transformation
df_clean = (df
    .withColumn("RenewalDate", F.coalesce(
        F.to_date("RenewalDate", "yyyy/MM/dd"),
        F.to_date("RenewalDate", "dd/MM/yyyy")
    ))
    .withColumn("Amount", F.regexp_replace("Amount", ",", "").cast("double"))
    .withColumn("PaymentMethod", F.initcap(F.trim("PaymentMethod")))
)

# Write clean data to Delta table
df_clean.write.mode("overwrite").saveAsTable("gold.renewal")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.renewal
