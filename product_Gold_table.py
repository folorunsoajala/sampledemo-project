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
# MAGIC CREATE TABLE IF NOT EXISTS gold.product (
# MAGIC     ProductID STRING,
# MAGIC     ProductName STRING,
# MAGIC     Category STRING,
# MAGIC     Premium DOUBLE,
# MAGIC     CoverageType STRING,
# MAGIC     DurationMonths INT
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/product.csv")

from pyspark.sql import functions as F


df_clean = (df
    .withColumn("Premium", F.col("Premium").cast("double"))
    .withColumn("ProductName", F.initcap(F.trim("ProductName")))
    .withColumn("Category", F.initcap(F.trim("Category")))
    .withColumn("CoverageType", F.initcap(F.trim("CoverageType")))
    .withColumn("DurationMonths", F.col("DurationMonths").cast("int"))
)

df_clean.write.mode("overwrite").saveAsTable("gold.product")




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.product
