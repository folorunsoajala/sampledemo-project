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
# MAGIC CREATE TABLE IF NOT EXISTS gold.claim (
# MAGIC     ClaimID STRING,
# MAGIC     PolicyID STRING,
# MAGIC     ClaimDate DATE,
# MAGIC     ClaimType STRING,
# MAGIC     Amount DOUBLE,
# MAGIC     Status STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/InsuranceClaim.csv")

from pyspark.sql import functions as F


df_clean = (df
    .withColumn("ClaimDate", F.to_date(F.col("ClaimDate"), "dd/MM/yyyy"))
    .withColumn("ClaimType", F.initcap(F.trim("ClaimType")))
    .withColumn("Status", F.when(F.lower(F.col("Status")) == "approved", "Approved")
                           .when(F.lower(F.col("Status")) == "pending", "Pending")
                           .otherwise("Rejected"))
    .withColumn("Amount", F.col("ClaimAmount").cast("double"))
)

df_clean.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("gold.claim")




# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.claim
