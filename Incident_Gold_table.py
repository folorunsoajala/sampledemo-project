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
# MAGIC Create the Incident Delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.incident (
# MAGIC     IncidentID STRING,
# MAGIC     ClaimID STRING,
# MAGIC     IncidentDate DATE,
# MAGIC     IncidentType STRING,
# MAGIC     Severity STRING,
# MAGIC     Description STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/Incident.csv")


from pyspark.sql import functions as F


# Data Cleaning
df_clean = (df
    .withColumn("IncidentDate", F.to_date("IncidentDate", "yyyy/MM/dd"))
    .withColumn("IncidentType", F.trim(F.initcap("IncidentType")))
    .withColumn("Severity", F.upper(F.trim("Severity")))
)

# Save cleaned data to Delta table
df_clean.write.mode("overwrite").saveAsTable("gold.incident")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.incident
