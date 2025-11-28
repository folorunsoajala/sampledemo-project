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
# MAGIC CREATE OR REPLACE TABLE policy (
# MAGIC     PolicyNo STRING,
# MAGIC     CustomerID STRING,
# MAGIC     ProductID STRING,
# MAGIC     AgentID STRING,
# MAGIC     StartDate DATE,
# MAGIC     EndDate DATE,
# MAGIC     Status STRING
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/Policy.csv")

from pyspark.sql import functions as F

df_policy_clean = (
    df
    .withColumn("StartDate", F.to_date(F.col("StartDate"), "yyyy-MM-dd"))
    .withColumn("EndDate", F.to_date(F.col("EndDate"), "dd/MM/yyyy"))
    .withColumn("Status", 
        F.when(F.lower(F.col("Status")) == "active", "Active")
         .when(F.lower(F.col("Status")) == "pending", "Pending")
         .otherwise("Inactive")
    )
)

df_policy_clean.write.mode("overwrite").saveAsTable("gold.policy")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.policy
