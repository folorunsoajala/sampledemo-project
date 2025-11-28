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
# MAGIC CREATE TABLE IF NOT EXISTS gold.customer (
# MAGIC     CustomerID STRING,
# MAGIC     FullName STRING,
# MAGIC     Gender STRING,
# MAGIC     DOB DATE,
# MAGIC     City STRING,
# MAGIC     Country STRING,
# MAGIC     Email STRING,
# MAGIC     Phone STRING,
# MAGIC     JoinDate DATE
# MAGIC )
# MAGIC USING DELTA;
# MAGIC

# COMMAND ----------

# load the data from the silver layer 

df = spark.read.option("header", "true").csv("abfss://landingzone@sampledemos.dfs.core.windows.net/customer.csv")

from pyspark.sql import functions as F


# Data cleaning
df_clean = (df
    # Convert DOB and JoinDate to date
    .withColumn("DOB", F.to_date("DOB", "dd/MM/yyyy"))
    .withColumn("JoinDate", F.to_date("JoinDate", "dd/MM/yyyy"))

    # Standardize Gender
    .withColumn("Gender", F.when(F.upper(F.col("Gender")) == "MALE", "Male")
                            .when(F.upper(F.col("Gender")) == "FEMALE", "Female")
                            .otherwise("Unknown"))

    # Fix invalid email formats
    .withColumn("Email", F.when(F.col("Email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), F.col("Email"))
                          .otherwise(None))

    # Fix city names and casing
    .withColumn("City", F.initcap(F.trim("City")))
    .withColumn("Country", F.initcap(F.trim("Country")))
    .withColumn("FullName", F.initcap(F.trim("FullName")))

    # Convert phone to string
    .withColumn("Phone", F.col("Phone").cast("string"))
)

# Write clean data to Delta table
df_clean.write.mode("overwrite").saveAsTable("gold.customer")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.customer
