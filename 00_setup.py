# Databricks notebook source
# MAGIC %md
# MAGIC ### 0. Setup
# MAGIC Make sure you use a Unity Catalog enabled [Machine Learning Runtime](https://docs.databricks.com/en/compute/configure.html#databricks-runtime-versions) cluster for this setup! If you'd like, you can also select a catalog and schema by passing them as arguments to `set_config()`. _This notebook will reset any data in the schema_, create the required data objects, and write some newly generated raw files into the landing zone in Unity Catalog [Volumes](https://docs.databricks.com/en/connect/unity-catalog/volumes.html). Once the setup notebook is done (it will only take a minute or two), you can open the DLT pipeline it creates and move on to notebook one, Data Ingestion!

# COMMAND ----------

# DBTITLE 1,Install
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from util.onboarding_setup import set_config, reset_tables, dgconfig
from util.data_generator import generate_iot , land_more_data
from util.resource_creation import create_pipeline

config = set_config(dbutils, catalog='default')
reset_tables(spark, config, dbutils)
create_pipeline(config, dbutils)
land_more_data(spark, dbutils, config, dgconfig)

# COMMAND ----------


