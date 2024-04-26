# Databricks notebook source
# MAGIC %md
# MAGIC ### 4. Actionable Insights
# MAGIC Make sure you use a Unity Catalog enabled Machine Learning Runtime cluster to run the final setup! This notebook will create a dashboard, an alert, and an example of landing more data which we can process incrementally with our new end-to-end pipeline. The dashboard can be used to identify and surface insights using the results of our pipeline, while the alert can be configured to notify stakeholders to take action after some threshold in anomalies is met. First off, let's install the Databricks SDK and import the required functions

# COMMAND ----------

# DBTITLE 1,Install SDK
# MAGIC %pip install databricks-sdk==0.24.0 -q
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

# DBTITLE 1,Import Libraries
from util.onboarding_setup import dgconfig, new_data_config
from util.data_generator import generate_iot, land_more_data
from util.resource_creation import create_sql_assets
from util.configuration import config

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we have our ETL pipeline built out, let's surface some results! Run the cell below and check the dashboards folder for your Lakeview Dashboard. Feel free to edit or move the widgets and click publish to share your insights with others. You can also schedule the dashboard to email users with fresh insights at regular intervals. The dashboard makes it easy to see our defect rate by factory or relate defect rates to variables like Density or Temperature.
# MAGIC
# MAGIC We've also created a SQL Alert titled `iot_anomaly_detection_<username>` in your Home folder and linked it below. This alert will notify the end users you specify when our anomaly warnings are triggered (note that `you'll need to refresh the alert` first by using a schedule or the button in the top right of the UI). If you run your pipeline continuously, you can set the alert to refresh as frequently as you'd like for real time alerting.

# COMMAND ----------

# DBTITLE 1,Install Dashboard and Alerting
create_sql_assets(config, dbutils) 

# COMMAND ----------

# MAGIC %md
# MAGIC In the next cell we'll land about 10,000 more rows worth of raw data files in the landing zone. Try running the pipeline again and notice the number of rows processed by our autoloader streaming tables - it's only the newly arrived data, meaning we don't waste compute (and therefore cost!) reprocessing data we've already seen. You can see the data arrive immediately by refreshing the dashboard

# COMMAND ----------

# DBTITLE 1,Land More Data
new_dgconfig = new_data_config(dgconfig)
land_more_data(spark, dbutils, config, new_dgconfig)

# COMMAND ----------

# MAGIC %md
# MAGIC If you run your pipeline again, you'll incrementally ingest the new data. You might see in the DLT event log that some Materialized Views while run as COMPLETE_RECOMPUTE while others show something like GROUP_AGGREGATE or PARTITION_OVERWRITE - what's happening here is DLT automatically determines the most efficient way to get your results, and if there's a shortcut available DLT will take it in order to reduce processing time. For example, consider the scenario where only some devices are updated. Wouldn't it be faster if we avoided re-computing the aggregations in our gold table for devices that didn't receive an update? With Materialized Views, we don't have to worry about answering that question. Out of the box we get simple, great performance.

# COMMAND ----------


