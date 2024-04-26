# Databricks notebook source
# DBTITLE 1,Install Tempo
# MAGIC %pip install dbl-tempo==0.1.26 -q

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Featurization
# MAGIC
# MAGIC In this notebook, we'll add the condition based threshold monitoring defined by our field maintenance engineers to flag engines that may require an inspection. Next, we'll pull our datasets together and calculate some interesting time series features such as an exponential moving average. This poses a couple of challenges: 
# MAGIC - How do we handle null, missing, or irregular data in our time series?
# MAGIC - How do we calculate time series features such as exponential moving average in parallel on a very large dataset, without growing cost exponentially with data volume?
# MAGIC - How do we pull together our datasets when the timestamps don't line up? In this case, our inspection defect warning might get flagged hours after the sensor data is generated. We need a join that allows "price is right" rules - attach the most recent sensor data to our inspection warning data, without exceeding the inspection timestamp. This way we can identify the leading, rather than lagging, indicators for more proactive maintenance events.
# MAGIC </br>
# MAGIC
# MAGIC All of these things might require a complex, custom library specific to time series data. Luckily, Databricks has done the hard part for you! We'll use the open source library [Tempo](https://databrickslabs.github.io/tempo/) from Databricks Labs to make these challenging operations simple. First things first, we installed dbl-tempo. Next we'll get the configuration from our setup and begin creating our tables. First of all, our rules for flagging defects are straightforward to add in DLT - the anomaly detected table will serve as a source for our automated alerts system, which can send emails, slack, teams, or generic webhook messages when certain conditions are met. 

# COMMAND ----------

# DBTITLE 1,Anomaly Warnings
from util.configuration import config
import dlt
from tempo import *
from pyspark.sql.functions import col, when

@dlt.table(
    name=config['anomaly_name'],
    comment='Uses physics-based modeling to predict anomaly warnings. Feeds Databricks SQL Alerts'
)
def calculate_anomaly_rules():
    bronze = dlt.readStream(config['sensor_name'])
    return ( 
        bronze.where(
            ((col('delay') > 155) & (col('rotation_speed') > 800)) | 
            (col('temperature') > 101) |
            (col('density') > 4.6) & (col('air_pressure') < 840)  
        )  
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Note a few differences in the table we define below from the streaming tables in previous notebook - we're using dlt.read() rather than spark.readStream, meaning that we're reading from our DLT Streaming Tables into a Materialized View. In our Materialized View, we're converting each bronze dataframe to a TSDF, Tempo's time series dataframe, which utilize a ts_col (the timestamp column) and a partition column for each series (in this case, device). The TSDF interface allows us to address the problems above - interpolate missing data with the mean from the surrounding points, calculate an exponential moving average for temperature, and do our "price is right" rules join, known as an as-of join. This allows us to grab the features leading up to the defect warning, without leaking data that arrived afterwards.

# COMMAND ----------

# DBTITLE 1,Silver Inspection Table
@dlt.table(
    name=config['silver_name'],
    comment='Joins bronze sensor data with defect reports' 
)
def create_timeseries_features():
    inspections = dlt.read(config['inspection_name']).drop('_rescued_data')
    inspections_tsdf = TSDF(inspections, ts_col='timestamp', partition_cols=['device_id']) # Create our inspections TSDF
    raw_sensors = (
        dlt.read(config['sensor_name'])
        .drop('_rescued_data') # Flip the sign when negative otherwise keep it the same
        .withColumn('air_pressure', when(col('air_pressure') < 0, -col('air_pressure'))
                                    .otherwise(col('air_pressure')))
    )
    sensors_tsdf = ( 
            TSDF(raw_sensors, ts_col='timestamp', partition_cols=['device_id', 'trip_id', 'factory_id', 'model_id'])
            .EMA('rotation_speed', window=5) # Exponential moving average over five rows
            .resample(freq='1 hour', func='mean') # Resample into 1 hour intervals
    )
    return (
        inspections_tsdf # Price is right (as-of) join!
        .asofJoin(sensors_tsdf, right_prefix='sensor')
        .df # Return the vanilla Spark Dataframe
        .withColumnRenamed('sensor_trip_id', 'trip_id') # Rename some columns to match our schema
        .withColumnRenamed('sensor_model_id', 'model_id') 
        .withColumnRenamed('sensor_factory_id', 'factory_id') 
    )

# COMMAND ----------


