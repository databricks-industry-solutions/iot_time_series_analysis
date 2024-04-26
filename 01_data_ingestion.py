# Databricks notebook source
# MAGIC %md 
# MAGIC ### 1. Data Ingestion
# MAGIC
# MAGIC In this notebook, we create two [Streaming Tables](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html) that read the newly arrived data that's landing in your UC Volume. For your use case, you may point your stream to receive events from a message queue like Kafka. We'll run our process manually, but you can use a schedule or a [file arrivel trigger](https://docs.databricks.com/en/workflows/jobs/file-arrival-triggers.html) for incremental batch processing. You can run the same pipeline in continuous mode for real-time processing. For more details about the differences, skim our [documentation](https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-streaming-table.html). First thing's first, we'll get the configuration we used in the setup notebook. Be sure you've run the setup!

# COMMAND ----------

# MAGIC %md
# MAGIC To get started running the DLT code below, you can simply connect to the DLT Pipeline as your compute resource in the top right and hit shift+enter in the DLT cell to validate the output of the table. To actually run the pipeline, you can click "start" in the top right once you've connected to the DLT Pipeline, or open the pipeline menu in a new page and run it from there.
# MAGIC </br></br>
# MAGIC For our first table, we've got raw CSV files landing in cloud storage, in this case in a Unity Catalog Volume. By using a streaming table we ensure our ETL is _incremental_, meaning we process each row exactly once. By only operating on newly arrived data, we eliminate the cost of re-processing rows we've already seen. Another convenient feature we make use of is Autoloader, the Databricks approach for incrementally reading files from cloud storage (thus the 'cloudFiles' format below). By providing schema hints for Autoloader we get three benefits: 
# MAGIC - We cast columns to types if possible, which can be more efficient than reading everything as a string when we're confident about the type of a column
# MAGIC - Any columns that don't match our schema hints get saved in the '_rescued_data' column, which means we can continue processing of valid data and reprocess invalid data later
# MAGIC - We can infer the types of columns we're unsure of later, providing flexibility to handle changing schemas
# MAGIC </br></br>The schema inference and rescued data capabilities of Autoloader particularly come in handy when we have upstream producers of data that change the schema of the data without warning, which is unfortunately common with other teams or third party vendors. Now we've got an approach for handling it! To learn more, try our [schema inference and evolution documentation](https://docs.databricks.com/en/ingestion/auto-loader/schema.html). Without further ado, let's define our first table

# COMMAND ----------

# DBTITLE 1,Bronze Sensor Table
from util.configuration import config
import dlt
from pyspark.sql.functions import col

@dlt.table(
    name=config['sensor_name'],
    comment='Loads raw sensor data into the bronze layer'
)
@dlt.expect("valid pressure", "air_pressure > 0") # This line flags any values for air_pressure which are negative
def autoload_sensor_data(): 
    schema_hints = 'device_id integer, trip_id integer, factory_id string, model_id string, timestamp timestamp, airflow_rate double, rotation_speed double, air_pressure float, temperature float, delay float, density float' # Provide hints as to what we expect the schema to be
    return ( 
        spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option('cloudFiles.schemaHints', schema_hints)
        .option('cloudFiles.schemaLocation', f"{config['checkpoints']}/sensor")
        .load(f"{config['sensor_landing']}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Next, we'll do the same with the landing zone for our inspection warnings data. Given some set of behavior, we can get defect warnings from the edge. Since the defects don't get flagged immediately, we'll want to put these datasets together and leverage them to make forward looking predictions about defective behavior in order to be more proactive about maintenance events in the field.

# COMMAND ----------

# DBTITLE 1,Bronze Inspection Table
@dlt.table(
    name=config['inspection_name'],
    comment='Loads raw inspection files into the bronze layer'
) # Drops any rows where timestamp or device_id are null, as those rows wouldn't be usable for our next step
@dlt.expect_all_or_drop({"valid timestamp": "`timestamp` is not null", 
                         "valid device id": "device_id is not null"}) 
def autoload_inspection_data():                                  
    schema_hints = 'defect float, timestamp timestamp, device_id integer'
    return (
        spark.readStream.format('cloudFiles')
        .option('cloudFiles.format', 'csv')
        .option('cloudFiles.schemaHints', schema_hints)
        .option('cloudFiles.schemaLocation', f"{config['checkpoints']}/inspection")
        .load(f"{config['inspection_landing']}")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC Check out the DLT graph for real time updates of data quality!

# COMMAND ----------


