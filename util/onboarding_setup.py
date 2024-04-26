import random
import datetime
import json
from databricks.sdk import WorkspaceClient


def set_config(dbutils, catalog='default', schema=None):
    w = WorkspaceClient()
    dbx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    url = "https://" + dbx.browserHostName().get()
    notebook_path = dbx.notebookPath().get()
    folder_path = '/'.join(str(x) for x in notebook_path.split('/')[:-1])
    if not schema:
        username = w.current_user.me().user_name
        schema = 'iot_anomaly_detection_' + username.replace('@', '_').replace('.', '_')
    sensor_name = 'sensor_bronze'
    inspection_name = 'inspection_bronze'
    silver_name = 'inspection_silver'
    anomaly_name = 'anomaly_detected'
    gold_name = 'inspection_gold'
    config = {
        'catalog': catalog,
        'schema': schema,
        'checkpoints': f'/Volumes/{catalog}/{schema}/iot_checkpoints',
        'sensor_landing': f'/Volumes/{catalog}/{schema}/{sensor_name}',
        'inspection_landing': f'/Volumes/{catalog}/{schema}/{inspection_name}',
        'sensor_name': sensor_name,
        'inspection_name': inspection_name,
        'silver_name': silver_name,
        'anomaly_name': anomaly_name,
        'gold_name': gold_name,
        'sensor_table': f'{catalog}.{schema}.{sensor_name}',
        'inspection_table': f'{catalog}.{schema}.{inspection_name}',
        'silver_table': f'{catalog}.{schema}.{silver_name}',
        'anomaly_table': f'{catalog}.{schema}.{anomaly_name}',
        'gold_table': f'{catalog}.{schema}.{gold_name}'
    }
    with open(f'/Workspace/{folder_path}/util/configuration.py', 'w') as f:
        f.write("config=")
        json.dump(config, f)
    return config

def reset_tables(spark, config, dbutils):
    try:
        print('Resetting tables...')
        spark.sql(f"drop schema if exists {config['catalog']}.{config['schema']} CASCADE")
        spark.sql(f"create schema {config['catalog']}.{config['schema']}")

        sensor_volume = '.'.join(config['sensor_landing'].split('/')[2:])
        spark.sql(f"drop volume if exists {sensor_volume}")
        spark.sql(f"create volume {sensor_volume}")
        inspection_volume = '.'.join(config['inspection_landing'].split('/')[2:])
        spark.sql(f"drop volume if exists {inspection_volume}")
        spark.sql(f"create volume {inspection_volume}")
        checkpoint_volume = '.'.join(config['checkpoints'].split('/')[2:])
        spark.sql(f"drop volume if exists {checkpoint_volume}")
        spark.sql(f"create volume {checkpoint_volume}")
    except Exception as e:
        if 'NO_SUCH_CATALOG_EXCEPTION' in str(e):
            spark.sql(f'create catalog {config["catalog"]}')
            reset_tables(spark, config, dbutils)
        else:
            raise

def new_data_config(dgconfig, rows=None, devices=None, start=None, end=None, year=False, mean_temp=30):
    if not rows:
        rows = random.randint(7500, 12500)
    if not devices:
        devices = random.randint(8, 12)
    if not start:
        start = datetime.datetime(2024, 1, 1, 0, 0, 0)
    if not end:
        end = datetime.datetime(2024, 2, 1, 0, 0, 0)
    dgconfig['shared']['num_rows'] = rows
    dgconfig['shared']['num_devices'] = devices
    dgconfig['shared']['start'] = start
    dgconfig['shared']['end'] = end
    dgconfig['temperature']['lifetime']['year'] = year
    dgconfig['temperature']['lifetime']['mean'] = mean_temp
    return dgconfig


dgconfig = {
    "shared": {
        "num_rows": random.randint(390000, 410000),
        "num_devices": random.randint(58, 62),
        "start": datetime.datetime(2023, 1, 1, 0, 0, 0),
        "end": datetime.datetime(2023, 12, 31, 23, 59, 59),
        "frequency": 0.35,
        "amplitude": 1.2,
    },
    "timestamps": {
        "column_name": "timestamp",
        "minimum": 10,
        "maximum": 350,
    },
    "temperature": {
      "lifetime": {
        "column_name": "temperature",
        "noisy": 0.3,
        "trend": 0.1,
        "mean": 58,
        "std_dev": 17,
      },
      "trip": {
        "trend": -0.8,
        "noisy": 1,
      }
    },
    "air_pressure": {
      "column_name": "air_pressure",
      "depedent_on": "temperature",
      "min": 913,
      "max": 1113,
      "subtract": 15 
    },
    "lifetime": {
        "trend": 0.4,
        "noisy": 0.6,
    },
    "trip": {
        "trend": 0.2,
        "noisy": 1.2,
    },
}
