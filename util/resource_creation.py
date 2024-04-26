import json
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import sql as dbsql
from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

def create_pipeline(config, dbutils, serverless=True, tried=0):
    dbx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    url = "https://" + dbx.browserHostName().get()
    notebook_path = dbx.notebookPath().get()
    folder_path = '/'.join(str(x) for x in notebook_path.split('/')[:-1])
    w = WorkspaceClient()
    pipeline_name = 'iot_anomaly_detection_' + w.current_user.me().user_name.replace('@', '_').replace('.', '_')
    try:
        pipeline_id = w.pipelines.create(name=pipeline_name, catalog=config['catalog'], target=config['schema'], 
                                    serverless=serverless, channel='Preview', continuous=False, development=True,
                    libraries=[PipelineLibrary(notebook=NotebookLibrary(path=folder_path+'/01_data_ingestion')), 
                                PipelineLibrary(notebook=NotebookLibrary(path=folder_path+'/02_featurization')),
                                PipelineLibrary(notebook=NotebookLibrary(path=folder_path+'/03_aggregated_metrics'))]
                    ).pipeline_id
        pipeline_url = f'{url}/#joblist/pipelines/{pipeline_id}'
        print('DLT Pipeline created at ' + pipeline_url)
    except Exception as e:
        if 'pipeline name' in str(e):
            print(f'Pipeline already exists, skipping that step. Go to pipeline {url}/#joblist/pipelines and search {pipeline_name} to view it')
        elif tried==0 and serverless:
            create_pipeline(config, dbutils, serverless=False, tried=1)
        else:
            print('Error creating DLT pipeline, please get access to run the demo')
            raise e

def insert_dashboard_tables(dashboard_str, config):
    dashboard_str = dashboard_str.replace('<inspection_table>', config['inspection_table'])
    dashboard_str = dashboard_str.replace('<sensor_table>', config['sensor_table'])
    dashboard_str = dashboard_str.replace('<silver_table>', config['silver_table'])
    dashboard_str = dashboard_str.replace('<gold_table>', config['gold_table'])
    return dashboard_str

def create_lakeview_dashboard(config, dbutils):
    dbx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    url = "https://" + dbx.browserHostName().get()
    notebook_path = dbx.notebookPath().get()
    folder_path = '/'.join(str(x) for x in notebook_path.split('/')[:-1])
    w = WorkspaceClient()
    try:
        with open(f'/Workspace/{folder_path}/config/IOT Anomaly Detection.lvdash.json', 'r') as file:
            dashboard_specs = json.dumps(json.load(file))
        dashboard_specs = insert_dashboard_tables(dashboard_specs, config)
        pipeline_name = 'iot_anomaly_detection'
        w.workspace.mkdirs(f'/Workspace/{folder_path}/dashboard/')
        dash = w.lakeview.create(display_name=pipeline_name, parent_path=f'/Workspace/{folder_path}/dashboard/', 
                                 serialized_dashboard=dashboard_specs)
        return dash.dashboard_id
    except Exception as e:
        if 'already exists' in str(e):
            print('Lakeview dashboard already exists, skipping that step. You can view it in the dashboard folder')
        else:    
            print('Error creating lakeview dashboard, please import config/IOT Anomaly Detection.lvdash.json' +  
                  ' in Lakeview to see the demo')
            raise e

def create_sql_alert(config):
    w = WorkspaceClient()
    warehouses = w.warehouses.list()
    serverless_warehouses = [warehouse for warehouse in warehouses if warehouse.enable_serverless_compute==True]
    if len(serverless_warehouses) > 0:
        warehouse_id = serverless_warehouses[0].id
    elif len(warehouses) > 0:
        warehouse_id = warehouses[0].id
    else:
        try:
            print('No access to SQL warehouse - attempting to create one...')
            warehouse_id = w.warehouses.create_and_wait(name="iot_anomaly_detection", cluster_size="X-Small", 
                                        enable_serverless_compute=True, max_num_clusters=1).id
        except Exception as e:
            print('Error creating SQL warehouse, try creating manually for the full demo')
    try:
        query = f'''
SELECT count(*) as defects_count
FROM {config['anomaly_table']}
WHERE `timestamp` > current_timestamp() - INTERVAL 10 MINUTES
        '''
        name = 'iot_anomaly_detection_' + w.current_user.me().user_name.replace('@', '_').replace('.', '_')
        query_id = w.queries.create(name=name, query=query).id
        options = dbsql.AlertOptions(column="defects_count", op=">", value="0", muted="True", 
                            custom_subject="Defects detected", custom_body="The number of potential defects detected")
        alert_id = w.alerts.create(name, query_id=query_id, options=options)
        return alert_id.id
    except Exception as e:
        print('Error creating SQL alert, try creating manually for the full demo')

def create_sql_assets(config, dbutils):
    dbx = dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
    url = "https://" + dbx
    dash_id = create_lakeview_dashboard(config, dbutils)
    alert_id = create_sql_alert(config)
    if dash_id is not None:
        print(f'You can view the dashboard at: {url}/dashboardsv3/{dash_id}')
    if alert_id is not None:
        print(f'You can view the alert at: {url}/sql/alerts/{alert_id}')
