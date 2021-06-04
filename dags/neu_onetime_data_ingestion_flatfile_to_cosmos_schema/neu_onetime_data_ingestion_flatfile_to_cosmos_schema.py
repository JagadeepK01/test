import glob
import json
import os
import sys
import uuid
import sqlite3
from datetime import datetime, timedelta


import airflow
import numpy as np
import pandas as pd
from airflow import DAG, settings
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from operators.neu_dataops_blob_operator import BlobOperator
from operators.neu_dataops_cosmos_operator import NeuDataCosmosDBOperator
from operators.neu_dataops_oracle_cloud_operator import \
    NeuDataOpsOracleCloudOperator
from typing_extensions import final

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import config


def download_data(table_name, **kwargs):
    """ 
        downloar the extracted data to blob storage
        :param str table_name:
            tables to be extracted
        :param str kwargs:
            additional parameters passed dynamically
        :return:
            extracted data
        :rtype: parquet
    """
    os.system("rm -rf {}{}".format(config.temp_file_path_download, config.flatfile_path))
    client = BlobOperator(config.connection_string, config.container_name)
    client.download(config.flatfile_path + "/JSON/", config.temp_file_path_download)
    client.download(config.flatfile_path + "/CSV/", config.temp_file_path_download)



def load_cosmosdb(database_name, container_name,  **kwargs):
    """ 
        insert the data into cosmos db & put logger in Logging db
        :param str database_name:
            database to be used for insertion
        :param str database_name:
            database to be used for insertion
        :param str kwargs:
            additional parameters passed dynamically
        :return:
            extracted data
        :rtype: parquet
    """
    _client = NeuDataCosmosDBOperator(config.uri, config.key)
    connection = _client.GetCosmosConnection()
    database = _client.CreateDatabase(client=connection, database_name=database_name)
    _client._CreateContainer(container_name=container_name, paths = "/JobID", database=database)
    database = connection.get_database_client(database_name)
    container = database.get_container_client(container_name)
    created_sproc = container.scripts.get_stored_procedure('BulkUpload')
    all_files = glob.glob(config.temp_file_path_download + "/CSV/*")
    for files in all_files:
        for df in pd.read_csv(files, chunksize=500):
            df['JobID'] = str(uuid.uuid4())
            final_json = [json.loads(df.to_json(orient='records'))]
            container.scripts.execute_stored_procedure(sproc=created_sproc,params=final_json, partition_key=df['JobID'].iloc[0])
    
    database = _client.CreateDatabase(client=connection, database_name="Logging")
    _client._CreateContainer(container_name="Logger", paths = "/JobID", database=database)
    database = connection.get_database_client("Logging")
    container = database.get_container_client("Logger")
    created_sproc = container.scripts.get_stored_procedure('BulkUpload')
    ID = str(uuid.uuid4())
    record = [
        [
            {
                "JOB_EXECUTION_ID": 7,
                "JOB_RUN_ID": "manual__" +  str(datetime.now()),
                "JOB_NAME": "neu_onetime_data_ingestion_flatfile_to_cosmos_schema",
                "TASK_ID": "flatfile_to_cosmos_migration_" + container_name,
                "START_DATETIME": str(datetime.now()),
                "END_DATETIME": str(datetime.now()),
                "JOB_TYPE": "onetime_ingestion",
                "JOB_STATUS": "success",
                "SOURCE_RECORD_COUNT": 'null',
                "DESTINATION_RECORD_COUNT": 'null',
                "JOB_MESSAGE": 'null',
                "SOURCE_QUERY": 'null',
                "DESTINATION_QUERY": 'null',
                "JobID": ID
            }
        ]
    ]
    # container.scripts.execute_stored_procedure(sproc=created_sproc,params=record, partition_key=ID)


load_cosmos_task = {}
download_data_task = {}
TABLE_NAME = ['FlatFiles']
schedule = None
tables = None


con = sqlite3.connect('/home/azureuser/airflow/Flask/flask_data.db')
df = pd.read_sql_query("SELECT * from jobs", con)
df = df.loc[df['source'].str.contains('adls')]
if df.shape[0] > 0:
    if df.schedule.iloc[0] and df.schedule.iloc[0] != '':
        schedule = df.schedule.iloc[0]
    if df.source_tables.iloc[0] and df.source_tables.iloc[0] != '':
        tables = df.source_tables.iloc[0]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['jai.janyani@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    'neu_onetime_data_ingestion_flatfile_to_cosmos_schema_job',
    default_args=default_args,
    schedule_interval=schedule,
)



start_task = DummyOperator(
    task_id='start',
    dag=dag
)

end_task = DummyOperator(
    task_id='end',
    dag=dag
)


for each in TABLE_NAME:
    download_data_task[each] = PythonOperator(
        task_id=('download_data_datalake_' + each).lower(),
        provide_context=False,
        python_callable=download_data,
        op_kwargs={'table_name': each},
        dag=dag)
    load_cosmos_task[each] = PythonOperator(
        task_id=('insert_data_cosmos_' + each).lower(),
        provide_context=False,
        python_callable=load_cosmosdb,
        op_kwargs={'database_name': "MigratedOracleDB", 'container_name' : each},
        dag=dag)


for i in TABLE_NAME:
    start_task >> download_data_task[i]
    download_data_task[i] >> load_cosmos_task[i]
    load_cosmos_task[i] >> end_task
