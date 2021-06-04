import glob
import json
import os
import sys
import uuid
from datetime import datetime, timedelta

import airflow
import numpy as np
import pandas as pd
from airflow import DAG, settings
import pandas as pd
import sqlite3
from sqlalchemy import create_engine
from airflow.models import Connection
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from operators.neu_dataops_blob_operator import BlobOperator
from operators.neu_dataops_cosmos_operator import NeuDataCosmosDBOperator
from operators.neu_dataops_oracle_cloud_operator import \
    NeuDataOpsOracleCloudOperator

sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import config


def get_configuration(ds, key, **kwargs):
    """ 
        get configuration for extra parameters
        :param str key:
            key value for the function
        :param str kwargs:
            additional parameters passed dynamically
        :return:
            value for the key
        :rtype: object
    """
    return key


def extract_oracle(table_name, chunks, **kwargs):
    """ 
        extract data from oracle db for provided tables
        :param str table_name:
            tables to be extracted
        :param str kwargs:
            additional parameters passed dynamically
        :return:
            extracted data
        :rtype: parquet
    """
    os.system(config.initialize_oracle_sting)
    os.system("rm {}/*".format(config.temp_file_path + str(table_name)))
    client = NeuDataOpsOracleCloudOperator(username = config.user, password = config.password, dsn_string = config.dsn)
    client.InitializeOracleLibraries(config.lib_path)
    connection = client.GetOracleConnection(config.database)
    df = client.GetQueryOutputDataframeChunks(table_name = table_name, connection=connection, chunks=chunks)
    for num, chunk in enumerate(df):
        filename = "part_" + str(num) + ".parquet"
        os.makedirs(config.temp_file_path + str(table_name), exist_ok=True)
        chunk.to_csv(config.temp_file_path + str(table_name) + "/" + str(filename), index=False)



def upload_data(table_name, **kwargs):
    """ 
        upload the extracted data to blob storage
        :param str table_name:
            tables to be extracted
        :param str kwargs:
            additional parameters passed dynamically
        :return:
            extracted data
        :rtype: parquet
    """
    client = BlobOperator(config.connection_string, config.container_name)
    client.rmdir(table_name)
    client.upload(config.temp_file_path + table_name, table_name)



def load_cosmosdb(database_name, container_name, tpu,  **kwargs):
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
    database = _client.CreateDatabase(client=connection, database_name=database_name, tpu=tpu)
    _client._CreateContainer(container_name=container_name, paths = "/JobID", database=database)
    database = connection.get_database_client(database_name)
    container = database.get_container_client(container_name)
    created_sproc = container.scripts.get_stored_procedure('BulkUpload')
    all_files = glob.glob(config.temp_file_path + str(container_name) + "/*")
    for files in all_files:
        for df in pd.read_csv(files, chunksize=100):
            df['JobID'] = str(uuid.uuid4())
            final_json = [json.loads(df.to_json(orient='records'))]
            try:
                container.scripts.execute_stored_procedure(sproc=created_sproc,params=final_json, partition_key=df['JobID'].iloc[0])
            except:
                pass
    
    database = _client.CreateDatabase(client=connection, database_name="Logging", tpu=400)
    _client._CreateContainer(container_name="Logger", paths = "/JobID", database=database)
    database = connection.get_database_client("Logging")
    container = database.get_container_client("Logger")
    created_sproc = container.scripts.get_stored_procedure('BulkUpload')
    ID = str(uuid.uuid4())
    record = [
        [
            {
                "JOB_EXECUTION_ID": 6,
                "JOB_RUN_ID": "manual__" +  str(datetime.now()),
                "JOB_NAME": "neu_onetime_data_ingestion_oracle_to_cosmos_schema",
                "TASK_ID": "oracle_to_cosmos_migration_" + container_name,
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

    


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['jai.janyani@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}


extract_oracle_task = {}
load_cosmos_task = {}
upload_data_task = {}
TABLE_NAME = ['SALES' ,  'CUSTOMER' , 'PRODUCT' , 'STORE']
schedule = None
tables = ['SALES' ,  'CUSTOMER' , 'PRODUCT' , 'STORE']
source_chunksize = 500
destination_throughput = 4000

con = sqlite3.connect('/home/azureuser/airflow/Flask/flask_data.db')
df = pd.read_sql_query("SELECT * from jobs", con)
df_dag = pd.read_sql_query("SELECT * from dag_job", con)
job_name = df_dag['job_name'].iloc[0]
df = df.loc[df['source'].str.contains('oracle')]
df = df.loc[df['job_name'].str.contains(job_name)]

if df.shape[0] > 0:
    if df.schedule.iloc[0] and df.schedule.iloc[0] != '':
        schedule = df.schedule.iloc[0]
    if df.source_tables.iloc[0] and df.source_tables.iloc[0] != '':
        tables = df.source_tables.iloc[0]
    if df.source_chunksize.iloc[0] and df.source_chunksize.iloc[0] != '':
        source_chunksize = int(df.source_chunksize.iloc[0])
    if df.destination_throughput.iloc[0] and df.destination_throughput.iloc[0] != '':
        destination_throughput = int(df.destination_throughput.iloc[0])

try:
    TABLE_NAME = tables.split(",")
except:
    TABLE_NAME = ['SALES' ,  'CUSTOMER' , 'PRODUCT' , 'STORE']
TABLE_NAME = list(set(TABLE_NAME))
if destination_throughput < 4000:
    destination_throughput = 4000

dag = DAG(
    'neu_onetime_data_ingestion_oracle_to_cosmos_schema_job',
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

config_task = PythonOperator(
    task_id='get_configuration',
    provide_context=True,
    python_callable=get_configuration,
    op_kwargs={"key": "{{ dag_run.conf['key'] }}"},
    dag=dag,
)


for each in TABLE_NAME:
    extract_oracle_task[each] = PythonOperator(
        task_id=('extract_data_oracle_' + each).lower(),
        provide_context=False,
        python_callable=extract_oracle,
        op_kwargs={'table_name': each, 'chunks': source_chunksize},
        dag=dag)
    upload_data_task[each] = PythonOperator(
        task_id=('upload_data_blob_' + each).lower(),
        provide_context=False,
        python_callable=upload_data,
        op_kwargs={'table_name': each},
        dag=dag)
    load_cosmos_task[each] = PythonOperator(
        task_id=('insert_data_cosmos_' + each).lower(),
        provide_context=False,
        python_callable=load_cosmosdb,
        op_kwargs={'database_name': 'MigratedOracleDB', 'container_name' : each, 'tpu' : destination_throughput},
        dag=dag)


config_task >> start_task
for i in TABLE_NAME:
    start_task >> extract_oracle_task[i]
    extract_oracle_task[i] >> upload_data_task[i]
    upload_data_task[i] >> load_cosmos_task[i]
    load_cosmos_task[i] >> end_task