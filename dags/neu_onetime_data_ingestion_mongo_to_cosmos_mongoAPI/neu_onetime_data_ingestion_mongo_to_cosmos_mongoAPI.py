import os
import sys

import airflow
from airflow import DAG, settings
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from operators.neu_dataops_mongo_db_operator import NeuDataMongoDBOperator
from azure.cosmos import CosmosClient, PartitionKey, exceptions
from operators.neu_dataops_cosmos_operator import NeuDataCosmosDBOperator


sys.path.insert(0,os.path.abspath(os.path.dirname(__file__)))
import glob
import json
import bson
from bson import json_util
import logging
import uuid
from datetime import datetime

import config_cosmos_mongo as config


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['jai.janyani@neudesic.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}



dag = DAG(
    'neu_onetime_data_ingestion_mongo_to_cosmos_mongoAPI',
    default_args=default_args,
    schedule_interval=None,
)


def extract_mongo(database_name, container_name,  **kwargs):
    try:
        client = NeuDataMongoDBOperator(host = config.host, port = config.port)
        connection = client.get_conn()
        cursor = client.GetMongoDataJSON(connection, database_name, container_name, config.batch_size)
        num=0
        for chunk in cursor:
            documentList = bson.decode_all(chunk)
            num = num + 1
            filename = "part_" + container_name + str(num) + ".json"
            path = "/home/azureuser/airflow/temp/cosmos_mongo/" + str(filename)
            if len(documentList) != 0:
                with open(path, 'w') as outfile:
                    json.dump(json_util.dumps(documentList), outfile)
    except Exception as e:
        logging.info(e)



def load_cosmosdb(database_name, container_name,  **kwargs):
    _client = NeuDataCosmosDBOperator(config.uri, config.key)
    connection = _client.GetCosmosConnection()
    client = NeuDataMongoDBOperator(uri = config.mongo_uri)
    all_files = glob.glob("/home/azureuser/airflow/temp/cosmos_mongo/*")
    try:
        for files in all_files:
            logging.info(files)
            with open(files, 'r') as jsonfile:
                data = json.load(jsonfile)
            documentList = json_util.loads(data)
            client.insert_many(database_name, container_name, documentList)

        database = _client.CreateDatabase(client=connection, database_name="Logging", tpu=400)
        _client._CreateContainer(container_name="Logger", paths="/JobID", database=database)
        database = connection.get_database_client("Logging")
        container = database.get_container_client("Logger")
        created_sproc = container.scripts.get_stored_procedure('BulkUpload')

        ID = str(uuid.uuid4())
        record = [[{
            "JOB_EXECUTION_ID": 1,
            "JOB_RUN_ID": "manual__" + str(datetime.now()),
            "JOB_NAME": "neu_onetime_data_ingestion_mongo_to_cosmos_mongoAPI",
            "TASK_ID": "mongo_to_cosmos_migration_" + container_name,
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
        }]]
        container.scripts.execute_stored_procedure(sproc=created_sproc,params=record, partition_key=ID)

        logging.info('DONE----------------------------------')
    except Exception as e:
        logging.info("Going in except")
        logging.info(e)
        database = _client.CreateDatabase(client=connection, database_name="Logging", tpu=400)
        _client._CreateContainer(container_name="Logger", paths="/JobID", database=database)
        database = connection.get_database_client("Logging")
        container = database.get_container_client("Logger")
        created_sproc = container.scripts.get_stored_procedure('BulkUpload')

        ID = str(uuid.uuid4())
        record = [[{
            "JOB_EXECUTION_ID": 1,
            "JOB_RUN_ID": "manual__" + str(datetime.now()),
            "JOB_NAME": "neu_onetime_data_ingestion_mongo_to_cosmos_mongoAPI",
            "TASK_ID": "mongo_to_cosmos_migration_" + container_name,
            "START_DATETIME": str(datetime.now()),
            "END_DATETIME": str(datetime.now()),
            "JOB_TYPE": "onetime_ingestion",
            "JOB_STATUS": "failed",
            "SOURCE_RECORD_COUNT": 'null',
            "DESTINATION_RECORD_COUNT": 'null',
            "JOB_MESSAGE": 'null',
            "SOURCE_QUERY": 'null',
            "DESTINATION_QUERY": 'null',
            "JobID": ID
        }]]
        container.scripts.execute_stored_procedure(sproc=created_sproc, params=record, partition_key=ID)



start_task = DummyOperator(task_id='start', dag=dag)
end_task = DummyOperator(task_id='end', dag=dag)

extract_mongo_task = {}
load_cosmos_task = {}

COLLECTIONS = ['sales', 'product', 'booking', 'store']
for each in COLLECTIONS:
    extract_mongo_task[each] = PythonOperator(
        task_id='extract_mongo_' + str(each),
        provide_context=False,
        python_callable=extract_mongo,
        op_kwargs={'database_name': config.database, 'container_name' : each},
        dag=dag)

    load_cosmos_task[each] = PythonOperator(
        task_id='load_cosmos_' + str(each),
        provide_context=False,
        python_callable=load_cosmosdb,
        op_kwargs={'database_name': config.database, 'container_name' : each},
        dag=dag)


# start_task >> extract_mongo_task
# extract_mongo_task >> load_cosmos_task
# load_cosmos_task >> end_task



for i in COLLECTIONS:
    start_task >> extract_mongo_task[i]
    extract_mongo_task[i] >> load_cosmos_task[i]
    load_cosmos_task[i] >> end_task