import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants

import os
import azure.cosmos.documents as documents
from azure.cosmos import CosmosClient, PartitionKey, exceptions



class NeuDataCosmosDBOperator(object):
    """Represents a document client.
    
    Provides base functions for Cosmos DB
    """

    def __init__(
            self,
            uri,
            key,
            *args, **kwargs
    ):
        """
        :param str uri 
            URI connection string to cosmos database.
        :param str key 
            Key for the database.
        """
        super().__init__(*args, **kwargs)
        self.uri = uri or os.environ.get('uri')
        self.key = key or os.environ.get('key')



    def GetCosmosConnection(self):
        """Gets connection clientect for cosmos db database
        :param str uri:
            Database uri link or ID based link.
        :param str key:
            Database primary key.
        :return:
            A Connection clientect
        :rtype: clientect
        """
        if not self.uri:
            raise ValueError("database is None or empty.")
        if not self.key:
            raise ValueError("database is None or empty.")
        return cosmos_client.CosmosClient(self.uri, {'masterKey': self.key})

    
    def CreateDatabase(self, client, database_name, tpu):
        """Creates database in cosmos db
        :param str uri:
            Database uri link or ID based link.
        :param str key:
            Database primary key.
        :return:
            A Connection clientect
        :rtype: clientect
        """
        database = client.create_database_if_not_exists(database_name, offer_throughput=tpu)
        return database



    def _CreateContainer(self, container_name, paths, database):
        """Creates database in cosmos db
        :param str uri:
            Database uri link or ID based link.
        :param str key:
            Database primary key.
        :return:
            A Connection clientect
        :rtype: clientect
        """
        container_definition = {
        'id': container_name,
        'partitionKey': {
            'paths': [paths],
            'kind': documents.PartitionKind.Hash
            }
        }
        try:
            container = database.create_container(id=container_name, partition_key=PartitionKey(path=paths), offer_throughput=None)
            with open('/home/azureuser/airflow/plugins/lib/sp/BulkUpload.js') as file:
                file_contents = file.read()
            sproc = {
                'id': 'BulkUpload',
                'serverScript': file_contents,
            }
            created_sproc = container.scripts.create_stored_procedure(body=sproc) 
        except:
            pass
        # except errors.HTTPFailure as e:
        #     if e.status_code == http_constants.StatusCodes.CONFLICT:
        #         container, created_sproc = 0, 0
        #     else:
        #         raise e

        return True


    def UpsertIntoCosmos(self, database_id, container_id, client, df):
        """Creates database in cosmos db
        :param str uri:
            Database uri link or ID based link.
        :param str key:
            Database primary key.
        :return:
            A Connection clientect
        :rtype: clientect
        """
        collection_link = "dbs/" + database_id + "/colls/" + container_id,
        for i in range(0,df.shape[0]):
            data_dict = dict(df.iloc[i,:])
            data_dict = json.dumps(data_dict)
            insert_data = client.UpsertItem(collection_link,json.loads(data_dict)
            )
        print('Records inserted successfully.')

    
    def SchemaValidator(self, database_id, container_id, client):
        """Creates database in cosmos db
        :param str uri:
            Database uri link or ID based link.
        :param str key:
            Database primary key.
        :return:
            A Connection clientect
        :rtype: clientect
        """
        try:
            container = client.ReadContainer("dbs/" + database_id + "/colls/" + container_id)
            return True
        except Exception as e:
            return False

    # def DataframeToJson(self, data):
        

    def CallStoredProcedure(self, sp_name, data, partition_key, database_name, container_name, connection):
        """Calls the stored procedure present in cosmos db
            uri:
            Database uri link or ID based link.
        :param str sp_name:
            Stored Procedure Name
        :param str data:
            Data object JSON
        :param str partition_key:
            Partition key for collection
        :param str database_name:
            Cosmos database name
        :param str container_name:
            Cosmos container name
        :param str connection
            Connection object
        :return:
            Result of load
        :rtype: str
        """
        database = connection.get_database_client(database_name)
        container = database.get_container_client(container_name)
        created_sproc = container.scripts.get_stored_procedure(sp_name)
        result = container.scripts.execute_stored_procedure(sproc=created_sproc,params=[data], partition_key=partition_key)
        return result