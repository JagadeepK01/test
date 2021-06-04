import os
import sys
# import config
import cx_Oracle
import numpy as np
import pandas as pd


class NeuDataOpsOracleCloudOperator(object):
    """Represents a document client.
    
    Provides base functions for Oracle Cloud Database
    """

    def __init__(
            self,
            dsn_string,
            username,
            password,
            *args, **kwargs
    ):
        """
        :param str dsn_string 
            TSN connection string to oracle database.
        :param str username 
            Username for the database.
        :param str password 
            Password for the database.
        """
        super().__init__(*args, **kwargs)
        self.dsn_string = dsn_string or os.environ.get('dsn_string')
        self.username = username or os.environ.get('username')
        self.password = password or os.environ.get('password')



    def GetOracleConnection(self, database):
        """Gets connection object for autonomous database
        :param str database:
            Database self link or ID based link.
        :return:
            A Connection object
        :rtype: object
        """
        if not database:
            raise ValueError("database is None or empty.")
        return cx_Oracle.connect(user=self.username, password=self.password, dsn=self.dsn_string)



    def GetQueryOutput(self, query, cursor):
        """Gets query output in tuples
        :param str query:
            SQL query string
        :param str cursor
            Cursor object
        :return:
            A SQL result in tuple
        :rtype: object
        """
        if not query:
            raise ValueError("query is None or empty.")
        results = cursor.execute(query)
        for i in results:
            print(i)
        return True



    def GetQueryOutputDataframe(self, query, connection):
        """Gets query output in tuples
        :param str query:
            SQL query string
        :param str cursor
            Cursor object
        :return:
            A SQL result in dataframe
        :rtype: object
        """
        if not query:
            raise ValueError("query is None or empty.")
        df = pd.read_sql(query, connection)
        return df

    def GetQueryOutputDataframeChunks(self, table_name, connection, chunks=500):
        """Gets query output in tuples
        :param str query:
            SQL query string
        :param str cursor
            Cursor object
        :return:
            A SQL result in dataframe
        :rtype: object
        """
        if not table_name:
            raise ValueError("query is None or empty.")
        query = "select * from " + str(table_name)
        df = pd.read_sql(query, connection, chunksize=chunks)
        return df


    def LoadTableDataframe(self, data, table, connection):
        """Inserts a dataframe into a table
        :param str data:
            Dataframe object
        :param str cursor
            connection object
        :return:
            True if success
        :rtype: object
        """
        # if not data:
        #     raise ValueError("data is None or empty.")
        data.to_sql(table, connection)
        return True
    
    def InitializeOracleLibraries(self, libpath):
        try:
            if sys.platform.startswith("darwin"):
                lib_dir = os.path.join(os.environ.get("HOME"), "Downloads",
                                    "instantclient_19_8")
                cx_Oracle.init_oracle_client(lib_dir=lib_dir)
            elif sys.platform.startswith("win32"):
                print('THIS IS LIB PATH: ', libpath)
                cx_Oracle.init_oracle_client(lib_dir=libpath)
            elif sys.platform.startswith("linux"):
                print('THIS IS LIB PATH: ', libpath)
                cx_Oracle.init_oracle_client(lib_dir=libpath)
        except Exception as err:
            print("Whoops!")
            print(err)