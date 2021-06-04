user = "ADMIN"
password = "Abc@123456789"
dsn = "tcps://adb.ap-hyderabad-1.oraclecloud.com:1522/g0a77afa38d07dd_oracledatabase_high.adb.oraclecloud.com?wallet_location=/home/azureuser/airflow/plugins/lib/wallet&retry_count=20&retry_delay=3"
database = "postgresdatabase"


uri = "https://cosmos-database-account.documents.azure.com:443/"
key = "sN85G6X0HOc5t6MFTlN5sgLVM5Zj1It1iSOCbIJkizKFVBkSLkevKEpKRmqE74XTK6ITXJdpy8NtkIhCzelYRw=="
lib_path = "/usr/local/airflow/plugins/lib/bin/instantclient_21_1"
temp_file_path = "/usr/local/airflow/temp/postgres_parquet/"
output_db = "MigratedPostgresDB"


connection_string = "DefaultEndpointsProtocol=https;AccountName=oraclemigrations;AccountKey=TvkOH/z4oexK+yRo8F1EMDzAbgHVkmKDC9t+1JzeWvpv6v+xWiF72r7rn02nrENzBfHxpXro5rCcOLdJQQ0h1A==;EndpointSuffix=core.windows.net"
container_name = "migrations"


flatfile_path = "Flatfiles"
temp_file_path_download = "/usr/local/airflow/temp/postgres_parquet_download/"


sqlite_string = 'sqlite:///flask_data.db'
initialize_oracle_sting = 'export LD_LIBRARY_PATH=/usr/local/airflow/plugins/lib/bin/instantclient_21_1'