# config.py

#%% PRG
# List of Table
pathPRGList = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\G_ConvertDBtoParquet\py_coverttoparq\main_PRGTableList.csv"
#pathPRGList = '/app/py_coverttoparq/main_PRGTableList.csv'

# PRG Database connection string
DB_CONNECTION_STRING = '''Driver={SQL Server};
Server=192.168.102.1;
Database=PRGDB20180101; 
UID=mbk_navstaging;
PWD=qe2#HsGq*'''

# SQL query (You can define multiple queries as needed)
# Using {} as a placeholder for table name
SQL_QUERY_All = '''SELECT * FROM {}''' 
SQL_QUERY_YEAR = '''SELECT * FROM {} 
where  year([Create Date CRM]) >= {}''' 
SQL_QUERY_FILE = r'D:\Users\anyamanee\Anyamanee_Work\98_Git\C_DockerAirflow\SaleType_convert.sql'
#List
source_table_prefixes = [
    ('PRG', 'PRG - Go Live'),
    ('KM', 'PRG-KM'),
    ('LG', 'PRG-LG'),
    ('RR', 'RR-Go Live '),
    ('GB', 'PRG-GB')]

#Mapping
source_db_mapping = {
    'PRG - Go Live': 'PRG',
    'PRG-GB': 'GB',
    'PRG-KM': 'KM',
    'PRG-LG': 'LG',
    'RR-Go Live ': 'RR',
}

#%% Common
pathBase = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\C_DockerAirflow\database_PRGDB20180101"

fileType_list = ["csv", "parquet"] # 0:csv, 1:parquet
fileType_csv = fileType_list[0]
fileType_paq = fileType_list[1]