# config.py

#%% PRG
# List of Table
pathPRGList = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\C_DockerAirflow\20240320_PRGTableList.csv"

# PRG Database connection string
DB_CONNECTION_STRING = '''Driver={SQL Server};
Server=192.168.102.1;
Database=PRGDB20180101; 
UID=mbk_navstaging;
PWD=qe2#HsGq*'''

# SQL query (You can define multiple queries as needed)
SQL_QUERY_All = '''SELECT * FROM {} where  year([Create Date CRM]) >= 2020'''  # Using {} as a placeholder for table name

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
fileType = fileType_list[1]