# config.py

#%% PRG
# List of Table
pathPRGList     = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\G_ConvertDBtoParquet\20240320_PRGTableList_location.csv"
pathPRGListExt  = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\G_ConvertDBtoParquet\ETL_PRG_PYCode\PRG_Department_Master.csv"

#pathPRGList = '/app/py_coverttoparq/main_PRGTableList.csv'

# PRG Database connection string
DB_CONNECTION_STRING = '''Driver={SQL Server};
Server=192.168.102.1;
Database=PRGDB20180101; 
UID=mbk_navstaging;
PWD=qe2#HsGq*'''

# TLS Database connection string
# DB_CONNECTION_STRING = '''Driver={SQL Server};
# Server=192.168.56.32;
# Database=tls_edoc; 
# UID=admin_tlsedoc;
# PWD=edoc@29Jan2024'''

# SQL query (You can define multiple queries as needed)
# Using {} as a placeholder for table name
SQL_QUERY_All = '''SELECT * FROM {}'''
SQL_QUERY_YEAR = '''SELECT * FROM {} 
where  year([Create Date CRM]) >= {}''' 
# SQL_QUERY_FILE = r'D:\User_Profile\jutamasp\Documents\Project\2024\01_Datalake\GitHub\ProfitAndLoss\SourcePreparation\SaleType_convert.sql'
SQL_QUERY_FILE = r'D:\Users\anyamanee\Anyamanee_Work\98_Git\G_ConvertDBtoParquet\ETL_PRG_PYCode\SaleInvoiceHeader.sql'

#List
src_tbl_prefixes = [
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
# pathBase = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\C_DockerAirflow\database_PRGDB20180101"
pathBase = r"D:\Users\anyamanee\Anyamanee_Work\98_Git\G_ConvertDBtoParquet\ETL_PRG_PYCode\ProfitAndLoss"

fileType_list = ["csv", "parquet"] # 0:csv, 1:parquet
fileType_csv = fileType_list[0]
fileType_paq = fileType_list[1]



