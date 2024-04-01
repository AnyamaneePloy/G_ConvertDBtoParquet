import pyodbc
import pandas as pd
from datetime import datetime
import time
import os
from zDBConnection import DatabaseConnector
from config import (pathPRGList, pathBase, fileType_csv, fileType_paq,
                     source_db_mapping, SQL_QUERY_FILE, source_table_prefixes)
import warnings
warnings.filterwarnings("ignore")

#%%
# Connect Database
DBconn = DatabaseConnector()

df_tblList_tmp = pd.read_csv(pathPRGList)
df_tblList_tmp['SourceDB'] = df_tblList_tmp['TABLE_NAME'].str.split('$').str[0]
df_tblList_tmp['TABLE_NAME_filt'] = df_tblList_tmp['TABLE_NAME'].str.split('$').str[1]
lst_prgscr = list(df_tblList_tmp['SourceDB'].unique())
lst_prgtbl = list(df_tblList_tmp['TABLE_NAME_filt'].unique())
df_tblList_all =DBconn.format_table_names(df_tblList_tmp)

# Base path where the folders are supposed to be
folder_date = time.strftime("%Y%m%d")
# full_path = os.path.join(pathBase, folder_date, fileType_paq)
# DBconn.check_path(full_path)
id_tbllst = 1
df_logs = pd.DataFrame()
for x_tblfilt in lst_prgtbl:
    print(f"\nList >>> {id_tbllst}/{len(lst_prgtbl)}: {x_tblfilt}")
    #init 
    id = 1
    folder_name = x_tblfilt.lower()
    df_logs_tbl = pd.DataFrame()
    df_tblList = df_tblList_all[df_tblList_all['TABLE_NAME_filt'] == x_tblfilt]
    id_tbllst+=1
    for x_tbl, x_tblName in zip(df_tblList['FORMATTED_NAME'],df_tblList['TABLE_NAME']):
        print(f"Table >>> {id}/{len(df_tblList)}: {x_tblName}")
        df_PRGDB_Flie = DBconn.execute_queries_from_template(SQL_QUERY_FILE, source_table_prefixes)
        print(f"Error executing template query for {x_tblName}: {e}, falling back to direct query.")
        
        df_PRGDB = DBconn.get_data_from_db(x_tbl)

        df_PRGDB['Source_DB'] = x_tblName.split('$')[0]
        df_PRGDB['Source_DB'] = df_PRGDB['Source_DB'].map(source_db_mapping)
        df_PRGDB,df_colname = DBconn.format_col_names(df_PRGDB)
        filename =x_tblName.replace("$","_")
        # Save in each table
        #DBconn.save_data(df_PRGDB, full_path, filename, fileType)
    
        # Append a new log entry
        new_log = pd.DataFrame({
            'ID':[id],
            'SourceDB' : [source_db_mapping.get(x_tblName.split('$')[0].strip(), x_tblName.split('$')[0].strip())],
            'tableName':[x_tblName],
            'fileName': [filename],
            'ColumnNumber': [df_PRGDB.shape[1]],
            'RowNumber': [df_PRGDB.shape[0]],
        })
        
        df_logs = pd.concat([df_logs, new_log], ignore_index=True)
        id+=1
        df_logs_tbl = pd.concat([df_logs_tbl, df_PRGDB], ignore_index=True)
        
    # Save merge table
    full_path_all = os.path.join(pathBase, folder_name, folder_date)
    DBconn.check_path(full_path_all)
    DBconn.save_data(df_logs_tbl, full_path_all, x_tblfilt, fileType_paq)
    DBconn.save_data(df_colname, pathBase, x_tblfilt+'_colName', fileType_csv)

log_file_path = os.path.join(pathBase,f"{folder_date}_log_file.csv")
df_logs.to_csv(log_file_path, index=False)