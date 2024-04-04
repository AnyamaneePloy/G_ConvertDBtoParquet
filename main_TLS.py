import pyodbc
import pandas as pd
from datetime import datetime
import time
import os
from zDBConnection import DatabaseConnector
from config import (pathPRGList, pathBase, fileType_csv, fileType_paq,
                     source_db_mapping, SQL_QUERY_FILE,  src_tbl_prefixes)
import warnings
warnings.filterwarnings("ignore")

#%%
# Connect Database
DBconn = DatabaseConnector()

df_tblList_tmp = pd.read_csv(pathPRGList)
lst_tbl = list(df_tblList_tmp['TABLE_NAME'].unique())
df_tblList_all =DBconn.format_table_names(df_tblList_tmp)

# Base path where the folders are supposed to be
folder_date = time.strftime("%Y%m%d")
# Start measuring total runtime
start_total_time = time.time()

id_tbllst = 1
df_logs = pd.DataFrame()
for x_tblfilt in lst_tbl:
    print(f"\nList >>> {id_tbllst}/{len(lst_tbl)}: {x_tblfilt}")
    # Measure time for processing each table
    start_table_time = time.time()
    #init 
    id = 1
    folder_name = x_tblfilt.lower()
    df_logs_tbl = pd.DataFrame()
    df_tblList = df_tblList_all[df_tblList_all['TABLE_NAME'] == x_tblfilt]
    id_tbllst+=1
    df_DB = DBconn.get_data_from_db(x_tblfilt)

    df_DB,df_colname = DBconn.format_col_names(df_DB)
    filename =x_tblfilt.replace("$","_")

    # Append a new log entry
    new_log = pd.DataFrame({
        'ID':[id],
        'tableName':[x_tblfilt],
        'fileName': [filename],
        'ColumnNumber': [df_DB.shape[1]],
        'RowNumber': [df_DB.shape[0]],
    })
    
    df_logs = pd.concat([df_logs, new_log], ignore_index=True)
    id+=1
        
    # Save table
    full_path_all = os.path.join(pathBase, folder_name, folder_date)
    DBconn.check_path(full_path_all)
    DBconn.save_data(df_DB, full_path_all, x_tblfilt, fileType_paq)
    DBconn.save_data(df_colname, pathBase, x_tblfilt+'_colName', fileType_csv)

    # Calculate and print time taken for each table
    end_table_time = time.time()
    print(f"Time taken for {x_tblfilt}: {end_table_time - start_table_time:.2f} seconds")

log_file_path = os.path.join(pathBase,f"{folder_date}_log_file.csv")
df_logs.to_csv(log_file_path, index=False)

print("\n\033[32mCompleted Process\033[0m")
# Calculate total runtime
end_total_time = time.time()
print(f"Total Runtime: {end_total_time - start_total_time:.2f} seconds")