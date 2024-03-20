import pyodbc
import pandas as pd
from datetime import datetime
import os
from config import DB_CONNECTION_STRING, SQL_QUERY_All

#%%
class DatabaseConnector:
    def __init__(self):
        # Database connection string
        self.conn_str = DB_CONNECTION_STRING
        self.today = datetime.now()

    def get_data_from_db(self, tbl_name):
        # Connect to the database and fetch data, then convert the data to a pandas DataFrame
        try:
            with pyodbc.connect(self.conn_str) as conn:
                # Your SQL query
                query = SQL_QUERY_All.format(tbl_name)  
                self.db_data  = pd.read_sql(query, conn)
                return self.db_data 
        except pyodbc.Error as e:
            print("Database error:", e)
        except Exception as e:
            print("Error:", e)

    # Define a function to format the table names in the DataFrame
    def format_table_names(self, df):
        # Add a new column with formatted names
        df['FORMATTED_NAME'] = '[' + df['TABLE_SCHEMA'] + '].[' + df['TABLE_NAME'] + ']'
        return df
    
    def check_path(self, full_path):
        # Check if the path exists, and create it if it does not
        if not os.path.exists(full_path):
            os.makedirs(full_path)
            print(f"Created directory {full_path}")
        else:
            print(f"Directory {full_path} already exists.")


    def save_data_to_csv(self, db_data, pathfile, filename):
        # Specify the file path
        path = os.path.join(pathfile, filename + ".csv")
        db_data.to_csv(path, index=False, encoding='utf-8-sig')  # Save the DataFrame to a CSV file with utf-8 encoding

    def save_data_to_parquet(self, db_data, pathfile, filename):
        # Specify the file path
        path = os.path.join(pathfile, filename + ".parquet")
        db_data.to_parquet(path, index=False)

    def save_data(self, db_data, pathfile, filename, filetype):
        # Define a mapping of file types to their corresponding save methods
        save_methods = {
            'csv': self.save_data_to_csv,
            'parquet': self.save_data_to_parquet
        }
        save_method = save_methods.get(filetype.lower())
        if save_method:
            save_method(db_data, pathfile, filename)
        else:
            print(f"Unsupported file type: {filetype}")


