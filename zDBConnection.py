import pyodbc
import pandas as pd
from datetime import datetime
import os
from config import DB_CONNECTION_STRING, SQL_QUERY_All, SQL_QUERY_YEAR
import pyarrow as pa
import pyarrow.parquet as pq

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
                # Try to execute the first query that includes a specific condition
                try:
                    query = SQL_QUERY_YEAR.format(tbl_name, '2020')
                    return pd.read_sql(query, conn)
                except Exception as e:
                    print("Error! executing query with year condition \n")
                    # If the first query fails, attempt the second, more general query
                    try:
                        query = SQL_QUERY_All.format(tbl_name)
                        return pd.read_sql(query, conn)
                    except pyodbc.Error as e:
                        print("Error! executing fallback query")
                        return None
        except pyodbc.Error as e:
            print("Database error:", e)
        except Exception as e:
            print("Error:", e)
    
    def execute_queries_from_template(self, sql_template_path, source_table_prefixes):
        try:
            # Read the SQL template from the file
            with open(sql_template_path, 'r') as file:
                sql_template = file.read()

            with pyodbc.connect(self.conn_str) as conn:
                results = []  # To store the final results
                for source_db, table_prefix in source_table_prefixes:
                    # Replace placeholders in the template with actual values
                    query = sql_template.format(source_db=source_db, table_prefix=table_prefix)
                    # Dynamically execute the constructed query
                    df_partial = pd.read_sql(query, conn)
                    results.append(df_partial)

                # Concatenate all partial DataFrames to create the final DataFrame
                df_final = pd.concat(results, ignore_index=True)
                return df_final
        except pyodbc.Error as e:
            print(f"Database error: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        return pd.DataFrame()  # Return an empty DataFrame in case of any failure

    # Define a function to format the table names in the DataFrame
    def format_table_names(self, df):
        # Add a new column with formatted names
        df['FORMATTED_NAME'] = '[' + df['TABLE_SCHEMA'] + '].[' + df['TABLE_NAME'] + ']'
        return df
    
    # Define a function to format the table names in the DataFrame
    def format_col_names(self, df):
        trg = '_'
        col_names = [name.replace(' ', trg).replace('-', trg).replace('__', trg).replace('_', trg) for name in df.columns]
        # Print the column names before renaming.
        # print("Before renaming the columns:", df.columns) 
        # Rename the columns in the DataFrame.
        df.columns = col_names
        # Print the column names after renaming.
        # print("After renaming the columns:", df.columns)      
        # Convert the list to a DataFrame
        df_colname = pd.DataFrame(col_names, columns=['Column_Names'])
        df_colname.head()

        return df, df_colname
     
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
        table = pa.Table.from_pandas(db_data)
        pq.write_table(table, path)

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


