from dotenv import load_dotenv
import yaml
import os
import argparse


import numpy as np 
import pandas as pd
from datetime import datetime

import psycopg2
from sqlalchemy import create_engine
import psycopg2.extras as extras


import warnings
warnings.filterwarnings("ignore")

load_dotenv()


host = os.environ.get('host')
port = os.environ.get('port')
dbname = os.environ.get('dbname')
user = os.environ.get('user')
password = os.environ.get('password')


def read_params(config_path):
    """
    read parameters from the params.yaml file
    input: params.yaml location
    output: parameters as dictionary
    """
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config



def load_and_clean_raw_data(file_path):
    df = pd.read_excel(file_path)
    
    
    df = df.iloc[3:].reset_index(drop = True) ## if the raw file has 4 header rows update this to 3:

# add value to first row, first column and first row and second column
    df.iloc[0,0] = 'vendors'
    df.iloc[0,1] = 'proxy_customer_id'

###################################### This section is only necessary if there is a TOTAL row provided in the raw data ######################################

# # identify index of row TOTAL
# condition_index = df[df['S.R.D.S Ltd'] == 'TOTAL'].index[0]

# # slice DataFrame to keep only rows before condition index
# df = df.iloc[:condition_index, :]

###################################### END ######################################


# remove rows where there is a null value in the first column
    df = df.dropna(axis = 0, subset = ['S.R.D.S Ltd'])

# setting the first row as the header of the dataframe
    df.columns =  df.iloc[0]

# dropping the first row and re-indexing the data, and dropping the Total column
    df.drop(0, axis = 'rows', inplace = True)
    
    df.drop('Total', axis = 'columns', inplace = True)
    
    df.reset_index(drop = True, inplace = True)
        
    return df
    

def load_company_dimension():
    with psycopg2.connect(host = host,
                          port = port,
                          database = dbname,
                          user = user,
                          password = password) as conn:
            sql = f"SELECT proxy_customer_id from stockpoint.company_dimension"
            
            df = pd.read_sql(sql, conn)
            
            df['proxy_customer_id'] = df['proxy_customer_id'].astype(int)

    print('Fetched company dimension data')
    
    return df

def melt_standard_dfs(df):
    
    """Melt dataframe."""
    
    df_melted = pd.melt(df, id_vars = ['proxy_customer_id','vendors'], var_name  = 'invoice_date', value_name = 'invoice_value')
    
    df_melted['vendors'] = df_melted['vendors'].str.strip()

    df_melted['vendors'] = [x.replace('/ ', '') for x in df_melted['vendors']]
    
    df_melted['proxy_customer_id'] = df_melted['proxy_customer_id'].astype('int64')
    
    df_melted.drop_duplicates(keep = 'last', inplace = True)
    
    df_melted['invoice_date'] = [datetime.strptime(x, '%d %b, %Y') for x in df_melted['invoice_date']]
    
    df_melted.fillna(0, inplace = True)
    
    return df_melted

def write_trx_data_to_db(conn, df, table):
    """
    function to write pandas scoring results df to db table
    input : connection parameters, df to insert, table name
    output : df inserted into table name
    
    """
  
    tuples = [tuple(x) for x in df.to_numpy()]
  
    cols = ','.join(list(df.columns))
    # SQL query to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        print('error here')
        conn.rollback()
        cursor.close()
        return 1
    print("the dataframe is inserted")
    cursor.close()
  
  
conn = psycopg2.connect(
            host = host,
             port = port,
             database = dbname,
             user = user,
             password = password
        )


def process_and_insert_raw_data(config_path):
    config = read_params(config_path)
    
    
    path = config['external_data_config']['external_data_path']
    
    ## ensure to update path in params.yaml file to your path and the new file name.
    df = load_and_clean_raw_data(path + 'SRDS SALES BY CUSTOMER_AUGUST_2023.xlsx')
        
    customer_info = load_company_dimension()
            
    final_df = pd.merge(customer_info, df, on = 'proxy_customer_id')
            
    df_melted = melt_standard_dfs(final_df)
    
    df_melted['proxy_customer_id'] = df_melted['proxy_customer_id'].astype(str)    
    
    write_trx_data_to_db(conn, df_melted, 'stockpoint.processed_transactions')
    
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    process_and_insert_raw_data(config_path = parsed_args.config)