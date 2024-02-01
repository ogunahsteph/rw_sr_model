import pandas as pd
import yaml
from dotenv import load_dotenv
import argparse

import os
import psycopg2
from sqlalchemy import create_engine
import psycopg2.extras as extras

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


def load_company_dimension():
    with psycopg2.connect(host = host,
                          port = port,
                          database = dbname,
                          user = user,
                          password = password) as conn:
            sql = f"SELECT vendors, proxy_customer_id from stockpoint.company_dimension"
            
            df = pd.read_sql(sql, conn)
            
            df['proxy_customer_id'] = df['proxy_customer_id'].astype(int)
                
    return df

def read_summaries_data_to_post(config_path):
    config = read_params(config_path)
    
    ## read in df with summaries
    final_df_with_summaries_and_limits_path = config["processed_data_config"]["processed_summaries_with_limits_path"]
    
    final_df_with_summaries_and_limits = pd.read_csv(final_df_with_summaries_and_limits_path)
    

    customer_info = load_company_dimension()
    
    final_df_with_summaries_and_limits = pd.merge(customer_info, final_df_with_summaries_and_limits, on = 'proxy_customer_id')
    
    final_df_with_summaries_and_limits.rename({'proxy_customer_id' : 'customer_id'}, axis = 1, inplace = True)
    
    final_df_with_summaries_and_limits['customer_id'] = final_df_with_summaries_and_limits['customer_id'].astype(str)
    
    vendors = final_df_with_summaries_and_limits['vendors']
    
    final_df_with_summaries_and_limits.drop('vendors', axis = 'columns', inplace = True)
    
    final_df_with_summaries_and_limits.insert(4, 'vendors', vendors)

    final_df_with_summaries_and_limits.dropna(axis = 'rows', inplace = True)
        
    return final_df_with_summaries_and_limits


#function to write pandas scoring results df to db table 
def write_trx_data_to_db(conn, df, table):
  
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


def process_and_post_data(config_path):
    
    summaries_df = read_summaries_data_to_post(config_path)
    

    write_trx_data_to_db(conn, summaries_df, "stockpoint.scoring_summaries_srds")
    
if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    process_and_post_data(config_path = parsed_args.config)