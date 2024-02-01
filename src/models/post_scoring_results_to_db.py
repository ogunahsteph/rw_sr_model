import pandas as pd
import yaml
from dotenv import load_dotenv
import argparse

import warnings
warnings.filterwarnings('ignore')

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
            sql = f"SELECT vendors, original_customer_id, proxy_customer_id from stockpoint.company_dimension"
            
            df = pd.read_sql(sql, conn)
                
    return df


# def zeroize_non_qualified_limits(df):
#     final_14_day_limit_rwf = df['final_14_day_limit_rwf']
#     is_qualified = df['is_qualified']
    
#     if is_qualified == False:
#         final_14_day_limit_rwf = 0
#         return final_14_day_limit_rwf
#     else: 
#         return final_14_day_limit_rwf
    
# def zeroize_usd_non_qualified_limits(df):
#     final_14_day_limit_usd = df['final_14_day_limit_usd']
#     is_qualified = df['is_qualified']
    
#     if is_qualified == False:
#         final_14_day_limit_usd = 0
#         return final_14_day_limit_usd
#     else: 
#         return final_14_day_limit_usd

def read_scoring_results_data_to_post(config_path):
    config = read_params(config_path)
    
    ## read in df with summaries
    final_df_with_summaries_and_limits_path = config["processed_data_config"]["processed_summaries_with_limits_path"]
        
    
    final_df_with_summaries_and_limits = pd.read_csv(final_df_with_summaries_and_limits_path)
    
    final_df_with_summaries_and_limits['proxy_customer_id'] = final_df_with_summaries_and_limits['proxy_customer_id'].astype(str)
    
    codes_mapping = load_company_dimension()
    
    
    ##merge other identifiers
    final_df_with_summaries_and_limits = pd.merge(codes_mapping, final_df_with_summaries_and_limits, on = 'proxy_customer_id')
    

    scoring_results_df = final_df_with_summaries_and_limits[["proxy_customer_id","vendors","original_customer_id","new_final_limit","scoring_refresh_date","is_qualified","rules_summary_narration","limit_reason"]]
    
    
    
    
    scoring_results_df.rename(columns = {'new_final_limit' : 'final_14_day_limit_rwf'}, inplace = True)
    
    scoring_results_df['final_14_day_limit_usd'] = scoring_results_df['final_14_day_limit_rwf'] / 1020
    
            
#     scoring_results_df['final_14_day_limit_rwf'] = scoring_results_df.apply(lambda x : zeroize_non_qualified_limits(x), axis = 1)
    
#     scoring_results_df['final_14_day_limit_usd'] = scoring_results_df.apply(lambda x : zeroize_usd_non_qualified_limits(x), axis = 1)

        
    ## create df to be posted
    
    vendors = scoring_results_df['vendors']

    original = scoring_results_df['original_customer_id']

    proxy = scoring_results_df['proxy_customer_id']

    limit_usd = scoring_results_df['final_14_day_limit_usd']

    limit_rwf = scoring_results_df['final_14_day_limit_rwf']

    date = scoring_results_df['scoring_refresh_date']
    
    rules_summary_narration = scoring_results_df['rules_summary_narration']
    
    limit_reason = scoring_results_df['limit_reason']
    
    #model_version = scoring_results_df['model_version']
    
    
    scoring_results = pd.DataFrame({'proxy_customer_id' : proxy,
                                   'vendors' : vendors,
                                   'original_customer_id' : original,
                                   'final_14_day_limit_usd' : limit_usd, 
                                   'final_14_day_limit_rwf' : limit_rwf,
                                   'scoring_refresh_date' : date,
                                   'limit_reason' : limit_reason,
                                   'rules_summary_narration' : rules_summary_narration,
                                   }) #'model_version' : model_version
    
    print(scoring_results.head(2))
    
    return scoring_results


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
    
    summaries_df = read_scoring_results_data_to_post(config_path)
    
    write_trx_data_to_db(conn, summaries_df, "stockpoint.scoring_results_srds")
    
if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    process_and_post_data(config_path = parsed_args.config)