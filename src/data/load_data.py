from dotenv import load_dotenv
import yaml
import argparse
import numpy as np 
import pandas as pd 
#import openpyxl
#import xlrd
import psycopg2
import os

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


def load_previous_scoring_results():
    
    with psycopg2.connect(host = host,
                          port = port,
                          database = dbname,
                          user = user,
                          password = password) as conn:
            sql = f"SELECT * from stockpoint.scoring_results_srds_view"  
            
            df = pd.read_sql(sql, conn)
            
    print('Fetched data')
            
    return df

def store_previous_scoring_results(config_path):
    config = read_params(config_path)
        

    raw_data_path = config["raw_data_config"]["previous_limits_csv"]
    
    df = load_previous_scoring_results()
    
    df.to_csv(raw_data_path, index = False)
    
    
def load_scoring_data():    
    with psycopg2.connect(host = host,
                          port = port,
                          database = dbname,
                          user = user,
                          password = password) as conn:
            sql = f"SELECT * from stockpoint.processed_transactions"  
            
            df = pd.read_sql(sql, conn)
            
    print('Fetched data')
            
    return df

def store_scoring_data(config_path):
    config = read_params(config_path)
        
    raw_data_path = config["raw_data_config"]["scoring_data_csv"]
    
    df = load_scoring_data()
    
    df.to_csv(raw_data_path, index = False)
    
    
def store_data(config_path):
    
    store_previous_scoring_results(config_path)

    store_scoring_data(config_path)
    
    
if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    store_data(config_path = parsed_args.config)