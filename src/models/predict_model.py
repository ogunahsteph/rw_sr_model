import os
import argparse
import pandas as pd
import yaml
import pandas as pd
import numpy as np
import math
import mlflow
#import urlparse
from dotenv import load_dotenv
load_dotenv()


import warnings
warnings.filterwarnings('ignore')

def read_params(config_path):
    """
    read parameters from the params.yaml file
    input: params.yaml location
    output: parameters as dictionary
    """
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config

def round_off(n):
    """
    This function rounds off elements by setting a ceiling to the next 100
    """
    return int(math.ceil(n / 100.0)) * 100


def limit_caps(n):
    """
    This function sets elements caps in line with product terms i.e 300K RWF - 3M RWF:
    * merchants qualifying for less than 300K RWF get a 0 loan limit
    * merchants qualifying for more than 3M RWF get the limits back-tracked to 3M RWF
    """
    if n < 50000:
        return 0
    elif n > 5000000:
        return 5000000
    else:
        return n
    
    
def add_scoring_refresh_date(df):
    """
    function to add date when scoring refresh was done
    
    Inputs:
    Model refresh date
    
    Outputs:
    new column with scoring refresh date 

    """
    
    scoring_refresh_date = (pd.Timestamp.today()).strftime("%Y-%m-%d")
    scoring_referesh_date = pd.Timestamp(scoring_refresh_date)
    
    return scoring_refresh_date

def add_model_version(df):
    """
    function to add date when scoring refresh was done
    
    Inputs:
    Model refresh date
    
    Outputs:
    new column with scoring refresh date 

    """
    
    ## 1st change - 2022-001[2022-05-06, 2023-02-16] - Limit cap was reviewed from 17% to 35% to align with other similar tenure products.
    
    model_version = f"2023-001[2023-02-16, {pd.to_datetime('today').date()}]"
    
    return model_version

def allocate_limits(df, config):
    df['gross_14_day_limit'] = df["average_monthly_value_last_6_months"] * df["trading_consistency_last_6_months"] * config["business_rules"]["limit_factor_14_day_product"]
    
    df["adjusted_14_day_limit"] = df["gross_14_day_limit"].apply(round_off)
    
    df["final_14_day_limit_rwf"] = df["adjusted_14_day_limit"].apply(limit_caps)
    
    df["final_14_day_limit_usd"] = df["final_14_day_limit_rwf"] / config["business_rules"]["rwandese_franc_base_rate"]
    
    df["scoring_refresh_date"] = add_scoring_refresh_date(df)
    
    df["model_version"] = add_model_version(df)

    
    return df

def limit_stabilization(df):
    
    current_limit = df['current_limit']
    previous_limit = df['previous_limit']
    
    if current_limit == 0:
        return current_limit
    
    elif previous_limit == 0 or pd.isnull(previous_limit):
        return current_limit

    elif current_limit >= (previous_limit * 1.5):
        return previous_limit * 1.5
    
    elif current_limit >= (previous_limit * 0.85) or current_limit <= (previous_limit * 1.15):
        return previous_limit

    else:
        return current_limit
    
def zeroize_non_qualified_limits(df):
    final_14_day_limit_rwf = df['final_14_day_limit_rwf']
    is_qualified = df['is_qualified']
    
    if is_qualified == False:
        return 0
    else: 
        return final_14_day_limit_rwf
    
def zeroize_usd_non_qualified_limits(df):
    final_14_day_limit_usd = df['final_14_day_limit_usd']
    is_qualified = df['is_qualified']
    
    if is_qualified == False:
        return 0
    else: 
        return final_14_day_limit_usd
    
def limit_stabilization_application(current_limits_df, config):
    
    ## read in previous results and clean df
    
    raw_data_path = config["raw_data_config"]["previous_limits_csv"]
   
    previous_scoring_results_df = pd.read_csv(raw_data_path)
    
    previous_results = previous_scoring_results_df[['proxy_customer_id','final_14_day_limit_rwf']]

    previous_results.rename(columns = {'final_14_day_limit_rwf' : 'previous_limit'}, inplace = True)
    
    previous_results['proxy_customer_id'] = previous_results['proxy_customer_id'].astype(str)
          
     ## clean current limits df 
    current_results = current_limits_df[['proxy_customer_id','final_14_day_limit_rwf']]

    current_results.rename(columns = {'final_14_day_limit_rwf' : 'current_limit'}, inplace = True)
    
    current_results['proxy_customer_id'] = current_results['proxy_customer_id'].astype(str)
    
    current_limits_df['proxy_customer_id'] = current_limits_df['proxy_customer_id'].astype(str)
    
    
    
    ## merge the dataframes
    
    stabilized_limits_df = pd.merge(previous_results, current_results, on = 'proxy_customer_id', how = 'inner')
    
    stabilized_limits_df['new_final_limit'] = stabilized_limits_df.apply(lambda x: limit_stabilization(x), axis = 1)
    
    stabilized_limits_df = stabilized_limits_df[['proxy_customer_id','previous_limit','new_final_limit']]
    
    current_limits_df = pd.merge(current_limits_df, stabilized_limits_df, on = 'proxy_customer_id', how = 'inner')
    
    return current_limits_df

def rules_summary_narration(df):
    age_on_network = df['age_on_network_months']
    trading_consistency = df['trading_consistency_last_6_months']
    months_since_last_transaction = df['diff_last_txn_months']
    allocated_14_day_limit = df['final_14_day_limit_rwf']
    new_final_14_day_limit = df['new_final_limit']
    previous_limit = df['previous_limit']
    is_qualified = df['is_qualified']
    total_value_last_6_months = df['total_value_last_6_months']
    
    if allocated_14_day_limit == 0 and total_value_last_6_months == 0 and is_qualified == False:
        return 'No trading activity: B003'
    elif new_final_14_day_limit == 0 and allocated_14_day_limit < new_final_14_day_limit and is_qualified == False:
        return 'Inconsistent trading activity: B002'
    elif new_final_14_day_limit > 0 and is_qualified == False:
        return 'Based on historical and current information on your business and loan history, you do not currently meet Asante lending criteria: C004'
    elif new_final_14_day_limit > 0 and new_final_14_day_limit == previous_limit and is_qualified == True:
        return 'All rules passed(limit maintained): F001'
    elif new_final_14_day_limit > 0 and previous_limit == 0 and is_qualified == True:
        return 'All rules passed (New above 0 limit): F001'
    elif new_final_14_day_limit > 0 and new_final_14_day_limit < allocated_14_day_limit and is_qualified == True:
        return 'All rules passed but lower than expected trading activity(limit decreased): B001'
    elif new_final_14_day_limit > 0 and new_final_14_day_limit > allocated_14_day_limit and is_qualified == True:
        return 'All rules passed(limit increased): F001'
    else:
        return 'Limits assigned less than product thresholds: D001'
    
    
def load_summaries_and_allocate_limits(config_path):
    config = read_params(config_path)
        
    scoring_summaries_data_path = config["processed_data_config"]["processed_scoring_summaries_data_path"]
    
    scoring_summaries_with_limits_path = config["processed_data_config"]["processed_summaries_with_limits_path"]
        
    
    scoring_summaries_df = pd.read_csv(scoring_summaries_data_path)
    
    data_with_limits = allocate_limits(scoring_summaries_df, config)
    
    final_df_with_limits = limit_stabilization_application(data_with_limits, config)

    
    ############# SUMMARIES FOR LOGGING ON MLFLOW ##############################
        
    average_age_on_network = scoring_summaries_df['age_on_network_months'].mean()

    average_transacted_months_last_6_months = scoring_summaries_df['unique_sales_months'].mean()

    average_monthly_revenue_last_6_months = scoring_summaries_df['average_monthly_value_last_6_months'].mean()
    
    average_trading_consistency_last_6_months = scoring_summaries_df['trading_consistency_last_6_months'].mean()
    
    average_recency_in_months = scoring_summaries_df['diff_last_txn_months'].mean()
    
    average_limit_allocated = final_df_with_limits['new_final_limit'].mean()
    
    #########################################################################

    final_df_with_limits["is_qualified"] = False
    
    print(final_df_with_limits[final_df_with_limits['proxy_customer_id'] == '101486430'][['proxy_customer_id', 'diff_last_txn_months', 'trading_consistency_last_6_months', 'age_on_network_months', 'new_final_limit', 'is_qualified']])
    
    final_df_with_limits.loc[
    (final_df_with_limits["diff_last_txn_months"] <= config["business_rules"]["maximum_allowed_recency_in_months"]) &
    (final_df_with_limits["trading_consistency_last_6_months"] >= config["business_rules"]["minimum_trading_consistency_threshold"]) &
    (final_df_with_limits["age_on_network_months"] >= config["business_rules"]["minimum_age_on_network"]) &
    (final_df_with_limits["new_final_limit"] > 0), "is_qualified"] = True ##[["proxy_customer_id","vendors","final_14_day_limit_usd","final_14_day_limit_rwf","scoring_refresh_date"]]
         
        
    final_df_with_limits['rules_summary_narration'] = final_df_with_limits.apply(lambda x: rules_summary_narration(x), axis = 1)
    final_df_with_limits[['rules_summary_narration','limit_reason']] = final_df_with_limits["rules_summary_narration"].astype("str").str.split(":", expand = True)    
    
    print(final_df_with_limits[final_df_with_limits['proxy_customer_id'] == '101486430'][['proxy_customer_id', 'new_final_limit', 'is_qualified']])
    
    final_df_with_limits['final_14_day_limit_rwf'] = final_df_with_limits.apply(lambda x : zeroize_non_qualified_limits(x), axis = 1)

    final_df_with_limits['final_14_day_limit_usd'] = final_df_with_limits.apply(lambda x : zeroize_usd_non_qualified_limits(x), axis = 1)
    
    qualified_vendors_df =  final_df_with_limits[final_df_with_limits["is_qualified"] == True]
    
    
#     ################### MLFLOW ###############################
    
    mlflow_config = config["mlflow_config"]
    remote_server_uri = mlflow_config["remote_server_uri"]

    mlflow.set_tracking_uri(remote_server_uri)
    mlflow.set_experiment(mlflow_config["experiment_name"])

    experiment = mlflow.get_experiment_by_name(mlflow_config["experiment_name"])
    print("Experiment_id: {}".format(experiment.experiment_id))
    print("Artifact Location: {}".format(experiment.artifact_location))
    print("Tags: {}".format(experiment.tags))
    print("Lifecycle_stage: {}".format(experiment.lifecycle_stage))
    
    # Get the current model registry uri
    mr_uri = mlflow.get_registry_uri()
    print("Current model registry uri: {}".format(mr_uri))
    # Get the current tracking uri
    tracking_uri = mlflow.get_tracking_uri()
    print("Current tracking uri: {}".format(tracking_uri))

    # They should be the same
    assert mr_uri == tracking_uri

    with mlflow.start_run(run_name = mlflow_config["run_name"]) as mlops_run:
        mlflow.log_metric('count_of_all_scored_vendors', str(final_df_with_limits.shape[0]))
        mlflow.log_metric('count_of_vendors_with_limits', str(qualified_vendors_df.shape[0]))
        mlflow.log_metric('average_age_on_network', str(average_age_on_network))
        mlflow.log_metric('average_transacted_months_last_6_months', str(average_transacted_months_last_6_months))
        mlflow.log_metric('average_monthly_revenue_last_6_months', str(average_monthly_revenue_last_6_months))
        mlflow.log_metric('average_trading_consistency_last_6_months', str(average_trading_consistency_last_6_months))
        mlflow.log_metric('average_recency_in_months', str(average_recency_in_months))
        mlflow.log_metric('average_limit_allocated', str(average_limit_allocated))
        
        
        mlflow.log_param('limit_factor_14_day_product', config["business_rules"]['limit_factor_14_day_product'])
        mlflow.log_param('rwandese_franc_base_rate' , config["business_rules"]['rwandese_franc_base_rate'])
        mlflow.log_param('minimum_trading_consistency_threshold' , config["business_rules"]['minimum_trading_consistency_threshold'])
        mlflow.log_param('minimum_age_on_network' , config["business_rules"]['minimum_age_on_network'])
        mlflow.log_param('maximum_allowed_recency_in_months' , config["business_rules"]['maximum_allowed_recency_in_months'])
        
        #mlflow.log_artifact(config)
               
       
        #tracking_url_type_store = urlparse(mlflow.get_artifact_uri()).scheme

    final_df_with_limits.to_csv(scoring_summaries_with_limits_path, index = False)
        
    
if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    load_summaries_and_allocate_limits(config_path = parsed_args.config)