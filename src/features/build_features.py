import pandas as pd
import numpy as np
import math
from datetime import timedelta
import datetime as dt
from dateutil.relativedelta import relativedelta

import os
import argparse
import pandas as pd
import yaml

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


def calculate_summaries(df):    
    
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    
    summaries = df.groupby(['proxy_customer_id']).agg(
    earliest_sales_date = pd.NamedAgg(column = 'invoice_date', aggfunc = min),
    most_recent_sales_date = pd.NamedAgg(column = 'invoice_date', aggfunc = max)).reset_index()
    
    summaries['age_on_network_months'] = (((summaries['most_recent_sales_date'] - summaries['earliest_sales_date']) / np.timedelta64(1, 'M')) + 1).astype(int)
    
#     # # Convert the date columns to datetime format if they are not already
#     summaries['most_recent_sales_date'] = pd.to_datetime(summaries['most_recent_sales_date'])
#     summaries['earliest_sales_date'] = pd.to_datetime(summaries['earliest_sales_date'])

#     # Calculate age_on_network_months
#     # Assuming summaries['most_recent_sales_date'] and summaries['earliest_sales_date'] are in datetime format
#     summaries['age_on_network_months'] = summaries.apply(lambda x: relativedelta(x['most_recent_sales_date'], x['earliest_sales_date']).months, axis=1).astype(np.int32)

    current_period = df["invoice_date"].max()
    target_analysis_period = current_period - pd.DateOffset(months = 6)

    scoring_df = df.loc[(df["invoice_date"] >= target_analysis_period) & (df["invoice_value"] > 0)]
    
    
    temp_df = scoring_df.copy()

    monthly_aggregates = temp_df.set_index("invoice_date").groupby([pd.Grouper(freq = "M"), "proxy_customer_id"]).sum().reset_index()
    
    monthly_aggregates = monthly_aggregates.groupby(["proxy_customer_id", pd.Grouper(key = "invoice_date", freq = "1M")]).sum().reset_index()

    num_unique_sales_months = monthly_aggregates.groupby("proxy_customer_id")["invoice_date"].nunique().rename("unique_sales_months").reset_index()
    
    
    
    
    customer_grouper = {"invoice_date": ["max", "min"],
                    "invoice_value": ["sum"]}

    customers_summary = scoring_df.groupby(["proxy_customer_id"]).agg(customer_grouper)

    customers_summary.columns = ['_'.join(col) for col in customers_summary.columns.values]

    customers_summary = pd.DataFrame(customers_summary.to_records())

    customers_summary.rename(
        columns = {
            "invoice_date_max": "most_recent_sales_date_last_6_months",
            "invoice_date_min": "earliest_sales_date_last_6_months",
            "invoice_value_sum": "total_value_last_6_months",
        }, inplace = True)


    customers_summary["trading_span_past_6_months"] = (customers_summary["most_recent_sales_date_last_6_months"].dt.to_period('M').astype(int) - customers_summary["earliest_sales_date_last_6_months"].dt.to_period('M').astype(int)) + 1

    customers_summary = pd.merge(customers_summary, num_unique_sales_months, on = "proxy_customer_id")
    

    customers_summary["trading_consistency_last_6_months"] = round(customers_summary["unique_sales_months"] / customers_summary["trading_span_past_6_months"],1)


    current_period = customers_summary["most_recent_sales_date_last_6_months"].max()
    
    print(current_period)
    
    NOW = current_period + pd.DateOffset(1)

    customers_summary["diff_last_txn_months"] = round(((current_period - customers_summary["most_recent_sales_date_last_6_months"]) / np.timedelta64(1, "M")),0)
    
#     customers_summary["average_monthly_value_last_6_months"] = customers_summary["total_value_last_6_months"] / customers_summary["trading_span_past_6_months"]
    
    max_last_6_months_time_span = 6

    customers_summary["average_monthly_value_last_6_months"] = customers_summary["total_value_last_6_months"] / max_last_6_months_time_span
    
    
    customers_summaries = pd.merge(summaries, customers_summary, on = 'proxy_customer_id')
    
    return customers_summaries



def load_data_and_calculate_summaries(config_path):
    config = read_params(config_path)
    
    raw_scoring_data_path = config["raw_data_config"]["scoring_data_csv"]  
    
    #interim_path = config["interim_data_config"]["interim_data_path"]  
    
    scoring_summaries_data_path = config["processed_data_config"]["processed_scoring_summaries_data_path"]
    
    
    df = pd.read_csv(raw_scoring_data_path)
        
    scoring_summaries = calculate_summaries(df)

    
    scoring_summaries.to_csv(scoring_summaries_data_path, index = False)
    
if __name__=="__main__":
    args = argparse.ArgumentParser()
    args.add_argument("--config", default = "params.yaml")
    parsed_args = args.parse_args()
    load_data_and_calculate_summaries(config_path = parsed_args.config)