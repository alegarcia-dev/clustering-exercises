################################################################################
#
#
#
#       wrangle_mall.py
#
#       Description: description
#
#       Variables:
#
#           variables
#
#       Functions:
#
#           functions
#
#
#
################################################################################

import os
import pandas as pd

from get_db_url import get_db_url

################################################################################

_mall_file = 'mall.csv'
_mall_db = 'mall_customers'

_sql = 'SELECT * FROM customers;'

################################################################################

def get_mall_data(use_cache: bool = True) -> pd.core.frame.DataFrame:
    # If the file is cached, read from the .csv file
    if os.path.exists(_mall_file) and use_cache:
        return pd.read_csv(_mall_file)
    
    # Otherwise read from the mysql database
    else:
        df = pd.read_sql(_sql, get_db_url(_mall_db))
        df.to_csv(_mall_file, index = False)
        return df

################################################################################

def summarize_data(df):
    print(f'Shape: {df.shape}')
    print('\n', df.info())
    print('\n', df.describe())

################################################################################

def get_upper_outliers(series, k):
    q1, q3 = series.quantile([0.25, 0.75])
    iqr = q3 - q1
    upper_bound = q3 + k * iqr
    return series.apply(lambda x: max([x - upper_bound, 0]))

################################################################################

def get_lower_outliers(series, k):
    q1, q3 = series.quantile([0.25, 0.75])
    iqr = q3 - q1
    lower_bound = q1 - k * iqr
    return series.apply(lambda x: min([x + lower_bound, 0]))

################################################################################

def encode_gender(df):
    return pd.get_dummies(df, columns = ['gender'], drop_first = True)