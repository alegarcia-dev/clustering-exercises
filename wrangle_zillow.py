################################################################################
#
#
#
#       wrangle_zillow.py
#
#       Description: This file contains the function for the clustering wrangle
#           exercises
#
#       Variables:
#
#           _zillow_file
#           _zillow_db
#           _sql
#
#       Functions:
#
#           get_zillow_data(use_cache = True)
#           summarize_column_nulls(df)
#           summarize_row_nulls(df)
#           prepare_zillow(df)
#           get_single_unit_properties(df)
#           handle_missing_values(df, prop_required_column, prop_required_row)
#           impute_missing_values(df, columns_strategy)
#
#
################################################################################

import os
import pandas as pd

from sklearn.impute import SimpleImputer

from get_db_url import get_db_url
from preprocessing import split_data

################################################################################

_zillow_file = 'zillow.csv'
_zillow_db = 'zillow'

_sql = '''
SELECT
    properties_2017.*,
    logerror,
    transactiondate,
    typeconstructiondesc,
    airconditioningdesc,
    architecturalstyledesc,
    buildingclassdesc,
    propertylandusedesc,
    storydesc,
    heatingorsystemdesc
FROM properties_2017
JOIN predictions_2017 ON properties_2017.parcelid = predictions_2017.parcelid
    AND predictions_2017.transactiondate LIKE '2017%%'
LEFT JOIN typeconstructiontype USING (typeconstructiontypeid)
LEFT JOIN airconditioningtype USING (airconditioningtypeid)
LEFT JOIN architecturalstyletype USING (architecturalstyletypeid)
LEFT JOIN buildingclasstype USING (buildingclasstypeid)
LEFT JOIN propertylandusetype USING (propertylandusetypeid)
LEFT JOIN storytype USING (storytypeid)
LEFT JOIN heatingorsystemtype USING (heatingorsystemtypeid)
    
JOIN (
    SELECT
        parcelid,
        MAX(transactiondate) AS date
    FROM predictions_2017
    GROUP BY parcelid
) AS max_dates ON properties_2017.parcelid = max_dates.parcelid
    AND predictions_2017.transactiondate = max_dates.date
    
WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
'''

################################################################################

def get_zillow_data(use_cache: bool = True) -> pd.core.frame.DataFrame:
    '''
        Return a dataframe containing data from the zillow dataset.

        If a zillow.csv file containing the data does not already
        exist the data will be cached in that file inside the current
        working directory. Otherwise, the data will be read from the
        .csv file.

        Parameters
        ----------
        use_cache: bool, default True
            If True the dataset will be retrieved from a csv file if one
            exists, otherwise, it will be retrieved from the MySQL database. 
            If False the dataset will be retrieved from the MySQL database
            even if the csv file exists.

        Returns
        -------
        DataFrame: A Pandas DataFrame containing the data from the zillow
            dataset is returned.
    '''

    # If the file is cached, read from the .csv file
    if os.path.exists(_zillow_file) and use_cache:
        return pd.read_csv(_zillow_file)
    
    # Otherwise read from the mysql database
    else:
        df = pd.read_sql(_sql, get_db_url(_zillow_db))
        df.to_csv(_zillow_file, index = False)
        return df

################################################################################

def summarize_column_nulls(df):
    return pd.concat([
        zillow.isnull().sum().rename('rows_missing'),
        zillow.isnull().mean().rename('percent_missing')
    ], axis = 1)

################################################################################

def summarize_row_nulls(df):
    return pd.concat([
        zillow.isnull().sum(axis = 1).rename('columns_missing'),
        zillow.isnull().mean(axis = 1).rename('percent_missing')
    ], axis = 1).value_counts().sort_index()

################################################################################

def prepare_zillow(df):
    columns = [
        'typeconstructiontypeid',
        'airconditioningtypeid',
        'architecturalstyletypeid',
        'buildingclasstypeid',
        'propertylandusetypeid',
        'storytypeid',
        'heatingorsystemtypeid',
        'propertylandusetypeid'
    ]
    df = df.drop(columns = columns)

    df = get_single_unit_properties(df)
    df = handle_missing_values(df, 0.90, 0.90)

    columns_strategy = {
        'mean' : [
            'calculatedfinishedsquarefeet',
            'finishedsquarefeet12',
            'structuretaxvaluedollarcnt',
            'taxvaluedollarcnt',
            'landtaxvaluedollarcnt',
            'taxamount'
        ],
        'most_frequent' : [
            'calculatedbathnbr',
            'fullbathcnt',
            'regionidcity',
            'regionidzip',
            'yearbuilt'
        ],
        'median' : [
            'censustractandblock'
        ]
    }

    return impute_missing_values(df, columns_strategy)

################################################################################

def get_single_unit_properties(df):
    property_types = [
        'Single Family Residential',
        'Condominium',
        'Cluster Home',
        'Mobile Home',
        'Manufactured, Modular, Prefabricated Homes',
        'Residential General',
        'Townhouse'
    ]
    df = df[df.propertylandusedesc.isin(property_types)]
    
    df = df[(df.unitcnt == 1) | (df.unitcnt.isnull())]
    
    return df

################################################################################

def handle_missing_values(df, prop_required_column, prop_required_row):
    df = df.dropna(axis = 'columns', thresh = round(df.shape[0] * prop_required_column))
    df = df.dropna(axis = 'index', thresh = round(df.shape[1] * prop_required_row))
    
    return df

################################################################################

def impute_missing_values(df, columns_strategy):
    train, validate, test = split_data(df)
    
    for strategy, columns in columns_strategy.items():
        imputer = SimpleImputer(strategy = strategy)
        imputer.fit(train[columns])

        train[columns] = imputer.transform(train[columns])
        validate[columns] = imputer.transform(validate[columns])
        test[columns] = imputer.transform(test[columns])
        
    return train, validate, test