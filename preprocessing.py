################################################################################
#
#
#
#       preprocessing.py
#
#       Description: This file contains commonly used preprocessing functions.
#
#       Variables:
#
#           scalers
#
#       Functions:
#
#           split_data(df, stratify, random_seed = 24)
#           remove_outliers(df, k, col_list)
#           scale_data(train, validate, test, columns, strategy)
#
#
################################################################################

import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler, StandardScaler, RobustScaler

################################################################################

scalers = {
    'MinMaxScaler' : MinMaxScaler,
    'StandardScaler' : StandardScaler,
    'RobustScaler' : RobustScaler
}

################################################################################

def split_data(df: pd.core.frame.DataFrame, random_seed: int = 24, stratify: str = None) -> tuple[
    pd.core.frame.DataFrame,
    pd.core.frame.DataFrame,
    pd.core.frame.DataFrame
]:
    '''
        Accepts a DataFrame and returns train, validate, and test DataFrames.
        Splits are performed randomly.

        Proportion of original dataframe that each return dataframe comprises.
        ---------------
        Train:      56% (70% of 80%)
        Validate:   24% (30% of 80%)
        Test:       20%

        Parameters
        ----------
        df : DataFrame
            A Pandas DataFrame containing prepared data. It is expected that
            the input to this function will already have been prepared and
            tidied so that it will be ready for exploratory analysis.

        stratify : str
            A string value containing the name of the column to be stratified
            in the sklearn train_test_split function. This parameter should
            be the name of a column in the df dataframe.

        random_seed : int, default 24
            An integer value to be used as the random number seed. This parameter
            is passed to the random_state argument in the sklearn train_test_split
            function.

        Returns
        -------
        tuple : A tuple containing three Pandas DataFrames for train, validate
            and test datasets.    
    '''
    test_split = 0.2
    train_validate_split = 0.3

    train_validate, test = train_test_split(
        df,
        test_size = test_split,
        random_state = random_seed,
        stratify = stratify if not stratify else df[stratify]
    )
    train, validate = train_test_split(
        train_validate,
        test_size = train_validate_split,
        random_state = random_seed,
        stratify = stratify if not stratify else train_validate[stratify]
    )
    return train, validate, test

################################################################################

def remove_outliers(df: pd.core.frame.DataFrame, k: float, col_list: list[str]) -> pd.core.frame.DataFrame:
    '''
        Remove outliers from a list of columns in a dataframe 
        and return that dataframe.
        
        Parameters
        ----------
        df: DataFrame
            A pandas dataframe containing data from which we want to remove
            outliers.
        
        k: float
            A numeric value that indicates how strict our outlier threshold
            should be. Typically 1.5.

        col_list: list[str]
            A list of columns from which we want to remove outliers.
        
        Returns
        -------
        DataFrame: A pandas dataframe with outliers removed.
    '''
    
    for col in col_list:

        q1, q3 = df[col].quantile([.25, .75])  # get quartiles
        
        iqr = q3 - q1   # calculate interquartile range
        
        upper_bound = q3 + k * iqr   # get upper bound
        lower_bound = q1 - k * iqr   # get lower bound

        # return dataframe without outliers
        
        df = df[(df[col] > lower_bound) & (df[col] < upper_bound)]
        
    return df

################################################################################

def scale_data(train: pd.DataFrame, validate: pd.DataFrame, test: pd.DataFrame, columns: list[str], strategy: str = 'MinMaxScaler') -> tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame
]:
    '''
        Scale all numeric columns using a MinMaxScaler.
    
        Parameters
        ----------
        train: DataFrame
            The training dataset for a machine learning problem.

        validate: DataFrame
            The out of sample validate dataset for a machine learning problem.

        test: DataFrame
            The out of sample test dataset for a machine learning problem.

        columns: list[str]
            A list of the columns that should be scaled.

        strategy: str, default MinMaxScaler
            The name of the scaler to use when scaling. Possible values are 
            ('MinMaxScaler', 'StandardScaler', 'RobustScaler').
    
        Returns
        -------
        tuple(DataFrame): A tuple of three dataframes with all the numeric 
            columns scaled.
    '''

    train_scaled, validate_scaled, test_scaled = train.copy(), validate.copy(), test.copy()

    scaler = scalers[strategy]()
    
    train_scaled[columns] = scaler.fit_transform(train[columns])
    validate_scaled[columns] = scaler.transform(validate[columns])
    test_scaled[columns] = scaler.transform(test[columns])
    
    return train_scaled, validate_scaled, test_scaled