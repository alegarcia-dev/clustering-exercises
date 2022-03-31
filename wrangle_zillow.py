################################################################################
#
#
#
#       wrangle_zillow.py
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

_zillow_file = 'zillow.csv'
_zillow_db = 'zillow'

_sql = '''
SELECT
    *
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