import os
import pandas as pd
import random
from sqlalchemy import create_engine
from config import DB_CONFIG
import numpy as np

QUERIES = {
    "SEASON_2025":
        """
        select * from updated_season_2025
        """,
    'ALLSTAR_LR_DATA' :
        """
         SELECT * from staging.logr_allstar_data
        """
           }





engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def percentile_group(file,stat,type):
    """
    Adds percentiles and percentile groups to a numeric stat and normalizes it be season
    """
    if type == 'sql':
        query = QUERIES.get(file, None)
        df = pd.read_sql(query, engine)
    elif type =='csv':
        df = pd.read_csv(file)
    else:
        raise ValueError(f"{type} does not exist, pick csv or sql")

    # actual percentage
    df[f'{stat} percentile'] = (df.groupby('season')[f'{stat}'].rank(pct=True)*100).round(1)

    def label_percentile(p):
        if pd.isna(p):
            return np.nan
        if p >= 99:
            return '99th percentile'
        elif p >= 95:
            return '95th percentile'
        elif p >= 90:
            return '90th percentile'
        elif p >= 80:
            return '80th percentile'
        elif p >= 70:
            return '70th percentile'
        elif p >= 60:
            return '60th percentile'
        elif p >= 50:
            return '50th percentile'
        else:
            return '<50th percentile'

    # percentile group
    df[f'{stat} percentile group'] = df[f'{stat} percentile'].apply(label_percentile)


    # df.to_csv(f'{file}.csv',index = False)
    df.to_csv('award_adjusted',index = False)

    print(f'{stat} percentile added')


if __name__ == '__main__':
    percentile_group('ALLSTAR_LR_DATA','MP','sql')
