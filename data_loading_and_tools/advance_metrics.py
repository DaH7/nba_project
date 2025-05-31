import os
import pandas as pd
import random
from sqlalchemy import create_engine
from config import DB_CONFIG
import numpy as np

QUERIES = {
    'ALLSTAR_LR_DATA' :
        """
         SELECT * from staging.new_logr_allstar_data
        """,
    "SEASON_TOTAL":
        """
        select * from final.season_total_2025
        """,
    "SEASON_TEAM_TOTAL":
        """
        select * from final.season_team_total_2025
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

def VOP(team_file,type):
    """
    calculates value of possession (vof), factor and DRB%

    returns: DataFrame with columns ['team', 'team_id', 'season', 'VOP']
    """

    if type == 'sql':
        query = QUERIES.get(team_file, None)
        df = pd.read_sql(query, engine)
    elif type =='csv':
        df = pd.read_csv(team_file)
    else:
        raise ValueError(f"{type} does not exist, pick csv or sql")

    #sums up these stats by season and renames them to lg_name in league_total
    league_totals = (
        df.groupby('season').agg({
            'PTS': 'sum',
            'FG': 'sum',
            'FGA': 'sum',
            'AST': 'sum',
            'TRB': 'sum',
            'ORB': 'sum',
            'TOV': 'sum',
            'FT': 'sum',
            'FTA': 'sum'
        })
        .rename(columns=lambda x: f'lg_{x}')
        .reset_index()
    )

    #VOP
    league_totals['VOP'] = (
        league_totals['lg_PTS'] /
        (league_totals['lg_FGA'] - league_totals['lg_ORB'] + league_totals['lg_TOV'] + 0.44 * league_totals['lg_FTA']))

    #factor
    league_totals['factor'] = ((2/3) -
                               (0.5* (league_totals['lg_AST']/league_totals['lg_FG']))/
                               (2 * (league_totals['lg_FG']/league_totals['lg_FT'])))

    #DRB%
    league_totals['DRB%'] = (league_totals['lg_TRB'] - league_totals['lg_ORB'])/league_totals['lg_TRB']

    result = df[['Team','season','abrv_team','team_id']].drop_duplicates()
    result = result.merge(league_totals[['season', 'VOP','factor','DRB%']], on='season', how='left')

    return result


def player_eff_rating(team_file,player_file,type):
    """
    PLAYER EFFICIENCY RATING:

    calculated PER for players using st
    """
    if type == 'sql':
        query1 = QUERIES.get(player_file, None)
        query2 = QUERIES.get(team_file, None)
        player_df = pd.read_sql(query1, engine)
        team_df = pd.read_sql(query2, engine)
    elif type =='csv':
        player_df = pd.read_csv(player_file)
        team_df = pd.read_csv(team_file)
    else:
        raise ValueError(f"{type} does not exist, pick csv or sql")


if __name__ == '__main__':
    # percentile_group('ALLSTAR_LR_DATA','eFG%','sql')
    temp = VOP('SEASON_TEAM_TOTAL','sql')

    print(temp[temp['season'] == 2024])
