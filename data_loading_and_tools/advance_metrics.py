import os
import pandas as pd
import random
from sqlalchemy import create_engine, text
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
        select a."Rk", a."Team", a."G",a."MP",a."FG",a."FGA",a."FG%",
        a."3P",a."3PA", a."3P%",a."2P",a."2PA",a."2P%",a."FT",a."FTA",a."FT%",
        a."ORB",a."DRB",a."TRB",a."AST",a."STL",a."BLK",a."TOV",a."PF",
        a."PTS",a."abrv_team",a."playoff",b."W",b."L",b."W/L%",b."GB",b."PS/G",
        b."PA/G",b."SRS",a."season",a."team_id"
            from final.season_team_total_2025 a
            left join staging.division_stats b
                on a.team_id = b.team_id
                and a.season = b.season
        """,
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

def PER(player_file, team_file, type_):
    """
    Calculates PER, value of possession (VOP), factor, and DRB%.

    Returns: DataFrame with columns ['player_id', 'Player', 'season', 'team_id', 'PER']
    """

    if type_ == 'sql':
        team_query = QUERIES.get(team_file, None)
        player_query = QUERIES.get(player_file, None)
        player_df = pd.read_sql(text(player_query), engine)
        team_df = pd.read_sql(text(team_query), engine)
    elif type_ == 'csv':
        player_df = pd.read_csv(player_file)
        team_df = pd.read_csv(team_file)
    else:
        raise ValueError(f"{type_} does not exist, pick 'csv' or 'sql'")

    # Summing stats by season for league totals
    league_totals = (
        team_df.groupby('season').agg({
            'PTS': 'sum',
            'MP': 'sum',
            'FG': 'sum',
            'FGA': 'sum',
            'AST': 'sum',
            'TRB': 'sum',
            'ORB': 'sum',
            'TOV': 'sum',
            'FT': 'sum',
            'FTA': 'sum',
            'PS/G': 'sum',
            'PA/G': 'sum',
            'PF': 'sum'
        })
        .rename(columns=lambda x: f'lg_{x}')
        .reset_index()
    )

    # Calculate VOP
    league_totals['VOP'] = (
        league_totals['lg_PTS'] /
        (league_totals['lg_FGA'] - league_totals['lg_ORB'] + league_totals['lg_TOV'] + 0.44 * league_totals['lg_FTA'])
    )

    # Calculate factor
    league_totals['factor'] = (
        (2/3) -
        (0.5 * (league_totals['lg_AST'] / league_totals['lg_FG'])) /
        (2 * (league_totals['lg_FG'] / league_totals['lg_FT']))
    )

    # Calculate DRB%
    league_totals['DRB%'] = (league_totals['lg_TRB'] - league_totals['lg_ORB']) / league_totals['lg_TRB']

    # Prepare a DataFrame for merging VOP, factor, DRB% by season
    result = team_df[['Team', 'season', 'abrv_team', 'team_id']].drop_duplicates()
    result = result.merge(league_totals[['season', 'VOP', 'factor', 'DRB%']], on='season', how='left')

    # Ensure team stats used for merging are unique per team-season
    team_stats = team_df[['team_id', 'season', 'AST', 'FG', 'FGA', 'FTA', 'ORB', 'TOV', 'PF', 'PTS', 'PA/G', 'G', 'MP']].drop_duplicates()

    # Merge team stats into player_df
    player_df = player_df.merge(team_stats, on=['team_id', 'season'], how='left', suffixes=('', '_team'))

    # Merge league-wide stats into player_df
    league_stats = league_totals[['season', 'VOP', 'factor', 'DRB%', 'lg_FT', 'lg_PF', 'lg_FTA']].drop_duplicates()
    player_df = player_df.merge(league_stats, on='season', how='left')

    # Handle missing player stats columns
    def handle_missing_stats(df):
        if '3P' not in df.columns:
            df['3P'] = 0
        if 'TOV' not in df.columns:
            df['TOV'] = 0
        if 'STL' not in df.columns:
            df['STL'] = 0
        if 'BLK' not in df.columns:
            df['BLK'] = 0
        if 'ORB' not in df.columns:
            df['ORB'] = 0.3 * df['TRB']
        return df

    player_df = handle_missing_stats(player_df)

    # Fill missing VOP and DRB%
    player_df['VOP'] = player_df['VOP'].fillna(1)
    player_df['DRB%'] = player_df['DRB%'].fillna(0.7)

    # Filter out players with zero or missing minutes (to avoid division errors)
    player_df = player_df[player_df['MP'] > 0].copy()

    # Calculate unadjusted PER (uPER)
    player_df['uPER'] = (1 / player_df['MP']) * (
        player_df['3P']
        + (2 / 3) * player_df['AST']
        + (2 - player_df['factor'] * (player_df['AST_team'] / player_df['FG_team'])) * player_df['FG']
        + (
            player_df['FT']
            * 0.5
            * (
                1
                + (1 - (player_df['AST_team'] / player_df['FG_team']))
                + (2 / 3) * (player_df['AST_team'] / player_df['FG_team'])
            )
        )
        - player_df['VOP'] * player_df['TOV']
        - player_df['VOP'] * player_df['DRB%'] * (player_df['FGA'] - player_df['FG'])
        - player_df['VOP'] * 0.44 * (0.44 + (0.56 * player_df['DRB%'])) * (player_df['FTA'] - player_df['FT'])
        + player_df['VOP'] * (1 - player_df['DRB%']) * (player_df['TRB'] - player_df['ORB'])
        + player_df['VOP'] * player_df['DRB%'] * player_df['ORB']
        + player_df['VOP'] * player_df['STL']
        + player_df['VOP'] * player_df['DRB%'] * player_df['BLK']
        - player_df['PF'] * (
            (player_df['lg_FT'] / player_df['lg_PF'])
            - 0.44 * (player_df['lg_FTA'] / player_df['lg_PF']) * player_df['VOP']
        )
    )

    # Calculate team possessions
    team_df['Poss'] = (
        team_df['FGA']
        + 0.44 * team_df['FTA']
        - team_df['ORB']
        + team_df['TOV']
    )

    # Calculate opponent possessions scaled by points allowed to points scored
    team_df['PA'] = team_df['PA/G'] * team_df['G']
    team_df['Opp_Poss'] = (team_df['PA'] / team_df['PTS']) * team_df['Poss']

    # Calculate team minutes played (5 players * 48 minutes * games)
    team_df['Team_MP'] = team_df['G'] * 240  # 5 * 48 = 240

    # Calculate team pace factor
    team_df['Pace'] = 48 * ((team_df['Poss'] + team_df['Opp_Poss']) / (2 * (team_df['Team_MP'] / 5)))

    # Calculate league possessions
    league_totals['Poss'] = (
        league_totals['lg_FGA']
        + 0.44 * league_totals['lg_FTA']
        - league_totals['lg_ORB']
        + league_totals['lg_TOV']
    )
    league_totals['MP_total'] = league_totals['lg_MP']

    # Calculate league pace
    league_totals['league_Pace'] = 48 * (league_totals['Poss'] / (league_totals['MP_total'] / 5))

    # Merge pace data back into player_df
    pace_df = team_df[['team_id', 'season', 'Pace']].drop_duplicates()
    player_df = player_df.merge(pace_df, on=['team_id', 'season'], how='left')

    # Merge league pace
    league_pace_df = league_totals[['season', 'league_Pace']].drop_duplicates()
    player_df = player_df.merge(league_pace_df, on='season', how='left')

    # Calculate adjusted PER (aPER)
    player_df['aPER'] = player_df['uPER'] * (player_df['league_Pace'] / player_df['Pace'])

    # Calculate league average aPER weighted by minutes played
    league_aPER = (player_df['aPER'] * player_df['MP']).sum() / player_df['MP'].sum()

    # Final PER normalized so league average is 15
    player_df['PER'] = player_df['aPER'] * (15 / league_aPER)

    # Drop duplicates if any remain for clean output
    player_df = player_df.drop_duplicates(subset=['player_id', 'season', 'team_id'])

    # Filter out players with very low minutes to avoid unstable PER
    player_df = player_df[player_df['MP'] >= 250]  # filter threshold

    # Return desired columns
    return player_df[['player_id', 'Player', 'season', 'Team','team_id', 'PER']]


if __name__ == '__main__':
    # percentile_group('ALLSTAR_LR_DATA','eFG%','sql')
    temp = PER('SEASON_TOTAL','SEASON_TEAM_TOTAL','sql')
    temp.to_csv('temp', index = False)
    print(temp)
