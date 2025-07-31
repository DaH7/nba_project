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
    "OPP_TEAM_TOTAL":
        """
        select * from opp_team_stats_total
        """,
        "PER":
        """
        select * from staging.PER_2025
        """,
        "LOG_R_DATA":
        """
        SELECT * FROM raw_logr_allstar_data
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
    df.to_csv(f'{stat} percentile',index = False)

    print(f'{stat}_percentile added')

def PER(player_file, team_file, opp_file, type_):
    """
    Calculates PER, value of possession (VOP), factor, DRB%, possessions, and pace.

    Returns:
        DataFrame with ['player_id', 'Player', 'season', 'Team', 'team_id', 'PER'] columns.
    """
    # loads data
    if type_ == 'sql':
        # Replace QUERIES and engine with your SQL setup
        team_query = QUERIES.get(team_file, None)
        player_query = QUERIES.get(player_file, None)
        opp_query = QUERIES.get(opp_file, None)
        player_df = pd.read_sql(text(player_query), engine)
        team_df = pd.read_sql(text(team_query), engine)
        opp_df = pd.read_sql(text(opp_query), engine)

    elif type_ == 'csv':
        player_df = pd.read_csv(player_file)
        team_df = pd.read_csv(team_file)
        opp_df = pd.read_csv(opp_file)

    else:
        raise ValueError(f"{type_} does not exist, pick 'csv' or 'sql'")

    # aggregates league totals of team data by season
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

    # aggregates opponents team totals by team data by season
    opp_totals = (
        opp_df.groupby(['team_id', 'season']).agg({
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
            'PF': 'sum'
        })
        .rename(columns=lambda x: f'opp_{x}')
        .reset_index()
    )

    # calculated VOP, factor, and DRB%
    league_totals['VOP'] = league_totals['lg_PTS'] / (
        league_totals['lg_FGA'] - league_totals['lg_ORB'] + league_totals['lg_TOV'] + 0.44 * league_totals['lg_FTA']
    )

    league_totals['factor'] = (
        (2 / 3) -
        (0.5 * (league_totals['lg_AST'] / league_totals['lg_FG'])) /
        (2 * (league_totals['lg_FG'] / league_totals['lg_FT']))
    )

    league_totals['DRB%'] = (league_totals['lg_TRB'] - league_totals['lg_ORB']) / league_totals['lg_TRB']

    # merges opp totals into team data for poss and pace calculations
    team_df = team_df.merge(opp_totals, on=['team_id', 'season'], how='left')

    # calculates poss and pace at team level
    team_df['Poss'] = team_df['FGA'] - team_df['ORB'] + team_df['TOV'] + 0.44 * team_df['FTA']
    team_df['Opp_Poss'] = team_df['opp_FGA'] - team_df['opp_ORB'] + team_df['opp_TOV']  + 0.44 * team_df['opp_FTA']
    team_df['Team_MP'] = team_df['G'] * 240  # 5 players * 48 minutes * games
    team_df['Pace'] = 48 * ((team_df['Poss'] + team_df['Opp_Poss']) / (2 * (team_df['Team_MP'] / 5)))

    # prepares data before merging to player_df
    team_stats = team_df[['team_id', 'season', 'AST', 'FG', 'FGA', 'FTA', 'ORB', 'TOV', 'PF', 'PTS', 'PA/G', 'G', 'MP','Poss','Opp_Poss']].drop_duplicates()
    league_stats = league_totals[['season', 'VOP', 'factor', 'DRB%', 'lg_FT', 'lg_PF', 'lg_FTA']].drop_duplicates()
    pace_df = team_df[['team_id', 'season', 'Pace']].drop_duplicates()

    # merges team stats, league stats, opponent totals, and pace into player_data
    player_df = (
        player_df
        .merge(team_stats, on=['team_id', 'season'], how='left', suffixes=('', '_team'))
        .merge(league_stats, on='season', how='left')
        .merge(opp_totals, on=['team_id', 'season'], how='left')
        .merge(pace_df, on=['team_id', 'season'], how='left')
    )

    # deals with missing columns and lack of data
    def handle_missing_stats(df):
        if '3P' not in df.columns:
            df['3P'] = 0
        if 'TOV' not in df.columns:
            df['TOV'] = 0
        if 'STL' not in df.columns:
            df['STL'] = 0
        if 'BLK' not in df.columns:
            df['BLK'] = 0
        if 'ORB' not in df.columns and 'TRB' in df.columns:
            df['ORB'] = 0.3 * df['TRB']
        return df

    player_df = handle_missing_stats(player_df)

    # fills vop and drb% with defaults following bball ref
    player_df['VOP'] = player_df['VOP'].fillna(1)
    player_df['DRB%'] = player_df['DRB%'].fillna(0.7)

    #filters out players with 0 mins, avoid division errors
    player_df = player_df[player_df['MP'] > 0].copy()

    # calculates uPER
    player_df['uPER'] = (1 / player_df['MP']) * (
        player_df['3P']
        + (2 / 3) * player_df['AST']
        + (2 - player_df['factor'] * (player_df['AST_team'] / player_df['FG_team'])) * player_df['FG']
        + (player_df['FT'] * 0.5 * (1 + (1 - (player_df['AST_team'] / player_df['FG_team'])) + (2 / 3) * (player_df['AST_team'] / player_df['FG_team'])))
        - player_df['VOP'] * player_df['TOV']
        - player_df['VOP'] * player_df['DRB%'] * (player_df['FGA'] - player_df['FG'])
        - player_df['VOP'] * 0.44 * (0.44 + (0.56 * player_df['DRB%'])) * (player_df['FTA'] - player_df['FT'])
        + player_df['VOP'] * (1 - player_df['DRB%']) * (player_df['TRB'] - player_df['ORB'])
        + player_df['VOP'] * player_df['DRB%'] * player_df['ORB']
        + player_df['VOP'] * player_df['STL']
        + player_df['VOP'] * player_df['DRB%'] * player_df['BLK']
        - player_df['PF'] * ((player_df['lg_FT'] / player_df['lg_PF']) - 0.44 * (player_df['lg_FTA'] / player_df['lg_PF']) * player_df['VOP'])
    )

    # calculates league poss and pace
    league_totals['Poss'] = league_totals['lg_FGA'] + 0.44 * league_totals['lg_FTA'] - league_totals['lg_ORB'] + league_totals['lg_TOV']
    league_totals = league_totals[
        (league_totals['lg_MP'] > 0) &
        (league_totals['Poss'] > 0)
        ].copy()

    league_totals['league_Pace'] = 48 * (league_totals['Poss'] / (league_totals['lg_MP'] / 5))

    # checks and cleans bad values from missing data
    league_totals = league_totals.replace([np.inf, -np.inf], np.nan)
    league_totals = league_totals.dropna(subset=['league_Pace'])

    # fill missing offensive stats with 0 for poss calculation
    team_df[['FGA', 'FTA', 'ORB', 'TOV']] = team_df[['FGA', 'FTA', 'ORB', 'TOV']].fillna(0)
    team_df[['opp_FGA', 'opp_FTA', 'opp_ORB', 'opp_TOV']] = team_df[
        ['opp_FGA', 'opp_FTA', 'opp_ORB', 'opp_TOV']].fillna(0)

    #recalculate team poss  and opp poss
    team_df['Poss'] = team_df['FGA'] + 0.44 * team_df['FTA'] - team_df['ORB'] + team_df['TOV']
    team_df['Opp_Poss'] = team_df['opp_FGA'] + 0.44 * team_df['opp_FTA'] - team_df['opp_ORB'] + team_df['opp_TOV']

    # merge league pace into player_df
    league_pace_df = league_totals[['season', 'league_Pace']].drop_duplicates()
    player_df = player_df.merge(league_pace_df, on='season', how='left')

    # calculate adjusted PER (aPER)
    player_df['aPER'] = player_df['uPER'] * (player_df['league_Pace'] / player_df['Pace'])

    # calculate league average aPER weighted by minutes
    league_aPER = (player_df['aPER'] * player_df['MP']).sum() / player_df['MP'].sum()

    # normalize PER so league average = 15
    # band aid fix with -10 PER for now as it is the current diff between my per and bball reference
    player_df['PER'] = player_df['aPER'] * (15 / league_aPER) - 10

    # drop duplicate player season-team rows
    player_df = player_df.drop_duplicates(subset=['player_id', 'season', 'team_id'])

    # filter out players with very low minutes and low games (unstable PER)
    player_df = player_df[(player_df['MP'] >= 500) & (player_df['G'] >= 27)]

    # # --- Debug prints for checking intermediate results ---
    #
    # print("\nTeam possessions and Opponent possessions (team_df head):")
    # print(team_df[['team_id', 'season', 'Poss', 'Opp_Poss']].head())
    #
    # print("\nLeague totals VOP and factor:")
    # print(league_totals[['season', 'VOP', 'factor']].head())
    #
    # print("\nPace statistics:")
    # print(team_df[['team_id', 'season', 'Pace']].describe())
    # print(league_totals[['season', 'league_Pace']].describe())

    # --- Return key columns ---
    return player_df[['player_id', 'Player', 'season', 'Team', 'team_id', 'PER','Pace','Poss','Opp_Poss','G','MP']]



if __name__ == '__main__':
    percentile_group('PER','PER','sql')
    # temp = PER('SEASON_TOTAL','SEASON_TEAM_TOTAL','OPP_TEAM_TOTAL','sql')
    # temp.to_csv('temp', index = False)
    # print(temp)
