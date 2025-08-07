import pandas as pd
import random
from sqlalchemy import create_engine
from config import DB_CONFIG

QUERIES = {
    "DRAFT_PRIMARY_KEY":
        """
        select * from final.key_draft_2025
        """,

    "TEAM_KEY":
        """
        select * from final.key_all_team_2025
        """,

    "UNDRAFTED_PLAYER_KEY":
        """
        SELECT * from final.key_undrafted_2025
        """,

    "ALL_PLAYER_KEY":
        """
        SELECT * from final.key_all_player_2025
        """,

    "PLAYOFF_KEY":
        """
        SELECT * from final.key_playoff_2024
        """,

    "EXPANDED_SEASON_STANDINNGS":
        """
        select * from staging.expanded_season_standings
        """,

    "NBA_ALLSTAR":
        """
        select * from staging.nba_allstar
        """,

    "SEASON_STAT_AVG":
        """
        select * from final.season_avg_2025
        """,

    "SEASON_STAT_TOTAL":
        """
        select * from final.season_total_2025
        """,

    "PLAYOFF_STAT_AVG":
        """
        select * from staging.player_playoff_stat_avg
        """,

    "PLAYOFF_STAT_TOTAL":
        """
        select * from staging.player_season_stat_total
        """,

    "SEASON_TEAM_STAT_AVG":
        """
        select * from staging.season_team_stat_avg
        """,

    "SEASON_TEAM_STAT_TOTAL":
        """
        select * from staging.season_team_stat_total
        """,

    "MVP":
        """
        select * from final.mvp_2024
        """,

    "MIP":
        """
        select * from final.mip_2025
        """,

    "DPOY":
        """
        select * from final.dpoy_2025
        """,
    "SMOY":
        """
        select * from final.smoy_2025
        """,
    "top75":
        """
        select * from top_75_players_draft 
        """,
    "TEMP":
        """
        select * from all_defense_long
        """

}

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def matching_player_id(query_key,query_df,type,to_db='no'):
    """
    MATCHES PLAYER ID TO PLAYER NAME KEY
    sql: ADD THE NEEDED QUERY AND CALL IT TO query_df
    csv: INPUT THE CSV FILE TO query_df
    """

    query = QUERIES.get(query_key,None)
    key = pd.read_sql(query, engine)

    if type == "sql":
        query_2 = QUERIES.get(query_df,None)
        df = pd.read_sql(query_2,engine)
    elif type =="csv":
        df = pd.read_csv(f"{query_df}")
        df.columns = df.columns.str.strip()
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    #clean names from df
    df['abrv_team'] = df['abrv_team'].str.replace('*', '', regex=False)
    df['Player'] = df['Player'].str.strip()
    key['Player'] = key['Player'].str.strip()

    #clean names for CHAMPIONSHIP DATASET
    # df['runnerup_abrv_team'] = df['runnerup_abrv_team'].str.replace('*', '', regex=False)
    # df['champ_abrv_team'] = df['champ_abrv_team'].str.replace('*', '', regex=False)
    # df['Finals MVP'] = df['Finals MVP'].str.strip()
    # key['Player'] = key['Player'].str.strip()


    #default code
    key['player_id'] = key['player_id'].astype('int64')
    new_df = df.merge(
        key[['Player',"season",'player_id']],
        on = ['Player',"season"],
        how = 'left'
    )

    #CHAMPIONSHIP DATASET
    # new_df = df.merge(
    #     key[['Player', 'season', 'player_id']],
    #     left_on=['Finals MVP', 'Year'],
    #     right_on=['Player', 'season'],
    #     how='left'
    # )

    unmatched = new_df[new_df['player_id'].isna()]
    new_df = new_df.drop_duplicates(keep='first')

    print(f"Unmatched entries: {len(unmatched)}")
    print("Unique Player in df:", df['Player'].nunique())
    print("Unique Player in key:", key['Player'].nunique())



    new_df.to_csv('player_id_test')

    if to_db == "yes":
        new_df.to_sql('player_id_test', engine, if_exists='replace', index=False)
    else:
        print('No data sent to db')

def matching_team_id(query_key,query_df,type,to_db ='no'):
    """
    MATCHES TEAM ID WITH ABV TEAM NAMES FROM TEAM KEY
    sql: ADD THE NEEDED QUERY AND CALL IT TO query_df
    csv: INPUT THE CSV FILE TO query_df
    """
    query = QUERIES.get(query_key,None)
    key = pd.read_sql(query, engine)
    if type == "sql":
        query_2 = QUERIES.get(query_df,None)
        df = pd.read_sql(query_2,engine)
    elif type =="csv":
        df = pd.read_csv(f"{query_df}")
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    #clean names from df
    key.columns = key.columns.str.strip()
    df.columns = df.columns.str.strip()


    #default code
    key['team_id'] = key['team_id'].astype('int64')
    new_df = df.merge(
        key[['abrv_team',"season", 'team_id']],
        on = ['abrv_team',"season"],
        how = 'left'
    )

    # #CHAMPSHIP DATASET
    # key['team_id'] = key['team_id'].astype('int64')
    # new_df = df.merge(
    #     key[['abrv_team',"season", 'team_id']],
    #     left_on = ['runnerup_abrv_team', 'Year'],
    #     right_on = ['abrv_team',"season"],
    #     how = 'left'
    # )

    unmatched = new_df[new_df['team_id'].isna()]
    new_df = new_df.drop_duplicates(keep='first')

    print(f"Unmatched entries: {len(unmatched)}")
    # print("Unique abrv_teams in df:", df['abrv_team'].nunique())
    print("Unique abrv_teams in key:", key['abrv_team'].nunique())

    # # CHAMPSHIP DATASET
    # print("Unique runnerup_abrv_team in df:", df['runnerup_abrv_team'].nunique())
    # print("Unique champ_abrv_team in df:", df['champ_abrv_team'].nunique())

    new_df.to_csv('team_id_test')

    if to_db == "yes":
        new_df.to_sql('team_id_test', engine, if_exists='replace', index=False)
    else:
        print('No data sent to db')

def matching_team(query_key, query_df,type,to_db= 'no'):

    """
    sql: ADD THE NEEDED QUERY AND CALL IT TO query_df
    csv: INPUT THE CSV FILE TO query_df
    """

    query = QUERIES.get(query_key,None)
    key = pd.read_sql(query, engine)
    if type == "sql":
        query_2 = QUERIES.get(query_df,None)
        df = pd.read_sql(query_2,engine)
    elif type =="csv":
        df = pd.read_csv(f"{query_df}")
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    # clean names from df
    df['abrv_team'] = df['abrv_team'].str.replace('*', '', regex=False)
    df['abrv_team'] = df['abrv_team'].str.strip()
    key['abrv_team'] = key['abrv_team'].str.strip()

    new_df = df.merge(
        key[['abrv_team','team_id','Team']],
        on=['abrv_team','team_id'],
        how='left'
    )

    unmatched = new_df[new_df['team_id'].isna()]
    new_df = new_df.drop_duplicates(keep='first')

    print(f"Unmatched entries: {len(unmatched)}")
    print("Unique Teams in df:", df['abrv_team'].nunique())
    print("Unique Teams in key:", key['abrv_team'].nunique())


    new_df.to_csv('team_name_test')

    if to_db == "yes":
        new_df.to_sql('team_name_test', engine, if_exists='replace', index=False)
        print('df sent to db')
    else:
        print('No data sent to db')

def generating_new_id(query,id_length):
    df = pd.read_sql(f'query',engine)
    print(df)

    def generate_player_id(length):
        return ''.join([str(random.randint(0,9)) for _ in range(length)])

    player_ids = []
    for index,row in df.iterrows():
        random_digits = generate_player_id(f'{id_length}')
        player_id = f"{9000}{random_digits}"
        player_ids.append(player_id)
    df['player_id'] = player_ids
    df.to_csv('undrafted_player_key', index=False)

    # #to db
    # df = pd.read_csv('undrafted_player_key')
    # df.to_sql('undrafted_player_key', engine, if_exists='replace', index=False)

def matching_id_by_name(query_key,query_df,type,to_db='no'):
    """
        added PLAYER ID from key by player name and season
        sql: ADD THE NEEDED QUERY AND CALL IT TO query_df
        csv: INPUT THE CSV FILE TO query_df
        """

    query = QUERIES.get(query_key, None)
    key = pd.read_sql(query, engine)

    if type == "sql":
        query_2 = QUERIES.get(query_df, None)
        df = pd.read_sql(query_2, engine)
    elif type == "csv":
        df = pd.read_csv(f"{query_df}")
        df.columns = df.columns.str.strip()
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    df['Player'] = df['Player'].str.strip()
    key['Player'] = key['Player'].str.strip()


    if 'Season' not in df.columns or 'season' not in key.columns:
        raise KeyError("Both dataframes must contain 'Player' and season columns.")

    #first match
    key['Player'] = key['Player']
    new_df = df.merge(
        key[['Player',"season","player_id"]],
        left_on = ['Player','Season'],
        right_on = ['Player', 'season'],
        how = 'left'
    )

    unmatched = new_df[new_df['player_id'].isna()]
    new_df = new_df.drop_duplicates(keep='first')

    matched = new_df[new_df['player_id'].notna()].copy()
    unmatched = new_df[new_df['player_id'].isna()].copy()

    #second match
    # fallback = unmatched.drop(columns=['player_id', 'season'])  # drop columns to avoid conflict
    # fallback = fallback.merge(
    #     key[['Player', 'player_id']],
    #     on='Player',
    #     how='left'
    # )
    #
    # final_df = pd.concat([matched, fallback], ignore_index=True)

    print(f"Total rows: {len(df)}")
    print(f"Matched by Player+Season: {len(matched)}")
    # print(f"Fallback matched by Player only: {fallback['player_id'].notna().sum()}")
    # print(f"Still unmatched: {final_df['player_id'].isna().sum()}")

    new_df.to_csv('player_id_test')

    if to_db == "yes":
        new_df.to_sql('player_id_test', engine, if_exists='replace', index=False)
    else:
        print('No data sent to db')



if __name__ == "__main__":
    # matching_team_id("ROY_KEY","TEMP","csv")
    # matching_team_id("TEAM_KEY","team_id_test",'csv',"yes")
    # matching_player_id("ALL_PLAYER_KEY","all_defense_long","csv")
    matching_id_by_name("ALL_PLAYER_KEY","all_rookie_long","csv","yes")