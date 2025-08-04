import os
import pandas as pd
import random
from sqlalchemy import create_engine
from config import DB_CONFIG
import re


QUERIES = {
    "SEASON_TEAM_TOTAL":
        """
        select * from season_team_total
        """,

    "abbrv_team":
        """
        select distinct("Team") from final.key_all_player_2025 
        order by "Team" asc
        """,

        "allstar_team":
        """
        select * from final.nba_allstar_2025
        """,

        "expanded_standings":
            """
            select * from expanded_standings
            """,

    "KEY_DPOY":
        """
        select "Player","season","player_id" from final.dpoy_2025
        """,

    "KEY_ALLSTAR":
        """
        select "Player","season","player_id" from final.key_allstar_2025
        """,

    "KEY_MIP":
        """
        select "Player","season","player_id" from final.mip_2025
        """,

    "KEY_SMOY":
        """
        select "Player","season","player_id" from final.smoy_2025
        """,

    "KEY_MVP":
        """
        select "Player","season","player_id" from final.mvp_2025
        """,

    "KEY_ROY":
        """
        select "Player","season","player_id" from final.roy_2025
        """,

    "KEY_CHAMPIONSHIP":
        """
        select * from final.KEY_CHAMPIONSHIP
        """,

    "old_all_div":
        """
        SELECT * from old_all_div
        """,
    "e_div":
        """
        SELECT * from east_div
        """,
    "w_div":
        """
        SELECT * from west_div
        """,
    "TEMP":
        """
        SELECT * from staging.top_75_players
        """,
}
engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


def removing_rows(file_path):
    #for a folder full of csv
    # for file in os.listdir(root_dir):
    #     if file.endswith('.csv'):
    #         file_path = os.path.join(root_dir, file) #creates path

            df = pd.read_csv(file_path, header= 1)  ##skips 1st row and uses 2nd as header
            # df = df.iloc[0:]  # drop 1st row
            # df = df.iloc[:-1]  #drops last row

            df.to_csv(file_path, index=False)  #back to csv
            print(f'{file_path} saved')

def remove_col(csv_file):
    df = pd.read_csv(csv_file)
    #remove last col
    # df = df.iloc[:,:-1]
    #remove first column
    df = df.iloc[:,1:]
    df.to_csv(csv_file, index=False)
    print(f'{csv_file} saved')

def awards_season_retool(file_path):
    # for group retooling in folder
    # for file in os.listdir(root_dir):
    #     if file.endswith('.csv'):
    #         file_path = os.path.join(root_dir, file) #creates path

            df = pd.read_csv(file_path)
            df['Season'] = df['Season'].astype(str).apply(lambda x: x[:2] + x[5:])
            df.iloc[:, 0] = df.iloc[:, 0].str.lower()
            df.to_csv(file_path, index=False)
            print(f'{file_path} saved')

def team_retool(data,type):
    '''
    ADDS  TEAM ABRV AND IF PLAYER MADE PLAYOFF THAT SEASON
    sql: from db
    csv : from csv
    '''
    # add 2 new columns, playoffs and Tm (the abbrivations for team)
    if type == 'csv':
        df = pd.read_csv(data)
    elif type == 'sql':
        query = QUERIES.get(data,None)
        df = pd.read_sql(query,engine)
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    # FOR CHAMPIONSHIP DATASET
    # df['champ_abrv_team'] = ''
    # df['runnerup_abrv_team'] = ''

    df['abrv_team'] = ''
    # df['playoff'] = ''
    # # if team has * in their name, playoffs columns is yes, otherwise no
    # df['playoff'] = df['Team'].str.contains(r'\*').map({True: 'yes', False: 'no'})
    # clean all * from name, make a dictionary of team names shorthand longhand
    df['Team'] = df['Team'].str.strip()
    df['Team'] = df['Team'].str.replace('*', '', regex=False)
    df=df.drop_duplicates()

    team_abbr_dict = {
        # Current Teams (including historical names)
        'Atlanta Hawks': 'ATL',
        'Boston Celtics': 'BOS',
        'Brooklyn Nets': 'BRK',
        'Charlotte Hornets': 'CHO',
        'Chicago Bulls': 'CHI',
        'Cleveland Cavaliers': 'CLE',
        'Dallas Mavericks': 'DAL',
        'Denver Nuggets': 'DEN',
        'Detroit Pistons': 'DET',
        'Golden State Warriors': 'GSW',
        'Houston Rockets': 'HOU',
        'Indiana Pacers': 'IND',
        'Los Angeles Clippers': 'LAC',
        'Los Angeles Lakers': 'LAL',
        'Memphis Grizzlies': 'MEM',
        'Miami Heat': 'MIA',
        'Milwaukee Bucks': 'MIL',
        'Minnesota Timberwolves': 'MIN',
        'New Orleans Pelicans': 'NOP',
        'New York Knicks': 'NYK',
        'Oklahoma City Thunder': 'OKC',
        'Orlando Magic': 'ORL',
        'Philadelphia 76ers': 'PHI',
        'Phoenix Suns': 'PHO',
        'Portland Trail Blazers': 'POR',
        'Sacramento Kings': 'SAC',
        'San Antonio Spurs': 'SAS',
        'Toronto Raptors': 'TOR',
        'Utah Jazz': 'UTA',
        'Washington Wizards': 'WAS',

        # Teams that were renamed or relocated (same franchise, different names)
        'Seattle SuperSonics': 'SEA',
        'New Jersey Nets': 'NJN',
        'San Diego Rockets': 'SDR',
        'San Francisco Warriors': 'SFW',
        'Washington Bullets': 'WSB',
        'New Orleans Jazz': 'NOJ',
        'New Orleans Hornets': 'NOH',
        'Vancouver Grizzlies': 'VAN',
        'Kansas City Kings': 'KCK',
        'Indianapolis Olympians': 'IND',
        'Baltimore Bullets': 'BAL',
        'Oakland Oaks': 'OAK',
        'Pittsburgh Ironmen': 'PIT',
        'Washington Capitols': 'WSC',  # Formerly Pittsburgh Ironmen, Washington Capitols
        'Los Angeles Stars': 'LAS',
        'Richmond Virginians': 'RIC',
        'Tampa Bay Rowdies': 'TBR',
        'Knoxville Blue Bulls': 'KBB',
        'Philadelphia Warriors': 'PHW',
        'Providence Steamrollers': 'PRO',
        'New York Nets': 'NYN',

        # Defunct Teams
        'Anderson Packers': 'AND',
        'Baltimore Bullets': 'BAL',
        'Chicago Stags': 'CHS',
        'Cleveland Rebels': 'CLR',
        'Detroit Falcons': 'DTF',
        'Indianapolis Olympians': 'INO',
        'Kansas City Steers': 'KCS',
        'Knoxville Blue Bulls': 'KBB',
        'Los Angeles Stars': 'LAS',
        'Memphis Tams': 'MMT',
        'Miami Floridians': 'MMF',
        'Minnesota Muskies': 'MNM',
        'New Jersey Americans': 'NJA',
        'New Orleans Buccaneers': 'NOB',
        'Oakland Oaks': 'OAK',
        'Pittsburgh Ironmen': 'PIT',
        'Providence Steamrollers': 'PRO',
        'Richmond Virginians': 'RIC',
        'San Diego Conquistadors': 'SDA',
        'Seattle SuperSonics': 'SEA',
        'St. Louis Bombers': 'STB',
        'St. Louis Hawks': 'STL',
        'Tampa Bay Rowdies': 'TBR',
        'Toronto Huskies': 'TRH',
        'Tri-Cities Blackhawks': 'TRI',


        #intial missing teams
        'Cincinnati Royals': 'CIN',
        'New Orleans/Oklahoma City Hornets': 'NOK',
        'Charlotte Bobcats': 'CHA',
        'San Diego Clippers': 'SDC',
        'Buffalo Braves': 'BUF',
        'Fort Wayne Pistons': 'FTW',
        'Chicago Packers': 'CHP',
        'Sheboygan Red Skins': 'SHE',
        'Kansas City-Omaha Kings': 'KCO',
        'Chicago Zephyrs': 'CHZ',
        'Rochester Royals': 'ROC',
        'Indianapolis Jets': 'INJ',
        'Syracuse Nationals': 'SYR',
        'Waterloo Hawks': 'WAT',
        'Capital Bullets': 'CAP',
        'Minneapolis Lakers': 'MNL',
        'Milwaukee Hawks': 'MLH'
    }
    df['abrv_team'] = df['Team'].map(team_abbr_dict)

    #speical conditions
    # Charlotte Hornets
    df.loc[(df['Team'] == 'Charlotte Hornets') & (df['season'] >= 2014), 'abrv_team'] = 'CHO'
    df.loc[(df['Team'] == 'Charlotte Hornets') & (df['season'] < 2014), 'abrv_team'] = 'CHH'
    # Baltimore Bullets
    df.loc[(df['Team'] == 'Baltimore Bullets') & (df['season'] <= 1955), 'abrv_team'] = 'BLB'
    df.loc[(df['Team'] == 'Baltimore Bullets') & (df['season'] > 1955), 'abrv_team'] = 'BAL'
    # Denver Nuggets
    df.loc[(df['Team'] == 'Denver Nuggets') & (df['season'] <= 1950), 'abrv_team'] = 'DNN'

    #FOR FINALS HISTORY DATASET
    # df['champ_abrv_team'] = df['Champion'].map(team_abbr_dict)
    # df['runnerup_abrv_team'] = df['Runner-Up'].map(team_abbr_dict)





    df.to_csv(f'retooled_{type}', index=False)
    return print(f'retooled_{type} created')

def franchise_grouping(csv_file):
    """
    SHOULD BE A ONE TIME USE FOR NOW
    groups nba franchises together
    This also gives completely new team ids..
    """
    df = pd.read_csv(csv_file)

    franchise_groups = {
        "ATL": ["TRI", "MLH", "STL", "ATL"],
        "BOS": ["BOS"],
        "BRK": ["NYN", "NJN", "BRK"],
        "CHA": ["CHH", "CHA", "CHO"],  # CHO = Bobcats renamed Hornets
        "CHI": ["CHI"],
        "CLE": ["CLE"],
        "DAL": ["DAL"],
        "DEN": ["DEN"],
        "DET": ["FTW", "DET"],
        "GSW": ["PHW", "SFW", "GSW"],
        "HOU": ["SDR", "HOU"],
        "IND": ["IND"],
        "LAC": ["BUF", "SDC", "LAC"],
        "LAL": ["MNL", "LAL"],
        "MEM": ["VAN", "MEM"],
        "MIA": ["MIA"],
        "MIL": ["MIL"],
        "MIN": ["MIN"],
        "NOP": ["NOH", "NOK", "NOP"],
        "NYK": ["NYK"],
        "OKC": ["SEA", "OKC"],
        "ORL": ["ORL"],
        "PHI": ["SYR", "PHI"],
        "PHO": ["PHO"],
        "POR": ["POR"],
        "SAC": ["ROC", "CIN", "KCK", "KCO", "SAC"],
        "SAS": ["SAS"],
        "TOR": ["TOR"],
        "UTA": ["NOJ", "UTA"],
        "WAS": ["CHP", "CHZ", "BAL", "CAP", "WSB", "WAS"]
    }

    # Create unique 12-digit franchise IDs
    franchise_id_map = {franchise: random.randint(10 ** 11, 10 ** 12 - 1) for franchise in franchise_groups}

    # Create a lookup from abrv_team to franchise_id
    abrv_to_id = {}
    for franchise, abrv_list in franchise_groups.items():
        for abrv in abrv_list:
            abrv_to_id[abrv] = franchise_id_map[franchise]

    # Map the 'abrv_team' column to franchise_id
    df['team_id'] = df['abrv_team'].map(abrv_to_id)

    #ids for remaining teams
    unique_abrv = set(df['abrv_team'].unique())
    missing_abrv = unique_abrv - set(abrv_to_id.keys())
    for abrvs in missing_abrv:
        abrv_to_id[abrvs] = random.randint(10**11, 10**12-1)
    df['team_id'] = df['abrv_team'].map(abrv_to_id)

    df.to_csv('team_key.csv', index=False)

def adding_season(folder):
    expected_col = ["Rk", "Team", "G", "MP", "FG", "FGA", "FG%", "3P", "3PA", "3P%", '2P', "2PA", "2P%", "FT",
                    "FTA", "FT%", "ORB", "DRB", "TRB", "AST", "STL", "BLK", "TOV", "PF", "PTS", "season"
                    ]

    csv_files = glob.glob(os.path.join(folder, "*.csv"))
    df_list = []

    for file in csv_files:
        df = pd.read_csv(file)

        # fills the season column based on csv name
        season = os.path.basename(file).split("_")[-1].replace(".csv", "")
        df['season'] = int(season)

        # if col does not exist, it fills with NA
        for col in expected_col:
            if col not in df.columns:
                df[col] = pd.NA

        df = df[expected_col]  # makes sure df matches with expected df
        df_list.append(df)  # fills df_list with csv data

    final_df = pd.concat(df_list, ignore_index=True)
    final_df.to_csv(os.path.join(folder, f"{folder}.csv"), index=False)
    print(f"uploaded")

def db_to_csv(db_query):
    """
    converts db dataset to csv file
    """
    query = QUERIES.get(db_query, None)
    df = pd.read_sql(query, engine)
    df.to_csv(f'{db_query}.csv', index=False)

def seperating_team_records(data,csv_name):
    """
    SEPERATES WIN-LOSS FORMAT INTO ITS OWN COLUMNS
    """
    query = QUERIES.get(data, None)
    df = pd.read_sql(data, engine)

    #splits the records into win and loss columns
    df[['Overall_W','Overall_L']] = df['Overall'].str.split("-",expand = True)
    df[['Home_W','Home_L']] = df['Home'].str.split("-", expand = True)
    df[['Road_W','Road_L']] = df['Road'].str.split("-",expand = True)
    df[['East_W','East_L']] = df['E'].str.split("-", expand = True)
    df[['West_W','West_L']] = df['W'].str.split("-",expand = True)
    df[['Atlantic_W','Atlantic_L']] = df['A'].str.split("-", expand = True)
    df[['Central_W','Central_L']] = df['C'].str.split("-",expand = True)
    df[['Southeast_W','Southeast_L']] = df['SE'].str.split("-", expand = True)
    df[['Northwest_W','Northwest_L']] = df['NW'].str.split("-",expand = True)
    df[['Pacific_W','Pacific_L']] = df['P'].str.split("-", expand = True)
    df[['Southwest_W','Southwest_L']] = df['SW'].str.split("-",expand = True)
    df[['Pre_W','Pre_L']] = df['Pre'].str.split("-", expand = True)
    df[['Post_W','Post_L']] = df['Post'].str.split("-",expand = True)
    df[['≤3_W','≤3_L']] = df['≤3'].str.split("-", expand = True)
    df[['≥10_W','≥10_L']] = df['≥10'].str.split("-",expand = True)
    df[['Oct_W','Oct_L']] = df['Oct'].str.split("-",expand = True)
    df[['Nov_W','Nov_L']] = df['Nov'].str.split("-", expand = True)
    df[['Dec_W','Dec_L']] = df['Dec'].str.split("-",expand = True)
    df[['Jan_W','Jan_L']] = df['Jan'].str.split("-", expand = True)
    df[['Feb_W','Feb_L']] = df['Feb'].str.split("-",expand = True)
    df[['Mar_W','Mar_L']] = df['Mar'].str.split("-", expand = True)
    df[['Apr_W','Apr_L']] = df['Apr'].str.split("-", expand = True)

    #convert new columns to int
    for col in df.columns:
        if col.endswith('_W') or col.endswith('_L'):
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)

    df.to_csv(f'{csv_name}.csv', index=False)

def award_check_and_count(key,query,award_name,type):
    """
    checks if the award was won in their career
    returns a True or False if that won it and continues being True after the season award is won
    """
    if type == 'sql':
        query = QUERIES.get(query, None)
        df = pd.read_sql(query, engine)
    elif type == 'csv':
        df = pd.read_csv(query)
    else:
        raise ValueError('sql or csv only')

    query_2 = QUERIES.get(key, None)
    key_df = pd.read_sql(query_2, engine)

    #rename columns in key
    key_df = key_df.rename(columns={
        'season': 'award_season',
        'player_id': 'key_player_id'
    })
    key_df = key_df.drop_duplicates(subset=['Player', 'award_season'])
    key_df = key_df.sort_values('award_season').drop_duplicates('Player') #only check once per player

    #merge the two df
    df.columns = df.columns.str.strip()
    key_df.columns = key_df.columns.str.strip()
    df = df.drop_duplicates()

    df = df.merge(key_df, on= 'Player', how = 'left')

    #check if player won the award
    df[f'won {award_name}'] = ((df['season'] > df['award_season'])
                               & df['award_season'].notna()
                               & (df['player_id'] == df['key_player_id']))

    #clean data
    df.drop(columns=['award_season', 'key_player_id'], inplace=True)
    df.drop_duplicates(inplace=True)



    df.to_csv(f'test_award_adjusted',index=False)
    print("test_award_adjusted created")

def drop_dupes(csv_file):
    """
    drops dupes in csv files
    """
    df = pd.read_csv(csv_file)
    before = len(df)
    df.drop_duplicates(inplace=True)
    after = len(df)
    df.to_csv(f'award_adjusted_df', index=False)
    print(f"Duplicates dropped: {before - after}")

def award_season_checks_and_count(key,query,award_name,type):
    """
    checks if they won a certain award this season
    returns a True and False for  award won for that season
    and count of how many times award was received overall and before current season

    """
    if type == 'sql':
        query = QUERIES.get(query, None)
        df = pd.read_sql(query, engine)
    elif type == 'csv':
        df = pd.read_csv(query)
    else:
        raise ValueError('sql or csv only')

    query_2 = QUERIES.get(key, None)
    key_df = pd.read_sql(query_2, engine)

    # rename columns in key
    key_df = key_df.rename(columns={
        'season': 'award_season',
        'player_id': 'key_player_id'
    })
    key_df = key_df.drop_duplicates(subset=['Player', 'award_season', 'key_player_id'])

    #clean and merge the two df
    df.columns = df.columns.str.strip()
    key_df.columns = key_df.columns.str.strip()
    df = df.drop_duplicates()
    df = df.merge(key_df, on= ['Player'], how = 'left')

    #check if player won the award
    df[f'this_season_{award_name}'] = ((df['season'] == df['award_season'])
                               & (df['player_id'] == df['key_player_id']))

    #for each player + season, keep only 1 row, if the row is true, keep it and delete false , otherwise keep false
    df.sort_values(by=f'this_season_{award_name}', ascending=False, inplace=True)  #sort to make true comes first
    df = df.drop_duplicates(subset=['Player', 'season'], keep='first')


    #clean data
    df.drop(columns=['award_season', 'key_player_id'], inplace=True)
    df.drop_duplicates(inplace=True)
    df = df.sort_values(by='season', ascending=True)


    df[f'num_{award_name}_selections_before'] = df.groupby('Player')[f'this_season_{award_name}'].cumsum() - df[
        f'this_season_{award_name}']

    df[f'num_{award_name}_selections_overall'] = df.groupby('Player')[f'this_season_{award_name}'].cumsum()

    df.to_csv(f'{award_name}_count',index=False)
    print(f'{award_name}_count created')

def data_cleaning(data,type):
    """
    removes *, numbers and () from a column

    """
    if type == 'sql':
        query = QUERIES.get(data, None)
        df = pd.read_sql(query, engine)
    elif type == 'csv':
        df = pd.read_csv(data)
    else:
        raise ValueError('sql or csv only')

    #removes *, numbers and () from a column
    # df['Team'] = df['Team'].str.replace(r'[\d\*\(\)]', '',regex=True)
    # removes *
    # df['Team'] = df['Team'].str.replace('*', '', regex=False)
    #removes ,,,,,,,,

    df['Team'] = df['Team'].str.strip()
    df.to_csv(f'cleaned_df',index=False)
    print('cleaned df saved')

def team_award_cleaner(csv):
    '''
    automates the process of getting all nba teams such as rookie, def, etc to database
    '''
    with open(csv, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    name_fix = {
        "Nikola JokiÄ" : "Nikola Jokić",
        "Luka DonÄiÄ" : "Luka Dončić",
        "Goran DragiÄ" : "Goran Dragić",
        "Peja StojakoviÄ" : "Peja Stojaković",
        "DraÅ¾en PetroviÄ" : "Dražen Petrović"
    }


    # Remove lines that only contain commas (and whitespace/newlines)
    cleaned_lines = []
    for line in lines:
        #fix names
        for bad_name, good_name in name_fix.items():
            line = line.replace(bad_name, good_name)
        # remove trailing position before a comma
        line = re.sub(r'\s*\b([CFG](?:-[CFG])*)\b(?=,|\n|$)', '', line)
        # fix season string
        line = re.sub(r'(\d{4})-(\d{2})', lambda m: str(int(m.group(1)[:2] + m.group(2))), line)
        line = line.replace("1900", "2000") #scuffed fix but it works

        if re.fullmatch(r'\s*,+\s*\n?', line):
            continue  # skip lines with only commas
        cleaned_lines.append(line)


    with open(f'cleaned_{csv}', 'w', encoding='utf-8') as f:
        f.writelines(cleaned_lines)

    print(f"Cleaned file saved as: cleaned_{csv}")

def championship_count(data,type):
    if type == 'sql':
        query = QUERIES.get(data, None)
        df = pd.read_sql(query, engine)
    elif type == 'csv':
        df = pd.read_csv(data)
    else:
        raise ValueError('sql or csv only')

    query_2 = QUERIES.get("KEY_CHAMPIONSHIP", None)
    key_df = pd.read_sql(query_2, engine)


    #clean and merge the two df
    df.columns = df.columns.str.strip()
    key_df.columns = key_df.columns.str.strip()
    df = df.drop_duplicates()
    # df = df.merge(key_df, left_on='Team', right_on='Champion', how='left')
    df = df.merge(key_df, left_on='Player', right_on='Finals MVP', how='left')
    df['season'] = df['season'].astype('Int64')  # Nullable integer dtype
    df['Year'] = df['Year'].astype('Int64')

    #check if player won the award
    df[f'this_season_champion'] = ((df['Champion'] == df['Team'])
                               & (df['season'] == df['Year'])
                                   )

    df[f'this_season_runner_up'] = ((df['Runner-Up'] == df['Team'])
                               & (df['season'] == df['Year'])
                                    )

    df[f'this_season_finals_mvp'] = ((df['Champion'] == df['Team'])
                               & (df['season'] == df['Year'])
                              & (df['Finals MVP'] == df['Player'])
                                     )
    print(df['Team'].unique())


    #for each player + season, keep only 1 row, if the row is true, keep it and delete false , otherwise keep false
    df.sort_values(by=f'this_season_champion', ascending=False, inplace=True)  #sort to make true comes first
    df = df.drop_duplicates(subset=['Player', 'season'], keep='first')

    # clean data
    df.drop_duplicates(inplace=True)
    df = df.sort_values(by='season', ascending=True)


    df[f'num_finals_win_before'] = df.groupby('Player')[f'this_season_champion'].cumsum() - df[
        f'this_season_champion']

    df[f'num_finals_win_overall'] = df.groupby('Player')[f'this_season_champion'].cumsum()

    df[f'num_finals_mvp_before'] = df.groupby('Player')[f'this_season_finals_mvp'].cumsum() - df[
        f'this_season_finals_mvp']

    df[f'num_finals_mvp_overall'] = df.groupby('Player')[f'this_season_finals_mvp'].cumsum()

    df[f'num_runner_up_before'] = df.groupby('Player')[f'this_season_runner_up'].cumsum() - df[
        f'this_season_runner_up']

    df[f'num_runner_up_overall'] = df.groupby('Player')[f'this_season_runner_up'].cumsum()

    df.to_csv(f'test',index=False)
    print("test created")






if __name__ == "__main__":
    # removing_rows('roy.csv')
    # awards_season_retool('roy.csv')
    # team_retool( "cleaned_finals_history.csv",'csv')
    # remove_col('team_id_test')
    # franchise_grouping('retooled_og_team.csv')
    # data_cleaning('og_all_defense','csv')
    # seperating_team_records('expanded_standings','expanded_standings')
    # award_check("KEY_MVP",'test_award_adjusted','MVP','csv')
    # drop_dupes('per_percentile')
    # award_season_checks_and_count("KEY_ROY","test",'ROY','csv')
    # db_to_csv("TEMP")
    # team_award_cleaner('all_defense')
    # championship_count("TEMP","sql")




