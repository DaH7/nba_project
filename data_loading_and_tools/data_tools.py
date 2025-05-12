import os
import pandas as pd
import random
from sqlalchemy import create_engine

user = 'postgres'
password= 'abc123'
host = 'localhost'
port = 5432
database = 'nba'

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
    """
}
engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')


def removing_rows(root_dir):
    for file in os.listdir(root_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(root_dir, file) #creates path

            # df = pd.read_csv(file_path)
            df = pd.read_csv(file_path)  ##skips 1st row and uses 2nd as header
            # df = df.iloc[0:]  # drop 1st row
            # df = df.iloc[:-1]  #drops last row

            df.to_csv(file_path, index=False)  #back to csv
            print(f'{file} saved')

def remove_col(csv_file):
    df = pd.read_csv(csv_file)
    df = df.iloc[:,:-1]
    df.to_csv(csv_file, index=False)
    print(f'{csv_file} saved')

def awards_season_retool(root_dir):
    for file in os.listdir(root_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(root_dir, file) #creates path

            df = pd.read_csv(file_path)
            df['Season'] = df['Season'].astype(str).apply(lambda x: x[:2] + x[5:])
            df.iloc[:, 0] = df.iloc[:, 0].str.lower()
            df.to_csv(file_path, index=False)
            print(f'{file} saved')

def team_retool(query,type):
    '''
    sql: from db
    csv : from csv
    '''
    # add 2 new columns, playoffs and Tm (the abbrivations for team)
    if type == 'csv':
        df = pd.read_csv(query)
    elif type == 'sql':
        query = QUERIES.get(query,None)
        df = pd.read_sql(query,engine)
    else:
        raise ValueError("source_type must be 'sql' or 'csv'")

    df['abrv_team'] = ''
    df['playoff'] = ''
    # if team has * in their name, playoffs columns is yes, otherwise no
    df['playoff'] = df['Team'].str.contains(r'\*').map({True: 'yes', False: 'no'})
    # clean all * from name, make a dictionary of team names shorthand longhand
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

    df.to_csv(f'retooled_{type}', index=False)
    return print(df)

def franchise_grouping(csv_file):
    """
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
    final_df.to_csv(os.path.join(folder, f"team_total_1975.csv"), index=False)
    print(f"uploaded")

def db_to_csv(db_query):
    query = QUERIES.get(db_query, None)
    df = pd.read_sql(query, engine)
    df.to_csv(f'{db_query}.csv', index=False)

if __name__ == "__main__":
    # removing_rows('awards')
    # awards_season_retool('awards')
    # team_retool( "allstar_team",'sql')
    remove_col('../retooled_sql')
    # franchise_grouping('retooled_og_team.csv')
    # db_to_csv('SEASON_TEAM_TOTAL')