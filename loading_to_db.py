from sqlalchemy import create_engine
import psycopg2
import os
import pandas as pd
import glob

user = 'postgres'
password= 'abc123'
host = 'localhost'
port = 5432
database = 'nba'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

def combining_df(folder):
    expected_col = ["Rk","Team","G","MP","FG","FGA","FG%","3P","3PA","3P%",'2P',"2PA","2P%","FT",
                    "FTA","FT%","ORB","DRB","TRB","AST","STL","BLK","TOV","PF","PTS","season"
                    ]

    csv_files = glob.glob(os.path.join(folder,"*.csv"))
    df_list=[]


    for file in csv_files:
        df = pd.read_csv(file)

        #fills the season column based on csv name
        season = os.path.basename(file).split("_")[-1].replace(".csv","")
        df['season'] = int(season)

        #if col does not exist, it fills with NA
        for col in expected_col:
            if col not in df.columns:
                df[col] = pd.NA


        df = df[expected_col] #makes sure df matches with expected df
        df_list.append(df) #fills df_list with csv data

    combined_df = pd.concat(df_list,ignore_index = True)
    print(combined_df)
    combined_df.to_sql(f"{folder}",engine,if_exists="replace",index=False)
    print(f"{folder} uploaded")

def individual_df(folder):
    csv_files = glob.glob(os.path.join(folder, "*.csv"))

    for file in csv_files:
        df = pd.read_csv(file)
        csv_name = os.path.splitext(os.path.basename(file))[0]
        df.to_sql(f'{csv_name}', con=engine, if_exists='replace', index=False)
        print(f'{csv_name} uploaded')



if __name__ == "__main__":
    combining_df("season_team_total")
    # individual_df("season_team_total")
