from sqlalchemy import create_engine
import psycopg2
import os
import pandas as pd
import glob
from config import DB_CONFIG



engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def combining_df(folder):
    expected_col = ["Rk","Team","Overall","Home","Road","E","W","A","C",
                    "SE","NW","P","SW","Pre","Post","≤3","≥10",
                    "Oct","Nov","Dec","Jan","Feb","Mar","Apr","season"
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

def individual_folder_df(folder):
    csv_files = glob.glob(os.path.join(folder, "*.csv"))

    for file in csv_files:
        df = pd.read_csv(file)
        csv_name = os.path.splitext(os.path.basename(file))[0]
        df.to_sql(f'{csv_name}', con=engine, if_exists='replace', index=False)
        print(f'{csv_name} uploaded')

def individual_df(file):
    df = pd.read_csv(f'{file}')
    csv_name = os.path.splitext(os.path.basename(file))[0]
    df.to_sql(f'{csv_name}', con=engine, if_exists='replace', index=False)
    print(f'{csv_name} uploaded')



if __name__ == "__main__":
    # combining_df("expanded_standings")
    individual_df("test")
