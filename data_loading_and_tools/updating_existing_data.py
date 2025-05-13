import pandas as pd
from sqlalchemy import create_engine
import os
import glob
from config import DB_CONFIG

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)


QUERIES = {
    "SEASON_TEAM_TOTAL":
        """
        select * from final.season_team_total_2025
        """,

    "SEASON_TEAM_AVG":
        """
        select * from final.season_team_avg_2025
        """,
}


query_to_update = QUERIES.get("SEASON_TEAM_TOTAL",None)
# df_to_update = pd.read_sql(query_to_update,engine)
df_to_update = pd.read_csv("retooled_SEASON_TEAM_TOTAL.csv")
new_df = pd.read_csv('new_total_1975.csv')


#checks columns
# print(df_to_update.columns)
# print(new_df.columns)

#strips white space
df_to_update.columns = df_to_update.columns.str.strip()
new_df.columns = new_df.columns.str.strip()

#to make sure columns lineup
df_to_update = df_to_update[["Rk","Team","G","MP","FG","FGA","FG%","3P","3PA","3P%","2P",
         "2PA","2P%","FT","FTA","FT%","ORB","DRB","TRB","AST","STL","BLK",
         "TOV","PF","PTS","season","abrv_team","playoff","team_id"]]

if not df_to_update.columns.equals(new_df.columns):
        missing_from_new_df = set(df_to_update.columns) - set(new_df.columns)
        missing_from_df_to_update = set(new_df.columns) - set(df_to_update.columns)
        print("❌ Column mismatch detected.")
        if missing_from_new_df:
            print("Missing from new_df:", missing_from_new_df)
        if missing_from_df_to_update:
            print("Missing from df_to_update:", missing_from_df_to_update)
        raise ValueError("Fix column mismatches before concatenation.")

#updates df
updated_df = pd.concat([df_to_update, new_df], ignore_index=True)

updated_df.to_csv('season_team_total.csv')
updated_df.to_sql('season_team_total', engine, if_exists='replace', index=False)

