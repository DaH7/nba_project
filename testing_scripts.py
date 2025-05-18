import pandas as pd
import requests
from bs4 import BeautifulSoup,Comment
from io import StringIO
import time
import re
import csv
import random
from numpy.core.defchararray import zfill
from sqlalchemy import create_engine
import plotly.express as px
import dash
import dash_bootstrap_components  as dbc
from dash import dcc,html
from dash.dependencies import Input,Output
from sympy import false
from config import DB_CONFIG

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES = {
    "SEASON_TEAM_TOTAL":
        """
        select * from final.season_team_total_2025
        """,

    "abbrv_team":
        """
        select distinct("Team") from final.key_all_player_2025 
        order by "Team" asc
        """,

    "full_team":
        """
        select distinct("Team") from staging.season_team_stat_avg
        order by "Team" asc
        """,

    "og_team":
        """
        select * from staging.season_team_stat_avg
        """,

        "missing":
        """
        SELECT
          "Team",
          "abrv_team",
          "season"
        FROM
          retooled_og_team
        -- Where 
        -- 	-- "abrv_team" = 'IND'
        -- 	"abrv_team" is null
        
        ORDER BY
          1 ASC, 3 Asc
        """,
'MAIN_DATA' :
    """
     SELECT * from staging.logr_allstar_data
    """,

}



engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
query = QUERIES.get("MAIN_DATA",None)
df = pd.read_sql(query,engine)
print(df)
# df.to_csv('SEASON_TEAM_TOTAL.csv',index = false)

# engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
# df = pd.read_csv('season_team_total/team_key.csv')
# df.to_sql('team_key.csv', con=engine, if_exists='replace', index=False)

# import plotly.express as px
# fig = px.scatter(x=[1, 2, 3], y=[4, 5, 6])
# fig.write_image("test.png", engine="orca")

