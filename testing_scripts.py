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

user = 'postgres'
password= 'abc123'
host = 'localhost'
port = 5432
database = 'nba'

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
        """

}



engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
query = QUERIES.get("SEASON_TEAM_TOTAL",None)
df = pd.read_sql(query,engine)
df.to_csv('SEASON_TEAM_TOTAL.csv',index = false)

# engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
# df = pd.read_csv('season_team_total/team_key.csv')
# df.to_sql('team_key.csv', con=engine, if_exists='replace', index=False)


