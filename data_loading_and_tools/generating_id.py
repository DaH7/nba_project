import pandas as pd
import random
from sqlalchemy import create_engine
from config import DB_CONFIG



engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

query  = """
select *
from undrafted_players
"""
df = pd.read_sql(query,engine)
# print(df)

def generate_player_id(length):
    return ''.join([str(random.randint(0,9)) for _ in range(length)])

player_ids = []
for index,row in df.iterrows():
    random_digits = generate_player_id(8)
    player_id = f"{9000}{random_digits}"
    player_ids.append(player_id)

df['player_id'] = player_ids
df.to_csv('undrafted_player_key', index=False)

# #to db
# df = pd.read_csv('undrafted_player_key')
# df.to_sql('undrafted_player_key', engine, if_exists='replace', index=False)

# def generating_team_id():
