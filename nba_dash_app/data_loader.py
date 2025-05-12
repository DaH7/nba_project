#connects to db and run query
from sqlalchemy import create_engine
import pandas as pd
from config import DB_CONFIG

QUERIES = {
    "player_stats": """
        SELECT "Player", "PTS", "TRB", "AST", "STL", "BLK", "TOV", "MP", "Age", "season"
        FROM staging.player_season_stat_avg     
    """

}

def load_data(query_key):
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    query = QUERIES.get(query_key,None)

    if query is not None:
        return pd.read_sql(query,engine)
    else:
        raise ValueError(f"Query Key '{query_key}' not found")

