import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from config import DB_CONFIG
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES = {
    'top_75': """
        SELECT * 
        FROM staging.refined_top_75
    """
}

def position_count_pie(query_key):
    if query_key not in QUERIES:
        raise ValueError(f"Invalid query key: {query_key}")

    df = pd.read_sql(QUERIES[query_key], engine)

    #position count
    positions = ["PG","SG","SF","PF","C"]
    position_counts = {
        position_counts : df["Positions Played"].str.contains(position_counts,case=False,na=False).sum()
        for position_counts in positions

    }

    # position versatility count
    position_versatility = (
                            (df["Positions Played"]
                            .str.split(",")
                            .apply(lambda x:len([pos.strip() for pos in x])))
                            .value_counts()
                            )
    # teams played on count
    team_loyalty = (
                    (df["Teams Played On"]
                    .str.split(",")
                    .apply(lambda x:len([tm.strip() for tm in x])))
                    .value_counts()
                    )


    labels = [f"{i}" for i in position_counts.keys()]
    values = list(position_counts.values())
    fig = px.pie(
        names=labels,
        values=values,
        title="Distribution of Players by Number of Positions Played"
    )
    fig.update_traces(textinfo='label+percent', textfont_size=14)
    fig.update_layout(showlegend=False)
    fig.show()

    labels = [f"{i} position" for i in position_versatility.index]  # make readable labels
    values = position_versatility.values
    fig = px.pie(
        names=labels,
        values=values,
        title="Distribution of Players by Number of Positions Played"
    )
    fig.update_traces(textinfo='label+percent', textfont_size=14)
    fig.update_layout(showlegend=False)
    fig.show()

    labels = [f"{i} Team" for i in team_loyalty.index]  # make readable labels
    values = team_loyalty.values
    fig = px.pie(
        names=labels,
        values=values,
        title="Distribution of Players by Number of Teams Played On"
    )
    fig.update_traces(textinfo='label+percent', textfont_size=14)
    fig.update_layout(showlegend=False)
    fig.show()



if __name__ == '__main__':
    position_count_pie('top_75')