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
    """,
    'test': """
    SELECT * 
    FROM staging.refined_top_75
    
"""
}

def position_count_pie(query_key):
    """
    pie charts for qualitative data of positions,position versitiliy and team loyalty
    """
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

def counting_stats(query_key):
    """
    pie charts for qualitative data of positions,position versitiliy and team loyalty
    """
    if query_key not in QUERIES:
        raise ValueError(f"Invalid query key: {query_key}")

    df = pd.read_sql(QUERIES[query_key], engine)

    pos_map = {
        'PG': 'G',
        'SG': 'G',
        'SF': 'F',
        'PF': 'F',
        'C': 'C'
    }
    df['Position Label'] = df['Positions Played'].apply(
        lambda x: 'Forward' if any(pos in x.split(', ') for pos in ['SF','PF'])
        else 'Guard' if any(pos in x.split(', ') for pos in ['PG','SG'])
        else 'Center'
    )

    color_map = {
        "Forward": "red",
        "Guard": "green",
        "Center": "blue"
    }
    df["color"] = df["Position Label"].map(color_map)

    pts_fig = px.scatter(
        df,
        x = "Avg PTS Percentile",
        y = "PPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title = "PPG vs Avg Points Percentile",
        color = "Position Label"
    )
    pts_fig.show()

    ast_fig = px.scatter(
        df,
        x = "Avg AST Percentile",
        y = "APG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="APG vs Avg Assist Percentile",
        color = "Position Label"
    )
    ast_fig.show()

    reb_fig = px.scatter(
        df,
        x = "Avg REB Percentile",
        y = "RPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title = "RPG vs Avg REB Percentile",
        color = "Position Label"
    )
    reb_fig.show()

    stl_fig = px.scatter(
        df,
        x = "Avg STL Percentile",
        y = "SPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="SPG vs Avg STL Percentile",
        color = "Position Label"
    )
    stl_fig.show()

    blk_fig = px.scatter(
        df,
        x = "Avg BLK Percentile",
        y = "BPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="BPG vs Avg BLK Percentile",
        color = "Position Label"
    )
    blk_fig.show()


if __name__ == '__main__':
    # position_count_pie('top_75')
    counting_stats('top_75')