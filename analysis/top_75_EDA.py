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
    where "Last Year Played" is not 2025
    
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
    counting stats based on positions
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
        color = "Position Label",
        color_discrete_map=color_map
    )
    pts_fig.show()

    ast_fig = px.scatter(
        df,
        x = "Avg AST Percentile",
        y = "APG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="APG vs Avg Assist Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    ast_fig.show()

    reb_fig = px.scatter(
        df,
        x = "Avg REB Percentile",
        y = "RPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title = "RPG vs Avg REB Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    reb_fig.show()

    stl_fig = px.scatter(
        df,
        x = "Avg STL Percentile",
        y = "SPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="SPG vs Avg STL Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    stl_fig.show()

    blk_fig = px.scatter(
        df,
        x = "Avg BLK Percentile",
        y = "BPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="BPG vs Avg BLK Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    blk_fig.show()

def age(query_key):
    """
    age and playing history for players
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
    df['Retired Age Label'] = df['Retired Age'].apply(
        lambda x: 'Early 30s (30-33)' if x <= 33
        else 'Mid 30s (34-36)' if x <= 36
        else 'Late 30s (37-39)' if x <= 39
        else 'Forties (40+)'
    )

    color_map = {
        "Early 30s (30-33)": "green",
        "Mid 30s (34-36)": "yellowgreen",
        "Late 30s (37-39)": "orange",
        'Forties (40+)': "red"
    }
    df["color"] = df["Retired Age Label"].map(color_map)

    age_fig = px.scatter(
        df,
        x = "Last Year Played",
        y = "Retired Age",
        hover_data= ["Player", "First Year Played", "Positions Played"],
        title = "Retired Age vs Last Year Played",
        color = "Retired Age Label",
        color_discrete_map=color_map,
        category_orders={"Retired Age Label": [
            "Early 30s (30-33)",
            "Mid 30s (34-36)",
            "Late 30s (37-39)",
            "Forties (40+)"
        ]}
    )
    age_fig.show()

if __name__ == '__main__':
    # position_count_pie('top_75')
    # counting_stats('top_75')
    age('top_75')