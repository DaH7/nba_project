import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from config import DB_CONFIG
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
import numpy as np

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
    where "Last Year Played" != 2025
    
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
        title = "PPG vs Points Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    pts_fig.show()

    ast_fig = px.scatter(
        df,
        x = "Avg AST Percentile",
        y = "APG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="APG vs Assist Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    ast_fig.show()

    reb_fig = px.scatter(
        df,
        x = "Avg REB Percentile",
        y = "RPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title = "RPG vs REB Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    reb_fig.show()

    stl_fig = px.scatter(
        df,
        x = "Avg STL Percentile",
        y = "SPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="SPG vs STL Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    stl_fig.show()

    blk_fig = px.scatter(
        df,
        x = "Avg BLK Percentile",
        y = "BPG",
        hover_data= ["Player", "First Year Played","Last Year Played", "Positions Played"],
        title="BPG vs BLK Percentile",
        color = "Position Label",
        color_discrete_map=color_map
    )
    blk_fig.show()

    # pts_fig.write_image("pts_scatter.png")
    # ast_fig.write_image("ast_scatter.png")
    # reb_fig.write_image("reb_scatter.png")
    # stl_fig.write_image("stl_scatter.png")
    # blk_fig.write_image("blk_scatter.png")

def age(query_key):
    """
    age and playing history for players
    """
    if query_key not in QUERIES:
        raise ValueError(f"Invalid query key: {query_key}")

    df = pd.read_sql(QUERIES[query_key], engine)

    def add_jitter(arr, jitter_strength=0.3):
        return arr + np.random.uniform(-jitter_strength, jitter_strength, size=len(arr))

    x = add_jitter(df["Retired Age"], jitter_strength=0.2)
    y = add_jitter(df["Last Year Played"], jitter_strength=0.2)


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
        x = y,
        y = x,
        hover_data= ["Player", "First Year Played", "Retired Age","Positions Played", ],
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

    #pie chart breakdown of retirement age
    age_breakdown = df["Retired Age Label"].value_counts().sort_index()
    labels = [f"{age} - {count}" for age, count in age_breakdown.items()]
    values = age_breakdown.values


    age_breakdown_fig = px.pie(
        names=labels,
        values=values,
        title="Distribution of Players by Retirement Age"
    )
    age_breakdown_fig.update_traces(
        textinfo='label+percent',
        textfont_size=14
    )
    age_breakdown_fig.update_layout(showlegend=False)
    age_breakdown_fig.show()

    # age_fig.write_image("retired_age_scatter.png")
    # age_breakdown_fig.write_image("retired_age_pie.png")

def rewards(query_key):
    """
    List out all achievements from players including mvps, championships, all nba teams, etc...
    (All Star Selection, Championship, Finals Mvp, Finals Lost,
    Overall NBA Team Selection, All NBA First Team Selection, All NBA Second Team Selection, All NBA Third Team Selection,
    All Defense Selection, All Defense First Team Selection, All Defense Second Team Selection,
    Overall Rookie Team Selection, All Rookie First Team Selection, All Rookie Second Team Selection)
    """

    if query_key not in QUERIES:
        raise ValueError(f"Invalid query key: {query_key}")

    df = pd.read_sql(QUERIES[query_key], engine)
    print(df.columns)

    # All NBA Graphs
    df_sorted = df.sort_values("Overall NBA Team Selection", ascending=False)
    df_top20 = df_sorted.head(20)
    df_long = df_top20.melt(
        id_vars="Player",
        value_vars=["All NBA First Team Selection", "All NBA Second Team Selection","All NBA Third Team Selection"],
        var_name="Team Type",
        value_name="Selections"
    )

    top_20_all_nba_fig = px.bar(
        df_long,
        x="Player",
        y="Selections",
        color="Team Type",
        barmode="stack",
        title="Top 20 All NBA Selections per Player"
    )

    top_20_all_nba_fig.show()
    df_bot20 = df_sorted.tail(20)
    df_long = df_bot20.melt(
        id_vars="Player",
        value_vars=["All NBA First Team Selection", "All NBA Second Team Selection","All NBA Third Team Selection"],
        var_name="Team Type",
        value_name="Selections"
    )

    bot_20_all_nba_fig = px.bar(
        df_long,
        x="Player",
        y="Selections",
        color="Team Type",
        barmode="stack",
        title="Bottom 20 All NBA Selections per Player"
    )
    bot_20_all_nba_fig.show()


    #All Defense
    df_sorted = df.sort_values("All Defense Selection", ascending=False)
    df_top20 = df_sorted.head(20)
    df_long = df_top20.melt(
        id_vars="Player",
        value_vars=["All Defense First Team Selection", "All Defense Second Team Selection"],
        var_name="Team Type",
        value_name="Selections"
    )

    top_20_all_def_fig = px.bar(
        df_long,
        x="Player",
        y="Selections",
        color="Team Type",
        barmode="stack",
        title="Top 20 All NBA Defense Selections per Player"
    )
    top_20_all_def_fig.show()

    df_sorted = df.sort_values("All Defense Selection", ascending=False)
    df_bot20 = df_sorted.tail(42)
    df_long = df_bot20.melt(
        id_vars="Player",
        value_vars=["All Defense First Team Selection", "All Defense Second Team Selection"],
        var_name="Team Type",
        value_name="Selections"
    )

    bot_42_all_def_fig = px.bar(
        df_long,
        x="Player",
        y="Selections",
        color="Team Type",
        barmode="stack",
        title="Bottom 42 All NBA Defense Selections per Player"
    )
    bot_42_all_def_fig.show()


    # Finals Appearance Graphs
    df['Finals Appearance'] = df['Championship'] + df['Finals Lost']
    df_sorted = df.sort_values("Finals Appearance", ascending=False)
    df_top20 = df_sorted.head(20)
    df_long = df_top20.melt(
        id_vars="Player",
        value_vars=["Championship", "Finals Lost"],
        var_name="Type",
        value_name="Appearances"
    )

    top_20_finals_fig = px.bar(
        df_long,
        x="Player",
        y="Appearances",
        color="Type",
        barmode="stack",
        title="Top 20 Finals Appearance per Player"
    )
    top_20_finals_fig.show()

    df_bot20 = df_sorted.tail(20)
    df_long = df_bot20.melt(
        id_vars="Player",
        value_vars=["Championship", "Finals Lost"],
        var_name="Type",
        value_name="Selections"
    )

    bot_20_finals_fig = px.bar(
        df_long,
        x="Player",
        y="Selections",
        color="Type",
        barmode="stack",
        title="Bottom 20 Finals Appearance per Player"
    )
    bot_20_finals_fig.show()

    #Individual Awards graphs ( all stars, mvp,dpoy,finals Mvp)
    df['Individual Awards'] = df['All Star Selection'] + df['MVP'] + df['DPOY'] + df['Finals MVP']
    df_sorted = df.sort_values("Individual Awards", ascending=False)
    df_top20 = df_sorted.head(20)
    df_long = df_top20.melt(
        id_vars="Player",
        value_vars=["All Star Selection", 'MVP', 'DPOY', "Finals MVP"],
        var_name="Award Type",
        value_name="Total Awards"
    )

    top_20_award_fig = px.bar(
        df_long,
        x="Player",
        y="Total Awards",
        color="Award Type",
        barmode="stack",
        title="Top 20 Individual Awards per Player"
    )
    top_20_award_fig.show()

    df_bot20 = df_sorted.tail(20)
    df_long = df_bot20.melt(
        id_vars="Player",
        value_vars=["All Star Selection", 'MVP', 'DPOY', "Finals MVP"],
        var_name="Award Type",
        value_name="Total Awards"
    )

    bot_20_award_fig = px.bar(
        df_long,
        x="Player",
        y="Total Awards",
        color="Award Type",
        barmode="stack",
        title="Bottom 20 Individual Awards per Player"
    )
    bot_20_award_fig.show()

    # top_20_all_nba_fig.write_image("top_20_all_nba_fig.png")
    # bot_20_all_nba_fig.write_image("bot_20_all_nba_fig.png")
    # top_20_all_def_fig.write_image("top_20_all_def_fig.png")
    # bot_42_all_def_fig.write_image("bot_42_all_def_fig.png")
    # top_20_finals_fig.write_image("top_20_finals_fig.png")
    # bot_20_finals_fig.write_image("bot_20_finals_fig.png")
    # top_20_award_fig.write_image("top_20_award_fig.png")
    # bot_20_award_fig.write_image("bot_20_award_fig.png")







if __name__ == '__main__':
    # position_count_pie('top_75')
    # counting_stats('top_75')
    age('top_75')
    # age('test')
    # rewards('top_75')