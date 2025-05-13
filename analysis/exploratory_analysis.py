import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
from config import DB_CONFIG
import plotly.express as px
import plotly.graph_objects as go

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

QUERIES ={
'SEASON_AVG' :
    """
    select * from final.season_avg_2025
    """,

}
def season_avg_query(stat):
    """
    THIS FUNCTION GIVES THE STAT AVG OF THE WHOLE LEAGUE FOR THE SELECT CAT PER SEASON
    CONNECTED TO SEASON AVG FUNCTION

    PTS: points per game
    AST: assist per game
    TRB: total rebounds per game
    STL: steal per game
    BLK: block per game
    TOV: turnover per game
    """
    return f"""
    select avg("{stat}") as "Average {stat}",season, count("Player") as "Player Count"
    from final.season_avg_2025
    group by season
    order by season
"""

def top_stat_query (stat):
    """
    THIS FUNCTION GIVES THE STAT LEADER OF THE SELECT CAT PER SEASON
    CONNECTED TO STAT_LEADER FUNCTION

    PTS: points per game
    AST: assist per game
    TRB: total rebounds per game
    STL: steal per game
    BLK: block per game
    TOV: turnover per game
    """
    return f"""
    select "Player","Team","{stat}","season",player_id,team_id
        from (
        select *,row_number() over (partition by "season" order by "{stat}" desc) as row_nums
        from final.season_avg_2025
        ) sub
    where row_nums = 1
"""

def stat_leaders(stat):
    query = top_stat_query(stat)
    if query is None:
        raise ValueError(f'Invalid {stat}')
    df = pd.read_sql(query, engine)

    #top 10 percentile and bottom 10 percentile
    top_10 = df[f'{stat}'].quantile(0.9)
    bot_10 = df[f'{stat}'].quantile(0.1)

    #splits the 3 different value groups
    def percentile_group(val):
        if val >= top_10:
            return 'Top 10%'
        elif val<= bot_10:
            return 'Bot 10%'
        else:
            return 'Middle 80%'

    #categorizes the value in the df
    df['Percentile'] = df[f'{stat}'].apply(percentile_group)

    fig = px.bar(
        df,
        x = 'season',
        y = f'{stat}',
        color = 'Percentile',
        color_discrete_map = {
            'Top 10%': '#006400', # dark green
            'Middle 80%': '#6495ED', # cornflowerblue
            'Bot 10%': '#DC143C' #Crimson
        },
        title = f'{stat} Leaders per Season',
        labels= {f'{stat}': f'{stat} Per Game'},
        hover_data = {'Player':True,'Team':True,'season': True, f'{stat}': ':.1f'}
    )
    fig.show()

def season_avg(stat):
    query = season_avg_query(stat)
    if query is None:
        raise ValueError(f'Invalid {stat}')
    df = pd.read_sql(query, engine)

    # top 10 percentile and bottom 10 percentile
    top_10 = df[f'Average {stat}'].quantile(0.9)
    bot_10 = df[f'Average {stat}'].quantile(0.1)

    # splits the 3 different value groups
    def percentile_group(val):
        if val >= top_10:
            return 'Top 10%'
        elif val <= bot_10:
            return 'Bot 10%'
        else:
            return 'Middle 80%'

    # categorizes the value in the df
    df['Percentile'] = df[f'Average {stat}'].apply(percentile_group)

    #assigning colors to bar chart
    bar_color = {
        'Top 10%': '#006400',  # dark green
        'Middle 80%': '#6495ED',  # cornflowerblue
        'Bot 10%': '#DC143C'  # crimson
    }
    bar_colors = df['Percentile'].map(bar_color)

    #initialize plot
    fig = go.Figure()

    #bar chart with stat averages
    fig.add_trace(go.Bar(
        x = df['season'],
        y = df[f'Average {stat}'],
        name = f'Average {stat}',
        marker_color = bar_colors,
        yaxis = 'y1',
        hovertext=[f"{s}<br>{v:.1f}" for s, v in zip(df['season'], df[f'Average {stat}'])],
        hoverinfo='text'
    ))

    #line chart for player count
    fig.add_trace(go.Scatter(
        x = df['season'],
        y = df['Player Count'],
        name =  'Player Count',
        mode = 'lines+markers',
        line = dict(color = 'orange', width = 2),
        yaxis = 'y2',
    ))

    #layout
    fig.update_layout(
        title = f'Average {stat} with Player Count per Season',
        xaxis = dict(title ='Season'),
        yaxis = dict(title = f'Average {stat} Per Game'),

        yaxis2 = dict(title = 'Player Count',
                      overlaying = 'y',
                      side = 'right'),
        legend = dict(x = 0.01, y = 0.99),
        template = 'plotly_white'
    )

    fig.show()



if __name__ == '__main__':
    # stat_leaders('TRB')
    # season_avg("PTS")
