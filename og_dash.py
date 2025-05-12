from sqlalchemy import create_engine
import pandas as pd
import plotly.express as px
import dash
import dash_bootstrap_components  as dbc
from dash import dcc,html
from dash.dependencies import Input,Output



user = 'postgres'
password= 'abc123'
host = 'localhost'
port = 5432
database = 'nba'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

query  = """
select "Player","PTS", "TRB", "AST", "STL", "BLK", "TOV", "MP", "Age", "season"
from final.player_season_stat_avg
"""
df = pd.read_sql(query,engine)

stat_options = [
    {'label': 'Points Per Game', 'value': 'PTS'},
    {'label': 'Rebounds Per Game', 'value': 'TRB'},
    {'label': 'Assists Per Game', 'value': 'AST'},
    {'label': 'Steals Per Game', 'value': 'STL'},
    {'label': 'Blocks Per Game', 'value': 'BLK'},
    {'label': 'Turnovers Per Game', 'value': 'TOV'}
]

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.DARKLY])
app.layout = html.Div([
    html.H1("Nba Analytics", className= "text-center my-4"),

    #scatter plot
    dbc.Row([
        dbc.Col(dcc.Graph(id = 'scatter-plot-s1'),
                width = 8), #graph to the left, 8/12 of row

        dbc.Col([
            html.Label("Stat",className="text-light"),
            dcc.Dropdown(
                id = 'stat-dropdown-s1',
                options = [{'label': 'Points Per Game', 'value': 'PTS'},
                {'label': 'Rebounds Per Game', 'value': 'TRB'},
                {'label': 'Assists Per Game', 'value': 'AST'},
                {'label': 'Steals Per Game', 'value': 'STL'},
                {'label': 'Blocks Per Game', 'value': 'BLK'},
                {'label': 'Turnovers Per Game', 'value': 'TOV'}],
                value = 'PTS', #default,
                clearable=False,
                style={
                    'backgroundColor': '#f0f0f0',
                    'color': '#ffffff',
                    'border': '1px solid #555',
                    'fontWeight': '600'
                }

            ),
            html.Label("Age",className="text-light"),
            dcc.Slider(
                id = 'age-slider-s1',
                min = 18,
                max = 44,
                step = 1,
                value = 25, #default age
                marks = {i: str(i) for i in range(18,45)},
                tooltip ={"placement": "bottom","always_visible": True}
            ),

            html.Br(),
            html.Label("Minutes Played",className="text-light"),
            dcc.Slider(
                id='mp-slider-s1',
                min=0,
                max=48,
                step=1,
                value=10,  # default age
                marks = {i: str(i) for i in range(0,49,5)},
                tooltip={"placement": "bottom", "always_visible": True}


            )
        ], width=4) #sliders to the right, 4/12 of row
    ]),

    html.Br(),
    #barplot
    dbc.Row([
        dbc.Col(dcc.Graph(id = 'bar-plot-b1'),
                width = 8),
        dbc.Col([
            html.Label("Stat",className="text-light"),
            dcc.Dropdown(
                id = 'stat-dropdown-b1',
                options = [{'label': 'Points Per Game', 'value': 'PTS'},
                {'label': 'Rebounds Per Game', 'value': 'TRB'},
                {'label': 'Assists Per Game', 'value': 'AST'},
                {'label': 'Steals Per Game', 'value': 'STL'},
                {'label': 'Blocks Per Game', 'value': 'BLK'},
                {'label': 'Turnovers Per Game', 'value': 'TOV'}],
                value = 'PTS', #default,
                clearable=False,
                style={
                    'backgroundColor': '#f0f0f0',
                    'color': '#ffffff',
                    'border': '1px solid #555',
                    'fontWeight': '600'
                }

            ),
            html.Label("Age",className="text-light"),
            dcc.Slider(
                id = 'age-slider-b1',
                min = 18,
                max = 44,
                step = 1,
                value = 25, #default age
                marks = {i: str(i) for i in range(18,45)},
                tooltip ={"placement": "bottom","always_visible": True}
            ),

            html.Br(),
            html.Label("Minutes Played",className="text-light"),
            dcc.Slider(
                id='mp-slider-b1',
                min=0,
                max=48,
                step=1,
                value=10,  # default age
                marks = {i: str(i) for i in range(0,49,5)},
                tooltip={"placement": "bottom", "always_visible": True}


            )
        ], width=4) #sliders to the right, 4/12 of row

    ])
])

@app.callback(
    Output('scatter-plot-s1', 'figure'),
    Input('stat-dropdown-s1','value'),
    Input('age-slider-s1','value'),
    Input('mp-slider-s1','value')
)

def player_stats_scatter(stat,age,mp):
    #filtered df
    filter_df = df[(df['Age'] == age) & (df['MP']>=mp)]
    
    fig=px.scatter(
        filter_df,
        x = 'season',
        y = stat,
        hover_name = 'Player',
        title = f' Season Average {stat} by Player Age {age} with at least {mp} Minutes Played',
        color_discrete_sequence=['#FF5733']
    )
    fig.update_layout(template="plotly_dark") #optional darkmode
    return fig

@app.callback(
    Output('bar-plot-b1', 'figure'),
    Input('stat-dropdown-b1','value'),
    Input('age-slider-b1','value'),
    Input('mp-slider-b1','value')
)

def overall_avg_bar(stat,age,mp):
    filter_df = df[(df['Age'] == age) & (df['MP'] >= mp)]

    avg_df = (filter_df.groupby('season', as_index = False)[stat].mean()).round(1)

    fig = px.bar(
        avg_df,
        x='season',
        y=stat,
        title=f' Overall Season Average {stat} by Player Age {age} with at least {mp} Minutes Played',
        color_discrete_sequence=['#2ECC71']
    )
    fig.update_layout(template="plotly_dark")  # optional darkmode
    return fig


if __name__ == '__main__':
    app.run(debug=True)



