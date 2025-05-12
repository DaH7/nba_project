#builds visual layout of dash app using html/bootstrap
from dash import html, dcc
import dash_bootstrap_components as dbc

def create_layout():
    return html.Div([
        html.H1("Nba Analytics", className="text-center my-4"),

        # scatter plot
        dbc.Row([
            dbc.Col(dcc.Graph(id='scatter-plot-s1'),
                    width=8),  # graph to the left, 8/12 of row

            dbc.Col([
                html.Label("Stat", className="text-light"),
                dcc.Dropdown(
                    id='stat-dropdown-s1',
                    options=[{'label': 'Points Per Game', 'value': 'PTS'},
                             {'label': 'Rebounds Per Game', 'value': 'TRB'},
                             {'label': 'Assists Per Game', 'value': 'AST'},
                             {'label': 'Steals Per Game', 'value': 'STL'},
                             {'label': 'Blocks Per Game', 'value': 'BLK'},
                             {'label': 'Turnovers Per Game', 'value': 'TOV'}],
                    value='PTS',  # default,
                    clearable=False,
                    style={
                        'backgroundColor': '#f0f0f0',
                        'color': '#ffffff',
                        'border': '1px solid #555',
                        'fontWeight': '600'
                    }

                ),
                html.Label("Age", className="text-light"),
                dcc.Slider(
                    id='age-slider-s1',
                    min=18,
                    max=44,
                    step=1,
                    value=25,  # default age
                    marks={i: str(i) for i in range(18, 45)},
                    tooltip={"placement": "bottom", "always_visible": True}
                ),

                html.Br(),
                html.Label("Minutes Played", className="text-light"),
                dcc.Slider(
                    id='mp-slider-s1',
                    min=0,
                    max=48,
                    step=1,
                    value=10,  # default age
                    marks={i: str(i) for i in range(0, 49, 5)},
                    tooltip={"placement": "bottom", "always_visible": True}

                )
            ], width=4)  # sliders to the right, 4/12 of row
        ]),

        html.Br(),

        # barplot
        dbc.Row([
            dbc.Col(dcc.Graph(id='bar-plot-b1'),
                    width=8),
            dbc.Col([
                html.Label("Stat", className="text-light"),
                dcc.Dropdown(
                    id='stat-dropdown-b1',
                    options=[{'label': 'Points Per Game', 'value': 'PTS'},
                             {'label': 'Rebounds Per Game', 'value': 'TRB'},
                             {'label': 'Assists Per Game', 'value': 'AST'},
                             {'label': 'Steals Per Game', 'value': 'STL'},
                             {'label': 'Blocks Per Game', 'value': 'BLK'},
                             {'label': 'Turnovers Per Game', 'value': 'TOV'}],
                    value='PTS',  # default,
                    clearable=False,
                    style={
                        'backgroundColor': '#f0f0f0',
                        'color': '#ffffff',
                        'border': '1px solid #555',
                        'fontWeight': '600'
                    }

                ),
                html.Label("Age", className="text-light"),
                dcc.Slider(
                    id='age-slider-b1',
                    min=18,
                    max=44,
                    step=1,
                    value=25,  # default age
                    marks={i: str(i) for i in range(18, 45)},
                    tooltip={"placement": "bottom", "always_visible": True}
                ),

                html.Br(),
                html.Label("Minutes Played", className="text-light"),
                dcc.Slider(
                    id='mp-slider-b1',
                    min=0,
                    max=48,
                    step=1,
                    value=10,  # default age
                    marks={i: str(i) for i in range(0, 49, 5)},
                    tooltip={"placement": "bottom", "always_visible": True}

                )
            ], width=4)  # sliders to the right, 4/12 of row

        ])
    ])