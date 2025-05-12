#register all interactivity in app (sliders,dropdowns,etc)
from dash import Input, Output
from figures import player_stats_scatter, overall_avg_bar
from data_loader import load_data

def register_callbacks(app):

    @app.callback(
        Output('scatter-plot-s1', 'figure'),
        Input('stat-dropdown-s1', 'value'),
        Input('age-slider-s1', 'value'),
        Input('mp-slider-s1', 'value')
    )

    def update_player_stats_scatter(stat,age,mp):
        df = load_data('player_stats')
        return player_stats_scatter(df,stat,age,mp)

    @app.callback(
        Output('bar-plot-b1', 'figure'),
        Input('stat-dropdown-b1', 'value'),
        Input('age-slider-b1', 'value'),
        Input('mp-slider-b1', 'value')
    )

    def update_overall_avg_bar(stat,age,mp):
        df = load_data('player_stats')
        return overall_avg_bar(df,stat,age,mp)
