#generates plotly graphs
import plotly.express as px

def player_stats_scatter(df,stat, age, mp):
    # filtered df
    filter_df = df[(df['Age'] == age) & (df['MP'] >= mp)]

    fig = px.scatter(
        filter_df,
        x='season',
        y=stat,
        hover_name='Player',
        title=f' Season Average {stat} by Player Age {age} with at least {mp} Minutes Played',
        color_discrete_sequence=['#FF5733']
    )
    fig.update_layout(template="plotly_dark")  # optional darkmode
    return fig


def overall_avg_bar(df,stat,age,mp):
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