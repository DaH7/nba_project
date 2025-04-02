import pandas as pd
import numpy as np
from dask.array.stats import kurtosis
from networkx.algorithms.bipartite import color
from param.ipython import green
from pygments.lexer import combined
from scipy.stats import norm
import matplotlib.pyplot as plt
from tabulate import tabulate
import os
import mplcursors

# exp_stand_df = pd.read_csv('expanded_standings_2024.csv')
# player_season_df = pd.read_csv('player_season_stat/nba_season_2024_per game.csv')

# new_player_season_df = player_season_df[player_season_df.MP >5]
# print(tabulate(new_player_season_df, headers='keys', tablefmt='grid'))


def cumulative_stat(directory,stat):
    root_dir = directory
    combined_df = pd.DataFrame()
    for file in os.listdir(root_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(root_dir, file)
            year = int(''.join(filter(str.isdigit,file))) #grabs year from csv name
            player_season_df = pd.read_csv(file_path)
            new_player_season_df = player_season_df[player_season_df.MP > 5] # players who averages more than 5 mins a game
            avg = new_player_season_df.loc[:, "PTS"]
            temp_df = pd.DataFrame({
                'Year': [year],
                'Mean': [np.mean(avg)],
                'Median': [np.median(avg)],
                'Max': [np.max(avg)],
                '25th percentile': [np.percentile(avg,25)],
                '50th percentile': [np.percentile(avg, 50)],
                '75th percentile': [np.percentile(avg, 75)],
                'Sample Size': [len(avg)]
            })
            combined_df = pd.concat([combined_df,temp_df.round(2)],ignore_index = True)
    return combined_df

def line_plot(x,y,title,x_axis_name,y_axis_name):
    plt.plot(x,y,color = 'red')
    scatter = plt.scatter(x,y,color = 'blue')
    mplcursors.cursor(scatter, hover = True).connect(
        "add",lambda sel: sel.annotation.set_text(f"({sel.target[0]:.0f}, {sel.target[1]:.2f})"))
    plt.xlabel(x_axis_name)
    plt.ylabel(y_axis_name)
    plt.title(title)
    plt.show()

def norm_dist(stat,name):
    x = np.linspace(stat.min(), stat.max(), 30) #spacing in distribution
    p = norm.pdf(x, np.mean(stat), np.std(stat))
    kurt = stat.kurtosis()

    plt.hist(stat,density = True, bins=30, alpha=0.5)
    plt.plot(x, p)
    plt.axvline(np.mean(stat), color='red', label = 'mean')
    plt.axvline(np.median(stat), color='green', label = 'median')
    plt.xlabel(f'{name}')
    plt.ylabel('Probability  Density')
    plt.title(f'Normal Distribution - {name}, Kurtosis - {kurt}')
    plt.legend()
    plt.show()


# norm_dist(avg_pts_2024,'Avg_Pts_2024')
# print(cumulative_stat('player_season_stat','PTS')['Year'])
print(tabulate(cumulative_stat('player_season_stat','PTS'), headers='keys', tablefmt='grid'))
line_plot(cumulative_stat('player_season_stat','PTS')['Year'],
          cumulative_stat('player_season_stat','PTS')['75th percentile'],
          'avg pts vs year',
          'year',
          'pts')