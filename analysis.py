import pandas as pd
import numpy as np
from dask.array.stats import kurtosis
from networkx.algorithms.bipartite import color
from param.ipython import green
from pygments.lexer import combined
from scipy.stats import norm,stats,f_oneway,shapiro
import matplotlib.pyplot as plt
from tabulate import tabulate
import os
import mplcursors


def cumulative_stat_team(directory,stat):
    root_dir = directory
    combined_df = pd.DataFrame()
    for file in os.listdir(root_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(root_dir, file)
            year = int(''.join(filter(str.isdigit,file))) #grabs year from csv name
            new_player_season_df = pd.read_csv(file_path)
            if stat not in new_player_season_df: #checks if stat exist
                avg = 0
            else:
                avg = new_player_season_df.loc[:, stat]

            temp_df = pd.DataFrame({
                'Year': [year],
                'Total': [np.sum(avg)],
                'Mean': [np.mean(avg)],
                'Median': [np.median(avg)],
                'Standard Deviation': [np.std(avg)],
                'Max': [np.max(avg)],
                '25th percentile': [np.percentile(avg,25)],
                '75th percentile': [np.percentile(avg, 75)],
                '90th percentile': [np.percentile(avg, 90)],
                '95th percentile': [np.percentile(avg, 95)],
                'Sample Size': [len(avg) if avg is not None and not isinstance(avg, int) else 0] #if there is no data, len is zero
            })
            combined_df = pd.concat([combined_df,temp_df.round(2)],ignore_index = True)
    return combined_df

def cumulative_stat_player(directory,stat):
    root_dir = directory
    combined_df = pd.DataFrame()
    for file in os.listdir(root_dir):
        if file.endswith(".csv"):
            file_path = os.path.join(root_dir, file)
            year = int(''.join(filter(str.isdigit,file))) #grabs year from csv name
            player_season_df = pd.read_csv(file_path)
            new_player_season_df = player_season_df[player_season_df.MP > 5] # players who averages more than 5 mins a game
            if stat not in new_player_season_df: #checks if stat exist
                avg = 0
            else:
                avg = new_player_season_df.loc[:, stat]

            temp_df = pd.DataFrame({
                'Year': [year],
                'Total': [np.sum(avg)],
                'Mean': [np.mean(avg)],
                'Median': [np.median(avg)],
                'Standard Deviation': [np.std(avg)],
                'Max': [np.max(avg)],
                '25th percentile': [np.percentile(avg,25)],
                '75th percentile': [np.percentile(avg, 75)],
                '90th percentile': [np.percentile(avg, 90)],
                '95th percentile': [np.percentile(avg, 95)],
                'Sample Size': [len(avg) if avg is not None and not isinstance(avg, int) else 0] #if there is no data, len is zero
            })
            combined_df = pd.concat([combined_df,temp_df.round(2)],ignore_index = True)
    return combined_df

def line_plot(x,y,title,x_axis_label,y_axis_label):
    plt.figure(figsize=(10, 5))
    plt.plot(x,y,color = 'red')
    scatter = plt.scatter(x,y,color = 'blue')
    mplcursors.cursor(scatter, hover = True).connect(
        "add",lambda sel: sel.annotation.set_text(f"({sel.target[0]:.0f}, {sel.target[1]:.2f})"))
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.title(title)
    plt.show()

def multi_line_plot(x1,y1,y1_label,x2,y2,y2_label,x_axis_label,y_axis_label,title):
    plt.figure(figsize=(10, 5))
    plt.plot(x1,y1,marker = ',', label = y1_label)
    plt.plot(x2, y2,  marker = 'v', label = y2_label)

    scatter1 = plt.scatter(x1,y1,color = 'blue')
    scatter2 = plt.scatter(x2, y2, color='orange')

    mplcursors.cursor(scatter1, hover = True).connect(
        "add",lambda sel: sel.annotation.set_text(f"({sel.target[0]:.0f}, {sel.target[1]:.2f})"))
    mplcursors.cursor(scatter2, hover = True).connect(
        "add",lambda sel: sel.annotation.set_text(f"({sel.target[0]:.0f}, {sel.target[1]:.2f})"))

    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.legend()
    plt.title(title)
    plt.show()

def norm_dist(stat,name):

    count, bins, _ = plt.hist(stat,density = True, bins=30, alpha=0.5) #histogram


    x = np.linspace(stat.min(), stat.max(), 1000) #spacing in distribution
    p = norm.pdf(x, np.mean(stat), np.std(stat))
    kurt = stat.kurtosis()


    plt.plot(x, p)
    plt.axvline(np.mean(stat), color='red', label = f'mean: {np.mean(stat):.2f}')
    plt.axvline(np.median(stat), color='green', label = f'median: {np.median(stat):.2f}')
    stat_w, p_value = shapiro(stat) # test for normality
    plt.xlabel(f'{name}')
    plt.ylabel('Probability  Density')
    plt.title(f'Normal Distribution - {name} \nKurtosis - {kurt:.2f} \n"Shapiro-Wilk p-value:{p_value:.2f}')
    plt.legend()
    plt.show()

def one_way_anova(year,stat_avg):
    year = list(year)
    stat_avg = list(stat_avg)

    grouped_df = pd.DataFrame({
        'Year': year,
        'Avg': stat_avg

    })

    grouped_data = [group["Avg"].tolist() for _, group in grouped_df.groupby("Year")]

    f_stat, p_value = f_oneway(*grouped_data)

    return f_stat, p_value

def temp(year,stat_avg):
    year = list(year)
    stat_avg = list(stat_avg)

    grouped_df = pd.DataFrame({
        'Year':year,
        'Avg':stat_avg

    })

    return grouped_df

if __name__ == "__main__":
    # exp_stand_df = pd.read_csv('expanded_standings_2024.csv')
    # player_season_df = pd.read_csv('player_season_stat_total/total_player_season_stat_1995.csv')
    # new_player_season_df = player_season_df[player_season_df.MP >0].head()
    # print(tabulate(new_player_season_df, headers='keys', tablefmt='grid'))
    # norm_dist(player_season_df['Age'],'avg points in 1998')

    # multi_line_plot(cumulative_stat('player_season_stat_total','PT')['Year'],
    #                 cumulative_stat('player_season_stat_total','PT')['Total'],
    #                 "2P",
    #                 cumulative_stat('player_season_stat_total', 'PT')['Year'],
    #                 cumulative_stat('player_season_stat_total', 'PT')['Total'],
    #                 "3P",
    #                 "Years",
    #                 "Total",
    #                 "Comparing 2P and 3P by Years"
    #                 )

    # print(tabulate(cumulative_stat_team('season_team_stat_total','PTS'), headers='keys', tablefmt='grid'))
    line_plot(cumulative_stat_team('player_season_stat','Age')['Year'],
              cumulative_stat_team('player_season_stat','Age')['Mean'],
              'Total Points over Years',
              'Year',
              'Total Points')

