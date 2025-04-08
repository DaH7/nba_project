import pandas as pd
import requests
from bs4 import BeautifulSoup,Comment
from io import StringIO
import time


def player_avg_stat_pull(start_year,end_year,type):
    years =range(start_year,end_year + 1)
    for year in years:
        if type == 'season':
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
        elif type == 'playoff':
            url = f'https://www.basketball-reference.com/playoffs/NBA_{year}_per_game.html'

        try:
            tables = pd.read_html(url)
            df = tables[0] #first table on page

            #save to csv
            if type == 'season':
                filename = f'nba_season_{year}_per game.csv'
            elif type == 'playoff':
                filename = f'nba_playoff_{year}_per game.csv'

            df.to_csv(filename,index = False)
            df = df[:-1]
            df.to_csv(filename,index = False)

            print(f"Successfully saved {filename}")
        except Exception as e:
            print(f"Failed to get data for {year}: {e}")

        time.sleep(5)

def expanded_standings(start_year, end_year):
    years = range(start_year,end_year +1)
    for year in years:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"


        try:
            # use requests to fetch the page
            response = requests.get(url)
            response.raise_for_status()  # Raise an error if request fails
            # BeautifulSoup to remove comments
            soup = BeautifulSoup(response.text, "html.parser")
            table_html = None

            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if 'expanded_standings' in comment:
                    table_html = BeautifulSoup(comment, "html.parser").find("table", {"id": "expanded_standings"})
                    break  # stop once found

            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]  # Convert to DataFrame

                # Save to CSV
                filename = f'expanded_standings_{year}.csv'
                df.to_csv(filename, index=False)

                print(f"Successfully saved {filename}")
            else:
                print(f"Table not found for {year}")

        except Exception as e:
            print(f"Failed to get data for {year}: {e}")

        time.sleep(5)

def standings(start_year,end_year,type):
    years =range(start_year,end_year + 1)
    for year in years:
        url =  f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"

        tables = pd.read_html(url)
        if type == 'east':
            df = tables[0]  # first table on page
        elif type == 'west':
            df = tables[1]
        elif type == 'div_east':
            df = tables[2]
        elif type == 'div_west':
            df = tables[3]

        try:
            #save to csv
            filename = f'{type}_standings_{year}.csv'
            df.to_csv(filename,index = False)

            print(f"Successfully saved {filename}")
        except Exception as e:
             print(f"Failed to get data for {year}: {e}")
        time.sleep(5)

def team_stats(start_year,end_year,type):
    """
    type
    Per Game Stats: per_game-team
    Total Stats: totals-team
    Per 36 Min: per_minute-team
    Per 100 Possessions: per_poss-team
    Advanced Stats: advanced-team
    Shooting: shooting-team
    Miscellaneous: misc-team
    Opponent Per Game: opponent-per_game
    Four Factors: four_factors
    """
    years = range(start_year, end_year + 1)
    for year in years:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"

        try:
            # use requests to fetch the page
            response = requests.get(url)
            response.raise_for_status()  # Raise an error if request fails
            # BeautifulSoup to remove comments
            soup = BeautifulSoup(response.text, "html.parser")
            table_html = None

            # Look inside HTML comments (Basketball Reference hides some tables there)
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))

            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if f'{type}' in comment:
                    table_html = BeautifulSoup(comment, "html.parser").find("table", {"id": f"{type}"})
                    break  # stop once found

            if not table_html: #check normal HTML
                table_html = soup.find("table", {"id": type})

            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]  # convert to DataFrame

                # Save to CSV
                filename = f'{type}_{year}.csv'
                df.to_csv(filename, index=False)
                df = df[:-1]
                df.to_csv(filename, index=False)

                print(f"Successfully saved {filename}")
            else:
                print(f"Table not found for {year}")

        except Exception as e:
            print(f"Failed to get data for {year}: {e}")

        time.sleep(5)

def player_total_stat_pull(start_year,end_year):
    years = range(start_year, end_year + 1)
    for year in years:
        url = f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"
        tables = pd.read_html(url)
        df = tables[0]

        try:
            #save to csv
            filename = f'total_player_season_stat_{year}.csv'
            df.to_csv(filename,index = False)
            df = df[:-1]
            df.to_csv(filename, index=False)

            print(f"Successfully saved {filename}")
        except Exception as e:
             print(f"Failed to get data for {year}: {e}")
        time.sleep(5)




if __name__ == "__main__":
    # team_stats(1976,2024,"totals-team")
    # player_total_stat_pull(2006,2019)
    # player_avg_stat_pull(2001,2024,'season')
    # expanded_standings(2024, 2024)
    # standings(2024,2024, 'west')

