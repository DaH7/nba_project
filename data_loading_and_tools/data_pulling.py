import pandas as pd
import requests
from bs4 import BeautifulSoup,Comment
from io import StringIO
import time
import csv


def player_avg_stat_pull(start_year,end_year,type):
    years =range(start_year,end_year + 1)
    for year in years:
        if type == 'season':
            if year >= 1950:
                url = f"https://www.basketball-reference.com/leagues/NBA_{year}_per_game.html"
            else:
                url = f"https://www.basketball-reference.com/leagues/BAA_{year}_per_game.html"
        elif type == 'playoff':
            if year >= 1950:
                url = f'https://www.basketball-reference.com/playoffs/NBA_{year}_per_game.html'
            else:
                url = f'https://www.basketball-reference.com/playoffs/BAA_{year}_per_game.html'

        try:
            tables = pd.read_html(url)
            df = tables[0] #first table on page
            #save to csv
            filename = f'nba_{type}_per_game_{year}.csv'
            df = df[:-1]
            df.to_csv(filename,index = False)

            print(f"Successfully saved {filename}")
        except Exception as e:
            print(f"Failed to get data for {year}: {e}")

        time.sleep(5)

def expanded_standings(start_year, end_year):
    #uses older loading, needs to parse through comments to find html
    years = range(start_year,end_year +1)
    for year in years:
        if year >= 1950:
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        else:
            url = f"https://www.basketball-reference.com/leagues/BAA_{year}_standings.html"


        try:
            # use requests to fetch the page
            response = requests.get(url)
            response.raise_for_status()  # Raise an error if request fails
            # BeautifulSoup to remove comments
            soup = BeautifulSoup(response.text, "html.parser")
            table_html = None

            for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
                if 'expanded_season_standings' in comment:
                    table_html = BeautifulSoup(comment, "html.parser").find("table", {"id": "expanded_season_standings"})
                    break  # stop once found
                elif 'expanded_season_standings' in comment:
                    table_html = BeautifulSoup(comment, "html.parser").find("table",{"id": "expanded_season_standings"})
                    break
                elif 'expanded_standings' in comment:
                    table_html = BeautifulSoup(comment, "html.parser").find("table",{"id": "expanded_standings"})
                    break

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
        if year >= 1950:
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}.html"
        else:
            url = f"https://www.basketball-reference.com/leagues/BAA_{year}.html"

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

def player_total_stat_pull(start_year,end_year,type):
    years = range(start_year, end_year + 1)
    for year in years:
        if type == "season":
            if year >= 1950:
                url = f"https://www.basketball-reference.com/leagues/NBA_{year}_totals.html"
            else:
                url = f"https://www.basketball-reference.com/leagues/BAA_{year}_totals.html"
        elif type == "playoff":
            if year >= 1950:
                url = f"https://www.basketball-reference.com/playoffs/NBA_{year}_totals.html"
            else:
                url = f"https://www.basketball-reference.com/playoffs/BAA_{year}_totals.html"

        try:
            tables = pd.read_html(url)
            df = tables[0]
            #save to csv
            filename = f'total_player_{type}_stat_{year}.csv'
            # df = df[:-1]
            df.to_csv(filename, index=False)

            print(f"Successfully saved {filename}")
        except Exception as e:
            print(f"Failed to get data for {year}: {e}")
        time.sleep(5)

def all_star_roster(start_year, end_year):
    years = range(start_year, end_year + 1)
    for year in years:
        if year != 1999:
            url = f'https://basketball.realgm.com/nba/allstar/game/rosters/{year}'
        else:
            print('No All-Star game in 1999')
            continue



        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')

            # find all tables, get the last one
            tables = soup.find_all('table')
            if not tables:
                print(f'No tables found for {year}')
                continue

            last_table = tables[-1]
            headers = [th.get_text(strip=True) for th in last_table.find('thead').find_all('th')]
            rows = last_table.find('tbody').find_all('tr')
            table_data = []

            for row in rows:
                cols = row.find_all('td')
                row_data = []
                for i, col in enumerate(cols):
                    text = col.get_text(strip=True)
                    if i == 0:  # Assuming the first column is the player's name
                        # Remove any quotes around player names
                        text = text.strip('"').strip("'")
                    row_data.append(text)
                if row_data:
                    table_data.append(row_data)

            if table_data:
                with open(f'nba_allstar_{year}.csv', 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(headers)
                    writer.writerows(table_data)
                print(f'Saved nba_allstar_{year}.csv')
            else:
                print(f'No data found in table for {year}')

        except Exception as e:
            print(f'Failed for year {year}: {e}')

        time.sleep(5)

def draft_class(start_year, end_year):
    #uses newer loading, dont need to parse comments
    for year in range(start_year, end_year + 1):
        if year >= 1950:
            url = f"https://www.basketball-reference.com/draft/NBA_{year}.html"
        else:
            url = f"https://www.basketball-reference.com/draft/BAA_{year}.html"

        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            table_html = soup.find("table", {"id": "stats"})

            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]
                filename = f'draft_class_{year}.csv'
                df.to_csv(filename, index=False)
                print(f"Successfully saved {filename}")
            else:
                print(f"Table not found for {year}")

        except Exception as e:
            print(f"Failed to get data for {year}: {e}")

        time.sleep(5)

def awards(award_type):
    """
    mvp: MOST VALUABLE PLAYER
    dpoy: DEFENSIVE PLAYER OF THE YEAR
    smoy: SIXTH MAN OF THE YEAR
    mip: MOST IMPROVED PLAYER
    all_league: ALL NBA TEAMS
    roy: ROOKIE OF THE YEAR
    """
    url = f'https://www.basketball-reference.com/awards/{award_type}.html'

    table_id_dict = {
        'mvp': 'mvp_NBA',
        'roy': 'roy_NBA',
        'dpoy': 'dpoy_NBA',
        'smoy': 'smoy_NBA',
        'mip': 'mip_NBA',
        'all_league': 'awards_all_league'
    }

    try:
        response = requests.get(url)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")
        table_html = soup.find("table", {"id": table_id_dict[f'{award_type}']})

        if table_html:
            df = pd.read_html(StringIO(str(table_html)))[0]
            filename = f'{award_type}'
            df.to_csv(filename, index=False)
            print(f"Successfully saved {filename}")
        else:
            print(f"Failed to get data for {award_type}: {e}")

    except Exception as e:
        print(f"Failed to get data for {award_type}: {e}")





if __name__ == "__main__":
    # team_stats(2025,2025,"advanced-team")
    # player_total_stat_pull(1947,2025,"season")
    # player_avg_stat_pull(1947,2024,'playoff')
    # expanded_standings(1947,2025)
    # standings(2024,2024, 'west')
    # all_star_roster(1998,2000)
    # draft_class(1947,1949)
    # awards('roy')

