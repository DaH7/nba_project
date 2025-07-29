import pandas as pd
import requests
from bs4 import BeautifulSoup,Comment
import csv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
import pandas as pd
from io import StringIO
import time
import random

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

def standings(start_year, end_year, division_type = 'none'):
    """
    grabs the division standings of each yeah in the nba
    returns full division standings for all years before 1970 and gets all years in east and west for 1971 forward
    """
    years = range(start_year, end_year + 1)
    for year in years:
        if year >= 1950:
            url = f"https://www.basketball-reference.com/leagues/NBA_{year}_standings.html"
        else:
            url = f"https://www.basketball-reference.com/leagues/BAA_{year}_standings.html"

        try:
            response = requests.get(url)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")

            # Initialize table_html
            table_html = None

            # Search for tables in comments
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                comment_soup = BeautifulSoup(comment, "html.parser")
                if year <= 1970:
                    table = comment_soup.find("table", {"id": "divs_standings_"})
                    if table:
                        table_html = table
                        break
                else:
                    if division_type == 'west':
                        table = comment_soup.find("table", {"id": "divs_standings_W"})
                        if table:
                            table_html = table
                            break
                    elif division_type == 'east':
                        table = comment_soup.find("table", {"id": "divs_standings_E"})
                        if table:
                            table_html = table
                            break

            # If not found in comments, search in main HTML
            if not table_html:
                if year >= 1971:
                    if division_type == 'west':
                        table_html = soup.find("table", {"id": "divs_standings_W"})
                    elif division_type == 'east':
                        table_html = soup.find("table", {"id": "divs_standings_E"})
                else:
                    table_html = soup.find("table", {"id": "divs_standings_"})

            # If table is found, process and save
            if table_html:
                df = pd.read_html(StringIO(str(table_html)))[0]
                if year >= 1971:
                    filename = f'{division_type}_div_standings_{year}.csv'
                else:
                    filename = f'all_div_standings_{year}.csv'

                df.to_csv(filename, index=False)
                print(f"Saved {filename}")
            else:
                print(f"No standings table found for {year}")

        except Exception as e:
            print(f"Failed to fetch or parse data for {year}: {e}")

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
    Opponent Per Game: per_game-opponent
    Four Factors: four_factors
    total opponent stat: totals-opponent
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
        'all_league': 'awards_all_league',
        'all_rookie': 'awards_all_rookie',
        'all_defense': 'awards_all_defense'
    }

    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Referer': 'https://www.basketball-reference.com/',
            'Upgrade-Insecure-Requests': '1',
        }
        response = requests.get(url, headers=headers)
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



def awards_selenium(award_type):
    """
    mvp: MOST VALUABLE PLAYER
    dpoy: DEFENSIVE PLAYER OF THE YEAR
    smoy: SIXTH MAN OF THE YEAR
    mip: MOST IMPROVED PLAYER
    roy: ROOKIE OF THE YEAR
    all_league: ALL NBA TEAMS
    all_rookie : ALL ROOKIE
    all_defense: ALL DEFENSE
    """

    url = f'https://www.basketball-reference.com/awards/{award_type}.html'

    table_id_dict = {
        'mvp': 'mvp_NBA',
        'roy': 'roy_NBA',
        'dpoy': 'dpoy_NBA',
        'smoy': 'smoy_NBA',
        'mip': 'mip_NBA',
        'all_league': 'awards_all_league',
        'all_rookie': 'awards_all_rookie',
        'all_defense': 'awards_all_defense'
    }

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # comment this out if you want to see the browser
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(random.uniform(2, 13))  # wait for page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        table_id = table_id_dict.get(award_type)
        if not table_id:
            raise ValueError(f"Invalid award_type: {award_type}")

        table_html = soup.find("table", {"id": table_id})
        if table_html:
            df = pd.read_html(StringIO(str(table_html)))[0]
            filename = f'{award_type}.csv'
            df.to_csv(filename, index=False)
            print(f" Successfully saved {filename}")
        else:
            print(f" Table with ID '{table_id}' not found on the page.")

    except Exception as e:
        print(f" Failed to get data for {award_type}: {e}")



if __name__ == "__main__":
    # team_stats(1947,2025,"totals-opponent")
    # player_total_stat_pull(1947,2025,"season")
    # player_avg_stat_pull(1947,2024,'playoff')
    # expanded_standings(1947,2025)
    # all_star_roster(1998,2000)
    # draft_class(1947,1949)
    # awards('mvp')

    awards_selenium('all_league')
