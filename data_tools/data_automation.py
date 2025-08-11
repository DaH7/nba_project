import pandas as pd
import random
from config import DB_CONFIG
import re
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
from io import StringIO
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

engine = create_engine(
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

def award_auto(award_type):
    """
     Automates the process of getting all nba teams such as rookie, def, etc, clean the data and loads it to database

    TYPES OF NBA TEAMS
    all_league: ALL NBA TEAMS
    all_rookie : ALL ROOKIE
    all_defense: ALL DEFENSE

    Individual rewards (not completely automated yet since cleaning is not there)
    mvp: MOST VALUABLE PLAYER
    dpoy: DEFENSIVE PLAYER OF THE YEAR
    smoy: SIXTH MAN OF THE YEAR
    mip: MOST IMPROVED PLAYER
    roy: ROOKIE OF THE YEAR
    """
    #grabs the team award
    url = f'https://www.basketball-reference.com/awards/{award_type}.html'

    # mapping award type to html table ID
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

    url = f'https://www.basketball-reference.com/awards/{award_type}.html'
    filename = f'{award_type}.csv'
    cleaned_filename = f'cleaned_{filename}'

    try:
        # setup headless browser
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(random.uniform(2, 13))  # Random sleep to mimic human browsing

        soup = BeautifulSoup(driver.page_source, "html.parser")
        driver.quit()

        table_id = table_id_dict.get(award_type)
        if not table_id:
            raise ValueError(f"Invalid award_type: {award_type}")

        table_html = soup.find("table", {"id": table_id})
        if table_html:
            df = pd.read_html(StringIO(str(table_html)))[0]

            # split any columns that contain commas (e.g. tied players)
            for col in df.columns:
                if df[col].astype(str).str.contains(',').any():
                    split_df = df[col].astype(str).str.split(',', expand=True)
                    # Remove (T) from each split cell
                    split_df = split_df.applymap(lambda x: re.sub(r'\s*\(T\)', '', x.strip()) if isinstance(x, str) else x)
                    split_df.columns = [f"{col}_{i+1}" for i in range(split_df.shape[1])]
                    df = df.drop(columns=[col]).join(split_df)


            df.to_csv(filename, index=False)
            print(f"Successfully saved {filename}")
        else:
            print(f"Table with ID '{table_id}' not found.")

    except Exception as e:
        print(f"Failed to scrape data for {award_type}: {e}")
        return

    # clean csv
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        name_fix = {
            "Nikola JokiÄ": "Nikola Jokić",
            "Luka DonÄiÄ": "Luka Dončić",
            "Goran DragiÄ": "Goran Dragić",
            "Peja StojakoviÄ": "Peja Stojaković",
            "DraÅ¾en PetroviÄ": "Dražen Petrović"
        }

        cleaned_lines = []
        for line in lines:
            for bad_name, good_name in name_fix.items():
                line = line.replace(bad_name, good_name)

            # remove trailing positions
            line = re.sub(r'\s*\b([CFG](?:-[CFG])*)\b(?=,|\n|$)', '', line)

            # Fix season formatting: 2024-25 → 2025 (unless it's 1999-00 → 2000)
            line = re.sub(r'(\d{4})-(\d{2})', lambda m: "2000" if m.group(1) == "1999" and m.group(2) == "00" else str(int(m.group(1)[:2] + m.group(2))), line)

            if re.fullmatch(r'\s*,+\s*\n?', line):
                continue  # Skip lines with only commas
            cleaned_lines.append(line)

        with open(cleaned_filename, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)

        print(f"Cleaned file saved as: {cleaned_filename}")

        # upload to database
        df = pd.read_csv(cleaned_filename, low_memory=False)
        df.to_sql(cleaned_filename.replace('.csv', ''), con=engine, if_exists='replace', index=False)
        print(f"Uploaded {cleaned_filename} to database")

    except Exception as e:
        print(f"Cleaning/upload failed: {e}")

def champ_history_auto():
    """
    scraps,cleans, and uploads data of nba finals matchup and results such as finals mvp and max counting stats during the matchup

    """
    url = 'https://www.basketball-reference.com/playoffs/'

    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # comment this out if you want to see the browser
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=chrome_options)
        driver.get(url)
        time.sleep(random.uniform(2, 13))  # wait for page to load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        table_html = soup.find("table", {"id": "champions_index"})
        driver.quit()

        if table_html:
            df = pd.read_html(StringIO(str(table_html)),header= 1)[0]
            filename = f'finals_history.csv'
            df.to_csv(filename, index=False)
            print(f"Successfully saved {filename}")
        else:
            print(f"Table not found f")

    except Exception as e:
        print(f"Failed to get data: {e}")

    # clean csv
    cleaned_filename = f'cleaned_{filename}'
    try:
        with open(filename, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        name_fix = {
            "S. Gilgeous-Alexander": "Shai Gilgeous-Alexander",
            "K. Towns": "Karl-Anthony Towns",
            "T. Haliburton": "Tyrese Haliburton",
            "J. Brown": "Jaylen Brown",
            "S. Curry": "Stephen Curry",
            "N. Jokić": "Nikola Jokić",
            "G. Antetokounmpo": "Giannis Antetokounmpo",
            "L. Dončić": "Luka Dončić",
            "D. White": "Derrick White",
            "J. Tatum": "Jayson Tatum",
            "A. Horford": "Al Horford",
            "J. Butler": "Jimmy Butler",
            "K. Leonard": "Kawhi Leonard",
            "D. Green": "Draymond Green",
            "K. Durant": "Kevin Durant",
            "A. Iguodala": "Andre Iguodala",
            "K. Thompson": "Klay Thompson",
            "R. Westbrook": "Russell Westbrook",
            "T. Duncan": "Tim Duncan",
            "D. Nowitzki": "Dirk Nowitzki",
            "R. Rondo": "Rajon Rondo",
            "T. Chandler": "Tyson Chandler",
            "J. Kidd": "Jason Kidd",
            "K. Bryant": "Kobe Bryant",
            "P. Gasol": "Pau Gasol",
            "D. Howard": "Dwight Howard",
            "P. Pierce": "Paul Pierce",
            "T. Parker": "Tony Parker",
            "D. Wade": "Dwyane Wade",
            "S. Nash": "Steve Nash",
            "K. Garnett": "Kevin Garnett",
            "L. James": "LeBron James",
            "A. Davis": "Anthony Davis",
            "J. Holiday": "Jrue Holiday",
            "K. Love": "Kevin Love",
            "C. Billups": "Chauncey Billups",
            "B. Wallace": "Ben Wallace",
            "S. O'Neal": "Shaquille O'Neal",
            "A. Iverson": "Allen Iverson",
            "D. Mutombo": "Dikembe Mutombo",
            "M. Jackson": "Mark Jackson",
            "L. Sprewell": "Latrell Sprewell",
            "A. Johnson": "Avery Johnson",
            "M. Jordan": "Michael Jordan",
            "D. Rodman": "Dennis Rodman",
            "J. Stockton": "John Stockton",
            "K. Malone": "Karl Malone",
            "M. Malone": "Moses Malone",
            "H. Olajuwon": "Hakeem Olajuwon",
            "P. Ewing": "Patrick Ewing",
            "C. Barkley": "Charles Barkley",
            "I. Thomas": "Isiah Thomas",
            "J. Dumars": "Joe Dumars",
            "J. Worthy": "James Worthy",
            "L. Bird": "Larry Bird",
            "M. Johnson": "Magic Johnson",
            "K. Johnson": "Kevin Johnson",
            "H. Grant": "Horace Grant",
            "B. Laimbeer": "Bill Laimbeer",
            "K. Abdul-Jabbar": "Kareem Abdul-Jabbar",
            "R. Parish": "Robert Parish",
            "C. Maxwell": "Cedric Maxwell",
            "M. Cheeks": "Maurice Cheeks",
            "C. Jones": "Caldwell Jones",
            "W. Unseld": "Wes Unseld",
            "B. Walton": "Bill Walton",
            "J. White": "Jo Jo White",
            "J. Erving": "Julius Erving",
            "R. Barry": "Rick Barry",
            "W. Reed": "Willis Reed",
            "J. West": "Jerry West",
            "J. Havlicek": "John Havlicek",
            "W. Chamberlain": "Wilt Chamberlain",
            "D. Johnson": "Dennis Johnson"
        }


        cleaned_lines = []
        for line in lines:
            for bad_name, good_name in name_fix.items():
                line = line.replace(bad_name, good_name)

            if re.fullmatch(r'\s*,+\s*\n?', line):
                continue  # Skip lines with only commas
            cleaned_lines.append(line)

        with open(cleaned_filename, 'w', encoding='utf-8') as f:
            f.writelines(cleaned_lines)

        print(f"Cleaned file saved as: {cleaned_filename}")

        # upload to database
        df = pd.read_csv(cleaned_filename, low_memory=False)
        df.to_sql(cleaned_filename.replace('.csv', ''), con=engine, if_exists='replace', index=False)
        print(f"Uploaded {cleaned_filename} to database")

    except Exception as e:
        print(f"Cleaning/upload failed: {e}")


if __name__ == "__main__":
    # award_auto()
    champ_history_auto()