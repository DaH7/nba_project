import os
import pandas as pd
import random
from sqlalchemy import create_engine
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
    function: automates the process of getting all nba teams such as rookie, def, etc, clean the data and loads it to database

    TYPES OF NBA TEAMS
    mvp: MOST VALUABLE PLAYER
    dpoy: DEFENSIVE PLAYER OF THE YEAR
    smoy: SIXTH MAN OF THE YEAR
    mip: MOST IMPROVED PLAYER
    roy: ROOKIE OF THE YEAR
    all_league: ALL NBA TEAMS
    all_rookie : ALL ROOKIE
    all_defense: ALL DEFENSE
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

award_auto('all_league')