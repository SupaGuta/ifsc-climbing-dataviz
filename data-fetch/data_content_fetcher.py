# This script will scrap all ids from seasons, seans_leagues, events, results and athletes from the ISFC API
# Everything is stored in a simple SQLite database for futher reference
# Once done, we'll use it to gather all the associated content

# Imported modules
from pathlib import Path
import sys
import sqlite3
import logging
import re
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))
from assets.helpers import data_fetcher


# General config
LOG_FILE = BASE_DIR / "assets" / "logs" / "data_content_fether_errors.log"
DB_STRUCT = BASE_DIR / "assets" / "databases" / "ifsc-data-struct.sqlite"  
DB_CONTENT = BASE_DIR / "assets" / "databases" / "ifsc-data-content.sqlite"


# Initiate log file
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

# Initialize databases
db_struct_conn = sqlite3.connect(DB_STRUCT)
db_struct_cur = db_struct_conn.cursor()

db_content_conn = sqlite3.connect(DB_CONTENT)
db_content_cur = db_content_conn.cursor()

# Drop then create database tables
db_content_cur.executescript('''
DROP TABLE IF EXISTS Seasons;
DROP TABLE IF EXISTS Leagues;
DROP TABLE IF EXISTS Disciplines;
DROP TABLE IF EXISTS Categories;
                     
CREATE TABLE IF NOT EXISTS Seasons (
    id INTEGER PRIMARY KEY,
    year INTEGER UNIQUE,
    ifsc_id INTEGER UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Leagues (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Disciplines (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Categories (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE,
    gender INT
);
''')


# Gather data from seasons endpoint
# API : /seasons/id
# Fill tables: Seasons, Leagues

# Retrieve valid seasons' ifsc_ids from struct database
seasons_ifsc_ids = [int(row[0]) for row in db_struct_cur.execute("SELECT ifsc_id FROM Seasons")]

# Start scraping data from API
print("Started scraping seasons...")
data_list, failed_ids = data_fetcher.scrape_parallel("seasons", seasons_ifsc_ids)

# Parse data and save to database
seasons_count = 0
leagues_count = 0

try:
    for data in data_list:
        # Seasons
        ifsc_id = data.get("ifsc_id", None)
        season_year = data.get("name", None) 
        db_content_cur.execute("INSERT OR IGNORE INTO Seasons (year, ifsc_id) VALUES ( ?, ? )", (season_year, ifsc_id)) 
        row = db_content_cur.execute('SELECT id FROM Seasons WHERE ifsc_id = ? ', (ifsc_id, ))
        season_id = db_content_cur.fetchone()[0]
        seasons_count = seasons_count + 1

        # Leagues
        leagues = data.get("leagues", None)
        for league in leagues:
            # Leagues
            league_name = league.get("name", None)
            db_content_cur.execute("INSERT OR IGNORE INTO Leagues (name) VALUES ( ? )", (league_name, )) 
            if db_content_cur.rowcount == 1:  
                leagues_count = leagues_count + 1 
        
        # Commit all season info to database
        db_content_conn.commit()

    # Output
    print(f"Scraped {seasons_count} seasons and {leagues_count} leagues.")

except Exception as e:
    logging.error(f"Error parsing data for '/seasons/{ifsc_id}': {e}")


# Gather data from season_leagues endpoint
# API : /season_leagues/id
# Fill tables: Disciplines, Categories, Events (partially)

# Retrieve valid seasons' ifsc_ids from struct database
season_leagues_ifsc_ids = [int(row[0]) for row in db_struct_cur.execute("SELECT ifsc_id FROM Season_Leagues")]

# Start scraping data from API
print("Started scraping season_leagues...")
data_list, failed_ids = data_fetcher.scrape_parallel("season_leagues", season_leagues_ifsc_ids)

# Parse data and save to database
season_leagues_count = 0
disciplines_count = 0
categories_count = 0
events_count = 0

try:
    for data in data_list:
        # Parse disciplines and categories (name and gender)
        dicipline_categories = data.get("d_cats", None)
        for d_cat in dicipline_categories:
            d_cat_name = d_cat.get("name", None)

            # Discipline and category name
            parts = d_cat_name.lower().strip().split(maxsplit=1)
            discipline_name = parts[0]
            category_name = parts[1] or ""

            # Gender (int)
            gender_match = re.search("\b(?P<g>men|male|women|female)\b", category_name)
            category_gender = None
            if gender_match:
                gender_str = gender_match.group("g").lower()
                category_gender = 0 if gender_str in ("men", "male") else 1

            # Add dsicipline
            db_content_cur.execute("INSERT OR IGNORE INTO Disciplines (name) VALUES ( ? )", (discipline_name, )) 
            if db_content_cur.rowcount == 1:  
                disciplines_count = disciplines_count + 1 

            # Add category
            db_content_cur.execute("INSERT OR IGNORE INTO Categories (name, gender) VALUES ( ?, ? )", (category_name, category_gender)) 
            if db_content_cur.rowcount == 1:  
                categories_count = categories_count + 1

        # Get season and league ids from the corresponding table
        season_year = data.get("season", None)         
        row = db_content_cur.execute('SELECT id FROM Seasons WHERE year = ? ', (season_year, ))
        season_id = db_content_cur.fetchone()[0]

        league_name = data.get("league", None)         
        row = db_content_cur.execute('SELECT id FROM Leagues WHERE name = ? ', (league_name, ))
        league_id = db_content_cur.fetchone()[0]

        
        # Commit all season info to database
        db_content_conn.commit()

    # Output
    print(f"Scraped {disciplines_count} disciplmines and {categories_count} categories.")

except Exception as e:
    logging.error(f"Error parsing data for '/season_leagues/{ifsc_id}': {e}")


# Close database handles
db_struct_conn.close()
db_content_conn.close()