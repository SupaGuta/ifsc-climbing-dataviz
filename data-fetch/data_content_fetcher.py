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
DATA_TO_FETCH = {
    "seasons" : True,
    "season_leagues" : True,
    "events" : True,
    "results" : True,
    "athletes" : True 
}


# Initiate log file
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')

# Initialize databases
db_struct_conn = sqlite3.connect(DB_STRUCT)
db_struct_cur = db_struct_conn.cursor()

db_content_conn = sqlite3.connect(DB_CONTENT)
db_content_cur = db_content_conn.cursor()


# Gather data from seasons endpoint
# API : /seasons/id
# Fill tables: Seasons, Leagues

if DATA_TO_FETCH["seasons"]:

    # Drop existing tables and create again
    db_content_cur.executescript('''
    DROP TABLE IF EXISTS Seasons;
    DROP TABLE IF EXISTS Leagues;
                        
    CREATE TABLE IF NOT EXISTS Seasons (
        id INTEGER PRIMARY KEY,
        year INTEGER UNIQUE,
        ifsc_id INTEGER UNIQUE
    );
                        
    CREATE TABLE IF NOT EXISTS Leagues (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    );
    ''')

    # Start scraping data from API
    print("Started scraping seasons...")
    # Retrieve valid seasons' ifsc_ids from struct database then start scraping
    seasons_ifsc_ids = [int(row[0]) for row in db_struct_cur.execute("SELECT ifsc_id FROM Seasons")]
    data_list, failed_ids = data_fetcher.scrape_parallel("seasons", seasons_ifsc_ids)

    # Parse data and save to database
    seasons_count = 0
    leagues_count = 0
    errors_count = 0
    
    for data in data_list:
        try:
            # Get current season_league id from IFSC 
            season_ifsc_id = data.get("ifsc_id", None)

            # Parse seasons data
            season_year = data.get("name", None) 
            db_content_cur.execute("INSERT OR IGNORE INTO Seasons (year, ifsc_id) VALUES ( ?, ? )", (season_year, season_ifsc_id)) 
            row = db_content_cur.execute('SELECT id FROM Seasons WHERE ifsc_id = ? ', (season_ifsc_id, ))
            season_id = db_content_cur.fetchone()[0]
            seasons_count = seasons_count + 1

            # Parse leagues data
            leagues = data.get("leagues", None)
            for league in leagues:
                league_name = league.get("name", None)
                db_content_cur.execute("INSERT OR IGNORE INTO Leagues (name) VALUES ( ? )", (league_name, )) 
                if db_content_cur.rowcount == 1:  
                    leagues_count = leagues_count + 1 
            
            # Commit all info to database
            db_content_conn.commit()

        except Exception as e:
            logging.error(f"Error parsing data from '/seasons/{season_ifsc_id}': {e}")
            errors_count = errors_count + 1
            continue

    # Output
    print(f"Scraped {seasons_count} seasons and {leagues_count} leagues ({errors_count} errors).")


# Gather data from season_leagues endpoint
# API : /season_leagues/id
# Fill tables: Disciplines, Categories, Events (partially: season_id, league_id_ ifsc_id)

if DATA_TO_FETCH["season_leagues"]:

    # Drop existing tables and create again
    db_content_cur.executescript('''
    DROP TABLE IF EXISTS Disciplines;
    DROP TABLE IF EXISTS Categories;
    DROP TABLE IF EXISTS Events;
                        
    CREATE TABLE IF NOT EXISTS Disciplines (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    );
                        
    CREATE TABLE IF NOT EXISTS Categories (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE,
        gender INT
    );
                        
    CREATE TABLE IF NOT EXISTS Events (
        id INTEGER PRIMARY KEY,
        season_id INTEGER,
        league_id INTEGER,
        ifsc_id INTEGER UNIQUE
    );
    ''')

    # Start scraping data from API
    print("Started scraping season_leagues...")
    # Retrieve valid season_leagues' ifsc_ids from struct database then start scraping
    season_leagues_ifsc_ids = [int(row[0]) for row in db_struct_cur.execute("SELECT ifsc_id FROM Season_Leagues")]
    data_list, failed_ids = data_fetcher.scrape_parallel("season_leagues", season_leagues_ifsc_ids)

    # Parse data and save to database
    season_leagues_count = 0
    disciplines_count = 0
    categories_count = 0
    events_count = 0
    errors_count = 0

    for data in data_list:
        try:
            # Get current season_league id from IFSC       
            season_leagues_ifsc_id = data.get("ifsc_id", None)

            # Parse disciplines and categories (name and gender)
            dicipline_categories = data.get("d_cats", None)
            for d_cat in dicipline_categories:
                d_cat_name = d_cat.get("name", None)

                # Discipline and category name
                parts = d_cat_name.strip().split(maxsplit=1)
                discipline_name = parts[0]
                if discipline_name == "BOULDER&LEAD":
                    discipline_name = re.sub(r"\s*&\s*", " & ", discipline_name)
                discipline_name = discipline_name.capitalize()
                category_name = parts[1] or ""

                # Gender (int)
                gender_match = re.search("\b(?P<g>men|male|women|female)\b", category_name, re.IGNORECASE)
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

            # Parse events form season_leagues endpoint
            events = data.get("events", None)
            for event in events:
                event_ifsc_id = event.get("event_id", None)

                # Add event to database
                db_content_cur.execute("INSERT OR IGNORE INTO Events (season_id, league_id, ifsc_id) VALUES ( ?, ?, ? )", (season_id, league_id, event_ifsc_id)) 
                if db_content_cur.rowcount == 1:  
                    events_count = events_count + 1 
            
            # Commit all info to database
            db_content_conn.commit()

        except Exception as e:
            logging.error(f"Error parsing data from '/season_leagues/{season_leagues_ifsc_id}': {e}")
            errors_count = errors_count + 1
            continue

    # Output
    print(f"Scraped {disciplines_count} disciplmines, {categories_count} categories and {events_count} events ({errors_count} errors).")


# Close database handles
db_struct_conn.close()
db_content_conn.close()