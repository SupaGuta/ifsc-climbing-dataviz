# This script will scrap all ids from seasons, seans_leagues, events, results and athletes from the ISFC API
# Everything is stored in a simple SQLite database for futher reference
# Once done, we'll use it to gather all the associated content

# Imported modules
from pathlib import Path
import sys
import sqlite3
import logging
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
DROP TABLE IF EXISTS Season_Leagues;
                     
CREATE TABLE IF NOT EXISTS Seasons (
    id INTEGER PRIMARY KEY,
    year INTEGER UNIQUE,
    ifsc_id INTEGER UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Leagues (
    id INTEGER PRIMARY KEY,
    name TEXT UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Season_Leagues (
    id INTEGER PRIMARY KEY,
    season_id INTEGER,
    league_id INTEGER,
    ifsc_id INTEGER UNIQUE,
    UNIQUE (season_id, league_id)
);
''')


# Gather data from seasons endpoint
# API : /seasons/id
# Fill tables: Seasons, Leagues, Season_Leagues

# Retrieve valid seasons' ifsc_ids from struct database
seasons_ifsc_ids = [int(row[0]) for row in db_struct_cur.execute("SELECT ifsc_id FROM Seasons")]

# Start scraping data from API
print("Started scraping seasons...")
data_list, failed_ids = data_fetcher.scrape_parallel("seasons", seasons_ifsc_ids)

# Parse data and save to database
seasons_count = 0
leagues_count = 0
season_leagues_count = 0

try:
    for data in data_list:
        # Seasons
        ifsc_id = data.get("ifsc_id", None)
        season_year = data.get("name", None) 
        db_content_cur.execute("INSERT OR IGNORE INTO Seasons (year, ifsc_id) VALUES ( ?, ? )", (season_year, ifsc_id)) 
        row = db_content_cur.execute('SELECT id FROM Seasons WHERE ifsc_id = ? ', (ifsc_id, ))
        season_id = db_content_cur.fetchone()[0]
        seasons_count = seasons_count + 1

        # Leagues and Season_Leagues
        season_leagues = data.get("leagues", None)
        for season_league in season_leagues:
            # Leagues
            league_name = season_league.get("name", None)
            db_content_cur.execute("INSERT OR IGNORE INTO Leagues (name) VALUES ( ? )", (league_name, )) 
            if db_content_cur.rowcount == 1:  
                leagues_count = leagues_count + 1   

            row = db_content_cur.execute('SELECT id FROM leagues WHERE name = ? ', (league_name, ))
            league_id = db_content_cur.fetchone()[0]

            # Season_Leagues
            season_league_ifsc_id = int(season_league.get("url", None).strip('/').split('/')[3])
            db_content_cur.execute("INSERT OR IGNORE INTO Season_Leagues (season_id, league_id, ifsc_id) VALUES ( ?, ?, ? )", (season_id, league_id, season_league_ifsc_id))
            season_leagues_count = season_leagues_count + 1
        
        # Commit all season info to database
        db_content_conn.commit()

    # Output
    print(f"Scraped {seasons_count} seasons, {leagues_count} leagues and {season_leagues_count} season leagues.")

except Exception as e:
    logging.error(f"Error parsing data for '/seasons/{ifsc_id}': {e}")


# Close database handles
db_struct_conn.close()
db_content_conn.close()