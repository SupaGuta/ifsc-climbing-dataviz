# This script will scrap all ids from seasons, seans_leagues, events, results and athletes from the ISFC API
# Everything is stored in a simple SQLite database for futher reference
# Once done, we'll use it to gather all the associated content

# Imported modules
from pathlib import Path
import sqlite3
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from queue import Queue


# General config
BASE_DIR = Path(__file__).resolve().parent.parent
LOG_FILE = BASE_DIR / "assets" / "logs" / "data_struct_fether_errors.log"
DB_FILE = BASE_DIR / "assets" / "databases" / "ifsc-data-struct.sqlite"   
API_BASE_URL =  "https://ifsc.results.info/api/v1"
HEADERS = {
    'X-Csrf-Token': 'as3PvBDSeMFu_fMVW8Qtp6VDdrJrsHsaToMcOCCK-fkVlwUyCXfeDGXOpFrUv1IBnYdUZYgY-uSW-SoIrWN8bQ',
    'Referer': 'https://ifsc.results.info',
    'Cookie': 'session_id=_verticallife_resultservice_session=fOdV4TuFGIzU315JSVh5TyUd2PeLcdwC9iy6lpNWDBtpjvUhnQCdDZgR90CO57VRc4OMyowGSrzA%2BczbyKPyCMIMa1yr5%2BojTEzGP2fQei8s6v4tNmyueStVlYL46gBo8HXYC%2Fx0yrvRSAuR2rWU4UPnqa%2FrG66wvDOmBBh86GzbWj2ZBfEnOCnxY1gI1PSKYu%2BW4SZ%2FKPR%2FOyL70oWRWCM3pytRBaRPn%2FKlEksHjM%2B2XlkzNRGQi7lFDDeElvUDTRj5aHR2cXkTl0JOFgRY%2B7LWq5vRH6WKwHCmO2%2BEDZxdtMhFqC7aruunhQ%3D%3D--rHK2FeAgP%2BAcKiNE--TvOXDr4r0Rxitjml2KEIFA%3D%3D',
}
# Seasons range IDs (checked manually)
START_ID = 2
END_ID = 37 # 2025


# Initiate log file
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, format='%(asctime)s %(levelname)s: %(message)s')


# Initialize database
db_conn = sqlite3.connect(DB_FILE)
db_cur = db_conn.cursor()

# Drop then create database tables
db_cur.executescript('''
DROP TABLE IF EXISTS Seasons;
DROP TABLE IF EXISTS Season_Leagues;
DROP TABLE IF EXISTS Events;
DROP TABLE IF EXISTS Results;
DROP TABLE IF EXISTS Athletes;
                     
CREATE TABLE IF NOT EXISTS Seasons (
    id INTEGER PRIMARY KEY,
    ifsc_id INTEGER UNIQUE
);
                     
CREATE TABLE IF NOT EXISTS Season_Leagues (
    id INTEGER PRIMARY KEY,
    ifsc_id INTEGER UNIQUE,
    season_id INTEGER
);
                     
CREATE TABLE IF NOT EXISTS Events (
    id INTEGER PRIMARY KEY,
    ifsc_id INTEGER UNIQUE,
    season_league_id INTEGER
);
                     
CREATE TABLE IF NOT EXISTS Results (
    id INTEGER PRIMARY KEY,
    ifsc_id INTEGER,
    event_id INTEGER,
    UNIQUE (ifsc_id, event_id)
);
                     
CREATE TABLE IF NOT EXISTS Athletes (
    id INTEGER PRIMARY KEY,
    ifsc_id INTEGER UNIQUE
);
''')


# Fetching API data
def fetch_data(path, data_id, data_queue, failed_queue):
    url = API_BASE_URL + path
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=120)
        if response.status_code == 200:
            data = response.json()
            data["ifsc_id"] = data_id # Add current data ID for future reference
            data_queue.put(data)
        else:
            logging.error(f"Failed to fetch '{path}': Status {response.status_code}, Reason: {response.reason}")
            failed_queue.put(data_id)
    
    except Exception as e:
        logging.error(f"Error fetching data for '{path}': {e}")
        failed_queue.put(data_id)


# Retry if fetching failed
def retry_failed_info(endpoint, failed_ids, max_retries=2, delay=2):
    retry_results = []
    failed_queue = Queue()

    for retry_count in range(max_retries):
        print(f"Retry attempt {retry_count + 1} for {len(failed_ids)} failed items.")
        retry_futures = []
        data_queue = Queue()
        
        with ThreadPoolExecutor(max_workers=20) as executor:            
            retry_futures = {}
            for data_id in failed_ids:
                path = parse_api_path(endpoint, data_id)
                retry_futures[executor.submit(fetch_data, path, data_id, data_queue, failed_queue)] = path

            failed_ids = []  # Reset failed_ids list for next retry
        
            for future in as_completed(retry_futures):
                try:
                    future.result()
                except Exception as e:
                    logging.error(f"Error during retry for: {e}")
        
        # Collect results from queues
        while not data_queue.empty():
            retry_results.append(data_queue.get())
        
        while not failed_queue.empty():
            failed_ids.append(failed_queue.get())
        
        if not failed_ids:
            break  # Exit loop if no more failed IDs
        
        time.sleep(delay)  # Wait between retries to avoid overloading the server
    
    if failed_ids:
        print(f"Final failed items after {max_retries} retries: {failed_ids}")
    
    return retry_results, failed_ids


# Start thread workers
def scrape_parallel(endpoint, data_ids, max_workers=25):
    info = []
    failed_ids = []
    data_queue = Queue()
    failed_queue = Queue()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit tasks for each ID futures
        futures = {}
        for data_id in data_ids:
            path = parse_api_path(endpoint, data_id)
            futures[executor.submit(fetch_data, path, data_id, data_queue, failed_queue)] = path
        
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error during scraping: {e}")
    
    # Collect results from queues
    while not data_queue.empty():
        info.append(data_queue.get())
    
    while not failed_queue.empty():
        failed_ids.append(failed_queue.get())
    
    # Retry for failed athlete IDs
    if failed_ids:
        retry_results, failed_ids = retry_failed_info(endpoint, failed_ids)
        info.extend(retry_results)  # Add successful retries

    return info, failed_ids


# Parse API path
def parse_api_path(endpoint, data_id):
    path = f"/{endpoint}/{data_id}"
    return path


# Gather data from seasons endpoint
# API : /seasons/id
# Fill tables: Seasons, Season_Leagues, Events
print("Started scraping seasons...")
data_list, failed_ids = scrape_parallel("seasons", range(START_ID, END_ID+1))

# Parse data and save to database
seasons_count = 0
season_leagues_count = 0
events_count = 0

try:
    for data in data_list:
        # Seasons
        ifsc_id = data.get("ifsc_id", None)    
        db_cur.execute("INSERT OR IGNORE INTO Seasons (ifsc_id) VALUES ( ? )", ( ifsc_id, )) 
        row = db_cur.execute('SELECT id FROM Seasons WHERE ifsc_id = ? ', (ifsc_id, ))
        season_id = db_cur.fetchone()[0]
        seasons_count = seasons_count + 1

        # Season_Leagues
        season_leagues = data.get("leagues", None)
        for season_league in season_leagues:
            season_league_ifsc_id = int(season_league.get("url", None).strip('/').split('/')[3])
            db_cur.execute("INSERT OR IGNORE INTO Season_Leagues (ifsc_id, season_id) VALUES ( ?, ? )", (season_league_ifsc_id, season_id))
            db_cur.execute("SELECT id FROM Season_Leagues WHERE ifsc_id = ?", (season_league_ifsc_id, ))  
            season_league_id = db_cur.fetchone()[0]
            season_leagues_count = season_leagues_count + 1

        # Events
        events = data.get("events", None) 
        for event in events:
            event_ifsc_id = event.get("event_id", None)
            db_cur.execute("INSERT OR IGNORE INTO Events (ifsc_id, season_league_id) VALUES ( ?, ? )", (event_ifsc_id, season_league_id))
            events_count = events_count + 1
        
        # Commit all season info to database
        db_conn.commit()

    # Output
    print(f"Scraped {seasons_count} seasons, {season_leagues_count} season leagues and {events_count} events.")

except Exception as e:
    logging.error(f"Error parsing data for '/seasons/{ifsc_id}': {e}")

# Gather data from events endpoint
# API : /events/id
# Fill table: Results
print("Started scraping events...")
events_ifsc_ids = [int(row[0]) for row in db_cur.execute("SELECT ifsc_id FROM Events")]
data_list, failed_ids = scrape_parallel("events", events_ifsc_ids)

# Parse data and save to database
events_count = 0
results_count = 0

try:
    for data in data_list:    
        ifsc_id = data.get("ifsc_id", None) 
        db_cur.execute("SELECT id FROM Events WHERE ifsc_id = ?", (ifsc_id, ))
        row = db_cur.fetchone()
        if row:
            event_id = row[0]
        else:
            continue
        events_count = events_count + 1

        # Results
        d_cats = data.get("d_cats", None)
        for d_cat in d_cats:
            result_ifsc_id = d_cat.get("dcat_id", None)
            db_cur.execute("INSERT OR IGNORE INTO Results (ifsc_id, event_id) VALUES ( ?, ? )", (result_ifsc_id, event_id))
            results_count = results_count + 1
        
        # Commit all season info to database
        db_conn.commit()

    # Output
    print(f"Scraped {events_count} events and {results_count} results.")

except Exception as e:
    logging.error(f"Error parsing data for '/events/{ifsc_id}': {e}")


# Gather data from results endpoint
# API : /events/id/result/id
# Fill table: Athletes

# Get event ids to scrape results
events_count = 0
results_count = 0
athletes_count = 0

events = db_cur.execute("SELECT id, ifsc_id FROM Events ORDER BY id ASC").fetchall()
for event_id, event_ifsc_id in events:
    print("Started scraping results for event", event_ifsc_id, "...")
    results_ifsc_ids = [int(row[0]) for row in db_cur.execute("SELECT ifsc_id FROM Results WHERE event_id = ?", (event_id, ))]

    data_list, failed_ids = scrape_parallel("events/"+str(event_ifsc_id)+"/result", results_ifsc_ids)
    events_count = events_count + 1

    try:
        # Parse data and save to database
        for data in data_list: 
            ifsc_id = data.get("ifsc", None)
            results_count = results_count + 1

            # Athletes
            rankings = data.get("ranking", None)
            if not rankings:
                continue

            for ranking in rankings:
                athlete_ifsc_id = ranking.get("athlete_id", None)
                db_cur.execute("INSERT OR IGNORE INTO Athletes (ifsc_id) VALUES ( ? )", (athlete_ifsc_id, ))
                if db_cur.rowcount == 1:
                    athletes_count = athletes_count + 1
            
            # Commit all season info to database
            db_conn.commit()

    except Exception as e:
        logging.error(f"Error parsing data for '/events/{event_ifsc_id}/result/{ifsc_id}': {e}")

    # Output
    print(f"Scraped {events_count} events, {results_count} results and {athletes_count} athletes.")


# Close database handle
db_cur.close()