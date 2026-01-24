# The purpose of this script is simply to test IFSC's API endpoints
# and store the response as a json file for future reference

# Imported modules
from pathlib import Path
import requests
import json
import re
import traceback


# General config
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "database" / "ifsc-data.sqlite"   
API_BASE_URL =  "https://ifsc.results.info/api/v1"
API_DATA_STRUCT_FOLDER = BASE_DIR / "assets" / "api-data-structures"
_NUMERIC_ID = re.compile(r"^\d+$")


# API fetch function
# (Needed headers found thanks to https://github.com/ChickenNungets/IFSC-data-analysis)
def fetch_data(path):
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }

    url = API_BASE_URL + path

    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            
            data_struct_filename = request_to_filename(path)
            with open(API_DATA_STRUCT_FOLDER / data_struct_filename, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
            print("Succesfully stored data struct to file:", data_struct_filename)

        else:
            print("REQUEST ERROR")
            print(
                f"URL: {response.url}\n"
                f"Status: {response.status_code} {response.reason}"
            )
    
    except Exception as e:
        print("EXCEPTION ERROR")
        traceback.print_exc()


# Transofrms all the request params into a json filename
def request_to_filename(path):
    if path == '':
        base = "root"
    
    else:
        path = path.strip("/")

        parts = []
        for part in path.split("/"):
            if _NUMERIC_ID.match(part):
                parts.append("id")
            else:
                parts.append(part.lower())

        base = "-".join(parts)

    return base + ".json"


# Get and process user input
request_path = input("Request path (i.e.: /athletes/1364): ")

# Fetch data with given params
fetch_data(request_path)