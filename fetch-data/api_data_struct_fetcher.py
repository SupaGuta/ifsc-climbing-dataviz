from pathlib import Path
import requests
import json

# Config
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database" / "ifsc-data.sqlite"   
API_ENDPOINT =  "https://ifsc.results.info/api/v1/athletes/"
DATA_STRUCT_FILE = BASE_DIR / "assets" / "athlete_data_struct.json"

# API fetch function
def fetch_data(id):
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }
    url = API_ENDPOINT+str(id)
    
    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            with open(DATA_STRUCT_FILE, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)
        else:
            print("Error")
    
    except Exception as e:
        print("Error")

fetch_data(1364)