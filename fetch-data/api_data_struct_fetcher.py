from pathlib import Path
import requests
import json

# Config
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database" / "ifsc-data.sqlite"   
API_URL =  "https://ifsc.results.info/api/v1/"
DATA_STRUCT_FOLDER = BASE_DIR / "assets"

# API fetch function
def fetch_data(endpoint, params):
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }

    url = API_URL+endpoint+"/"+str(params)

    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            
            data_struct_file = endpoint+"_data_struct.json"
            with open(DATA_STRUCT_FOLDER / data_struct_file, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)

        else:
            print(response)
    
    except Exception as e:
        print(e)

endpoint = input("Enter endpoint name: ")
params = input("Enter params: ")
fetch_data(endpoint, params)