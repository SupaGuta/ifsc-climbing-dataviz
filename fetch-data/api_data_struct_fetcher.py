from pathlib import Path
import requests
import json
import shlex
from urllib.parse import quote
from typing import Sequence
import re

# Config
BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "database" / "ifsc-data.sqlite"   
API_URL =  "https://ifsc.results.info/api/v1/"
API_DATA_STRUCT_FOLDER = BASE_DIR / "assets" / "api-data-struct"
_NUMERIC_ID = re.compile(r"^\d+$")


# API fetch function
def fetch_data(request_params: list[str] = []) -> None:
    headers = {
        'X-Csrf-Token': 'QsiFWuxEY1S9h_-dQgRA_7S5w9uvvmXsjq56QbTPw4i_g_XR68rMCBFFhW6HngBRtHskfN5yjX8GQmawqs8BlQ',
        'Referer': 'https://ifsc.results.info',
        'Cookie': 'session_id=_verticallife_resultservice_session=6RHN3xZrXnftTiScNfSHg7BVvuebLzGAmC9P5vIpzdySn2vG7VwQpjSZRDHug%2BPKCWlkt831HjLvHsPoVKrzTGsPVR6mqSOtjHB%2Bwht%2Bj39KxYO%2FJlaU6zmh8VhNFEl9bXHiOlPGk8AxnZqiBSYKTxJFCqh34nqdurXfFDcsRnbEtYCixcOdx%2F32E4zYGLVw7DSXXIKOVTUivS43UJZq5zDWPctX95UWm%2FD7%2B6UYT2s0B%2B3XJVPgjMWCMR%2FVZs%2FQC45Gjm4uCpHHe8Yt73nM3J%2Br43V1HuHGSvRpRczrJ4QdovlJHDEpg4rjUA%3D%3D',
    }

    request_path = build_path(request_params)
    url = API_URL + request_path

    try:
        response = requests.get(url, headers=headers, timeout=120)
        if response.status_code == 200:
            
            data_struct_filename = request_to_filename(request_path)
            with open(API_DATA_STRUCT_FOLDER / data_struct_filename, 'w', encoding='utf-8') as f:
                json.dump(response.json(), f, ensure_ascii=False, indent=4)

        else:
            print(response)
    
    except Exception as e:
        print("Exception")
        print(e)


# Build the API endpoint and params part
def build_path(params: Sequence[str] = ()) -> str:
    segments = [quote(p.strip("/"), safe="") for p in params]
    return "/".join(segments)


# Parse user inputs as endpoint and params
def parse_user_input(raw: str) -> list[str]:
    params = shlex.split(raw.strip())
    if not params:
        raise ValueError("Empty input. Example: endpoint param1 param2 etc")
    return params


# Transofrms all the request params into a json filename
def request_to_filename(path: str, ext: str = "json") -> str:
    # Cleans path var
    path = path.strip("/")
    segments = [s for s in path.split("/") if s]

    parts = []
    for seg in segments:
        if _NUMERIC_ID.match(seg):
            parts.append("id")
        else:
            parts.append(seg.lower())

    base = "-".join(parts) if parts else "root"

    # Clean strange characters
    base = re.sub(r"[^a-z0-9_-]+", "-", base).strip("-")

    return f"{base}.{ext}"


# Get and process user input
raw_input = input("Data to fetch: ")
raw_params = parse_user_input(raw_input)

# Fetch data with given params
fetch_data(raw_params)