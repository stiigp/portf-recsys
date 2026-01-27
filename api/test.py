import os
import requests
from dotenv import load_dotenv

load_dotenv()
key = os.getenv("TMDB_API_KEY")

url = f"https://api.themoviedb.org/3/movie/862"
headers = {
    "accept": "application/json",
    "Authorization": f"Bearer {key}"
}

res = requests.get(url, headers=headers)
data = res.json()

print(data.get("poster_path"))
