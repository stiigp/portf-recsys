import pandas as pd
import json
import requests
import os
from dotenv import load_dotenv

load_dotenv()

TMDB_API_KEY = os.getenv("TMDB_API_KEY")
OUTPUT_FILE = 'elastic_module/movies_with_tags.ndjson'
INDEX_NAME = 'movies'


def read_movies_and_tags():
    return pd.read_csv("dataset/movies_clean.csv"), pd.read_csv("dataset/tags.csv")

def read_links():
    return pd.read_csv("dataset/links_w_rt.csv")

def read_ratings():
    return pd.read_csv("dataset/ratings_clean.csv")

def convert_row_to_dict(row):
    return row.to_dict()

def put_tags_into_row(movie_id, row, tags):
    row['tags'] = list(set([tag for tag in tags.loc[tags['movieId'] == movie_id]['tag']]))

def calculate_avg_rating(ratings):
    return ratings['rating'].mean()

def calculate_imdb_rating(movie_id, ratings, avg_rating):
    minimum_ratings = 50
    movie_ratings = ratings.loc[ratings['movieId'] == movie_id]['rating']

    movie_avg_rating = movie_ratings.mean()
    number_of_movie_ratings = movie_ratings.shape[0]

    return ((number_of_movie_ratings/(number_of_movie_ratings+minimum_ratings)) * movie_avg_rating) + ((minimum_ratings/(number_of_movie_ratings+minimum_ratings)) * avg_rating)
    
    

def put_imdb_rating_into_row(movie_id, row, avg_rating):
    imdb_rating = calculate_imdb_rating(movie_id, avg_rating)

    row['imdbrating'] = imdb_rating


def put_tmdb_id_and_poster_path_into_row(movie_id, row, links):
    tmdbid = links.loc[links['movieId'] == movie_id]['tmdbId'].iloc[0]
    url = f"https://api.themoviedb.org/3/movie/{tmdbid}"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_API_KEY}"
    }

    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            row['tmdbId'] = tmdbid

            data = response.json()
            row['poster_path'] = data.get("poster_path")

    except Exception as e:
        print(f"Erro ao buscar {tmdbid}: {e}")
        row['tmdbId'] = 0
        
def transform_genres_into_list_in_row(row):
    row['genres'] = row['genres'].split("|")

def write_action_line_in_output_file(outfile, movie_id):
    action = { "index": { "_index": INDEX_NAME, "_id": movie_id} }
    outfile.write(json.dumps(action) + '\n')

def write_item_line_in_output_file(outfile, movie_id, row, tags, links):
    put_tags_into_row(movie_id, row, tags)
    put_tmdb_id_and_poster_path_into_row(movie_id, row, links)
    transform_genres_into_list_in_row(row)

    row_dict_version = row.to_dict()

    outfile.write(json.dumps(row_dict_version) + '\n')

def write_pair_of_lines_in_output_file(outfile, movie_id, row, tags, links):
    write_action_line_in_output_file(outfile, movie_id)
    write_item_line_in_output_file(outfile, movie_id, row, tags, links)

def generate_complete_movies_ndjson():
    movies, tags = read_movies_and_tags()
    links = read_links()

    with open(OUTPUT_FILE, "w") as outfile:
        for (index, row) in movies.iterrows():         
            movie_id = row['movieId']

            write_pair_of_lines_in_output_file(outfile, movie_id, row, tags, links)

def add_imdb_rating_on_movies_ndjson():
    ratings = read_ratings()

    avg_rating = calculate_avg_rating(ratings)

    with open(OUTPUT_FILE, "r+", encoding="utf-8") as f:
        lines = f.readlines()
        f.seek(0)
        
        for i, linha in enumerate(lines):
            if i % 2 == 1:
                try:
                    data = json.loads(linha)
                    data["imdbrating"] = calculate_imdb_rating(data['movieId'], ratings, avg_rating)

                    f.write(json.dumps(data, ensure_ascii=False) + "\n")
                except json.JSONDecodeError:
                    f.write(linha)
            else:
                f.write(linha)

        f.truncate()

if __name__ == "__main__":
    # generate_complete_movies_ndjson()
    add_imdb_rating_on_movies_ndjson()
