import pandas as pd
import json
import requests

OUTPUT_FILE = 'elastic_module/movies_with_tags.ndjson'
INDEX_NAME = 'movies'

def read_movies_and_tags():
    return pd.read_csv("dataset/movies_clean.csv"), pd.read_csv("dataset/tags.csv")

def read_links():
    return pd.read_csv("dataset/links_w_rt.csv")

def convert_row_to_dict(row, rating_medio):
    if row['imdbrating'] == rating_medio:
        del row['imdbrating']

    return row.to_dict()

def put_tags_into_row(movie_id, row, tags):
    row['tags'] = list(set([tag for tag in tags.loc[tags['movieId'] == movie_id]['tag']]))

def put_imdb_rating_into_row(movie_id, row, links):
    rating = links.loc[links['movieId'] == movie_id]['imdbrating'].iloc[0]
    row['imdbrating'] = rating

def transform_genres_into_list_in_row(row):
    row['genres'] = row['genres'].split("|")

def write_action_line_in_output_file(outfile, movie_id):
    action = { "index": { "_index": INDEX_NAME, "_id": movie_id} }
    outfile.write(json.dumps(action) + '\n')

def write_item_line_in_output_file(outfile, movie_id, row, tags, links):
    put_tags_into_row(movie_id, row, tags)
    put_imdb_rating_into_row(movie_id, row, links)
    transform_genres_into_list_in_row(row)

    row_dict_version = convert_row_to_dict(row, 6.155158089773474)

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

if __name__ == "__main__":
    generate_complete_movies_ndjson()
