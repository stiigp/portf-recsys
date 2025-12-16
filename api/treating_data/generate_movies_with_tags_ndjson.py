import pandas as pd
import json

OUTPUT_FILE = 'movies_with_tags.ndjson'
INDEX_NAME = 'movies'

def read_movies_and_tags():
    return pd.read_csv("../dataset/movies.csv"), pd.read_csv("../dataset/tags.csv")

def convert_row_to_dict(row):
    return row.to_dict()

def put_tags_into_row(movie_id, row, tags):
    row['tags'] = list(set([tag for tag in tags.loc[tags['movieId'] == movie_id]['tag']]))

def write_action_line_in_output_file(outfile, movie_id):
    action = { "index": { "_index": INDEX_NAME, "_id": movie_id} }
    outfile.write(json.dumps(action) + '\n')

def write_item_line_with_tags_in_output_file(outfile, movie_id, row, tags):
    put_tags_into_row(movie_id, row, tags)
    row_dict_version = convert_row_to_dict(row)

    outfile.write(json.dumps(row_dict_version) + '\n')

def write_pair_of_lines_in_output_file(outfile, movie_id, row, tags):
    write_action_line_in_output_file(outfile, movie_id)
    write_item_line_with_tags_in_output_file(outfile, movie_id, row, tags)

def generate_movies_with_tags_ndjson():
    movies, tags = read_movies_and_tags()

    with open(OUTPUT_FILE, "w") as outfile:
        for (index, row) in movies.iterrows():            
            movie_id = row['movieId']

            write_pair_of_lines_in_output_file(outfile, movie_id, row, tags)

if __name__ == "__main__":
    generate_movies_with_tags_ndjson()
