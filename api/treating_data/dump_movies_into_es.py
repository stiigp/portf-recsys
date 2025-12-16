import requests
import os

BASE_URL = "http://es01:9200"
MOVIES_INDEX_NAME = "movies"

def ndjson_file_has_been_generated():
    return os.path.exists('./movies_with_tags.ndjson')

def delete_movies_index():
    response = requests.delete(BASE_URL+"/"+MOVIES_INDEX_NAME)
    return response.ok

def movies_index_exists():
    response = requests.get(BASE_URL+"/"+MOVIES_INDEX_NAME)

    return response.ok

def create_movies_index():
    mapping = {
        "mappings": {
        "properties": {
            "movieId": { "type": "integer" },
            "title":   { "type": "text" },
            "genres":  { "type": "keyword" },
                "tags": {
                    "type": "text",
                    "fields": {
                    "keyword": { "type": "keyword" }
                    }
                }
            }
        }
    }

    response = requests.put(BASE_URL+"/"+MOVIES_INDEX_NAME, json=mapping)

    return response.ok

def create_movies_index_if_it_doesnt_exist():
    if not movies_index_exists():
        return create_movies_index()
    
    return False

def dump_movies_into_index():
    with open("./movies_with_tags.ndjson", "rb") as f:
        data = f.read()

    headers = {
        "Content-Type": "application/x-ndjson"
    }

    resp = requests.post(f"{BASE_URL}/_bulk", headers=headers, data=data)

    return resp.ok

def dump_movies_into_index_if_file_exists():
    if movies_index_exists() and ndjson_file_has_been_generated():
        return dump_movies_into_index()
    return False

def create_index_and_dumps_data_if_index_doesnt_exists():
    return create_movies_index_if_it_doesnt_exist() and dump_movies_into_index_if_file_exists()

def reset_movies_index():
    return delete_movies_index() and create_index_and_dumps_data_if_index_doesnt_exists()
