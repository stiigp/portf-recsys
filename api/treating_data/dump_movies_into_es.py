from es_connection import es

MOVIES_INDEX_NAME = "movies"

def movies_index_exists():
    try:
        return es.indices.exists(index=MOVIES_INDEX_NAME)
    except:
        return False

def create_movies_index():
    mapping = {
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

    try:
        es.indices.create(index=MOVIES_INDEX_NAME)
        es.indices.put_mapping(index=MOVIES_INDEX_NAME, body=mapping)

        return True
    except Exception as e:
        print("excep: ", e)
        return False

def create_movies_index_if_it_doesnt_exist():
    if not movies_index_exists():
        return create_movies_index()
    
    return False

def dump_movies_into_index():
    with open("treating_data/movies_with_tags.ndjson", "rb") as f:
        data = f.read()

    try:
        es.bulk(body=data)

        return True
    except Exception as e:
        print("excp: ", e)
        return False
