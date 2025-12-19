from deps import es
import requests
import asyncio

MOVIES_INDEX_NAME = "movies"
BASE_URL = "http://es01:9200"

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
    with open("elastic_module/movies_with_tags.ndjson", "rb") as f:
        data = f.read()

    try:
        es.bulk(body=data)

        return True
    except Exception as e:
        print("excp: ", e)
        return False

async def dump_movies_on_startup():
    # tenta algumas vezes, em caso de ES ainda iniciando
    for i in range(10):
        try:
            if requests.get(BASE_URL).ok:
                break
        except requests.RequestException:
            pass

        if i == 9:
            break

        await asyncio.sleep(3)
    try:
        if not movies_index_exists():
            created = create_movies_index()
            if not created:
                print("Falha ao criar índice movies.")            

            ok = dump_movies_into_index()
            print("Dump NDJSON:", ok)
    except requests.RequestException as e:
        print("Erro ao tentar criar índice: ", e)
