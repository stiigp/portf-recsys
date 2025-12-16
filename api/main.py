from fastapi import FastAPI
from es_connection import es
from contextlib import asynccontextmanager
from treating_data.dump_movies_into_es import dump_movies_into_index, create_movies_index, movies_index_exists
import requests
import asyncio

BASE_URL = "http://es01:9200"
MOVIES_INDEX_NAME = "movies"

@asynccontextmanager
async def lifespan(app: FastAPI):
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

    yield
    # aqui fica o código pro shutdown da API

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "ok"}
