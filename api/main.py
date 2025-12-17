from fastapi import FastAPI
from contextlib import asynccontextmanager
from elastic_module.dump_movies_into_es import dump_movies_on_startup
from postgres_module.dump_ratings_into_pg import dump_ratings_on_startup

BASE_URL = "http://es01:9200"
MOVIES_INDEX_NAME = "movies"

@asynccontextmanager
async def lifespan(app: FastAPI):
    await dump_movies_on_startup()
    await dump_ratings_on_startup()
    
    yield
    # aqui fica o código pro shutdown da API

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "ok"}
