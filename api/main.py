from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from elastic_module.dump_movies_into_es import dump_movies_on_startup
from postgres_module.dump_ratings_into_pg import dump_ratings_on_startup
from recs.cbf import get_similar_movies_cbf
import recs.cf as cf
import os

BASE_URL = "http://es01:9200"
MOVIES_INDEX_NAME = "movies"

@asynccontextmanager
async def lifespan(app: FastAPI):
    await dump_movies_on_startup()
    await dump_ratings_on_startup()
    await cf.train_and_dump_model_on_startup()
    
    yield
    # aqui fica o código pro shutdown da API

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/cbf/{movie_id}")
def cbf(movie_id: int):
    similar_movies = get_similar_movies_cbf(movie_id=movie_id)

    return similar_movies

@app.get("/cf")
def get_all_ratings():
    ratings = cf.get_ratings()

    return ratings.describe()

@app.get("/cf/check-model")
def train_model():
    if os.path.exists("recs/als_model.pkl"):
        return {"file_name": "als_model.pkl", "size_mb": os.path.getsize("recs/als_model.pkl")/1024/1024}
    
    raise HTTPException(
        status_code=404,
        detail="als model file not found"
    )
