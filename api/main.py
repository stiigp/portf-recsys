from fastapi import FastAPI, HTTPException
from contextlib import asynccontextmanager
from elastic_module.dump_movies_into_es import dump_movies_on_startup
from postgres_module.dump_ratings_into_pg import dump_ratings_on_startup
from recs.cbf import get_similar_movies_cbf, get_cbf_score_of_movie_with_id
from deps import es
import recs.cf as cf
import os

MOVIES_INDEX_NAME = "movies"

@asynccontextmanager
async def lifespan(app: FastAPI):
    await dump_movies_on_startup()
    await dump_ratings_on_startup()
    cf.train_and_dump_model_on_startup()
    app.state.als_model = cf.load_model_on_startup()
    app.state.movie_ids = cf.load_movie_ids_on_startup()
    app.state.movie_mapping = cf.load_movie_mapping_on_startup()
    
    yield
    # aqui fica o código pro shutdown da API

app = FastAPI(lifespan=lifespan)

@app.get("/")
def read_root():
    return {"status": "ok"}

@app.get("/cbf/{movie_id}")
def cbf_rec(movie_id: int):
    similar_movies = get_similar_movies_cbf(movie_id=movie_id, n_movies=20)

    return similar_movies

@app.get("/cf/{movie_id}")
def cf_rec(movie_id: int):
    model = app.state.als_model
    mapping = app.state.movie_mapping
    movie_ids = app.state.movie_ids

    model_id = mapping[movie_id]

    ids, scores = model.similar_items(model_id, N=11)

    recs = []

    for item_id, score in zip(ids, scores):
        es_id = movie_ids[item_id]
        try:
            doc = es.get(index=MOVIES_INDEX_NAME, id=es_id)
            title = doc["_source"]["title"]
        except Exception as e:
            print("movie not found in ES")
            continue

        recs.append(
            {"movie_id": int(es_id), "title": title, "score": float(score)}
        )

    return {"recommendations": recs}


@app.get("/cf/check-model")
def train_model():
    if os.path.exists("recs/als_model.pkl"):
        return {"file_name": "als_model.pkl", "size_mb": os.path.getsize("recs/als_model.pkl")/1024/1024}
    
    raise HTTPException(
        status_code=404,
        detail="als model file not found"
    )

@app.get("/hb/{movie_id}")
def hybrid_rec(movie_id: int):
    model = app.state.als_model
    mapping = app.state.movie_mapping
    movie_ids = app.state.movie_ids

    model_id = mapping[movie_id]

    ids, scores = model.similar_items(model_id, N=200)

    similar_movies_cbf = get_similar_movies_cbf(movie_id=movie_id, n_movies=200)
    max_cbf_score = similar_movies_cbf['hits']['max_score']

    recs = []

    for item_id, score in zip(ids, scores):
        es_id = movie_ids[item_id]

        score_cbf_current_movie = get_cbf_score_of_movie_with_id(similar_movies_cbf, es_id)
        final_score = (score_cbf_current_movie / max_cbf_score) * 80 + score * 20

        try:
            doc = es.get(index=MOVIES_INDEX_NAME, id=es_id)
            title = doc["_source"]["title"]
        except Exception as e:
            print("movie not found in ES")
            continue

        recs.append(
            {"movie_id": int(es_id), "title": title, "score": float(final_score)}
        )
    
    recs = sorted(recs, key=lambda x: x['score'], reverse=True)

    return {"recommendations": recs}
