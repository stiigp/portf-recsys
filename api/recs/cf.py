import numpy as np
import pandas as pd
import implicit
import pickle
import os
from postgres_module.db import engine
from scipy.sparse import coo_matrix
from implicit.als import AlternatingLeastSquares

def get_ratings():
    ratings = pd.read_sql("SELECT * FROM ratings", engine)
    return ratings

def load_user_ids_and_movie_ids_in_chunks():
    users = set()
    movies = set()

    for chunk in pd.read_sql("SELECT * FROM ratings", engine, chunksize=1_000_000):
        users.update(chunk["userId"].unique())
        movies.update(chunk["movieId"].unique())

    user_ids = np.array(sorted(users))
    movie_ids = np.array(sorted(movies))

    return user_ids, movie_ids

def make_user_and_movie_mappings(user_ids, movie_ids):
    user_id_to_idx = {u: i for i, u in enumerate(user_ids)}
    movie_id_to_idx = {m: i for i, m in enumerate(movie_ids)}

    return user_id_to_idx, movie_id_to_idx

def make_sparse_matrix(user_mapping: dict, movie_mapping: dict):
    rows = []
    cols = []
    data = []

    for chunk in pd.read_sql("SELECT * FROM ratings", engine, chunksize=1_000_000):
        rows.extend(chunk["userId"].map(user_mapping))
        cols.extend(chunk["movieId"].map(movie_mapping))
        data.extend(chunk["rating"].astype(np.float32))

    user_item = coo_matrix(
        (data, (rows, cols)),
        shape=(len(user_mapping), len(movie_mapping))
    ).tocsr()

    return user_item

def create_and_train_model(user_item, alpha: float):
    confidence = (user_item * alpha).astype("float32")
    model = implicit.als.AlternatingLeastSquares(
        factors=64,
        regularization=0.1,
        iterations=15,
    )

    model.fit(confidence)

    return model

def offline_processing():
    user_ids, movie_ids = load_user_ids_and_movie_ids_in_chunks()

    user_id_to_idx, movie_id_to_idx = make_user_and_movie_mappings(user_ids, movie_ids)

    user_item = make_sparse_matrix(user_id_to_idx, movie_id_to_idx)

    trained_model = create_and_train_model(user_item=user_item, alpha=40.0)

    print("n_items model:", trained_model.item_factors.shape[0])
    print("n_movies array:", len(movie_ids))

    with open("models/als_model.pkl", "wb") as f:
        pickle.dump(trained_model, f)
    
    with open("models/movie_id_to_idx.pkl", "wb") as f:
        pickle.dump(movie_id_to_idx, f)
    
    np.save("models/movie_ids.npy", np.array(movie_ids, dtype=np.int32))
    

def train_and_dump_model_on_startup():
    if not os.path.exists("models/als_model.pkl"):
        offline_processing()
        print("trained and dumped model")

def load_model_on_startup() -> AlternatingLeastSquares:
    if os.path.exists("models/als_model.pkl"):
        with open("models/als_model.pkl", "rb") as f:
            model: AlternatingLeastSquares = pickle.load(f)
            return model
    else:
        print("als model file not found")

def load_movie_ids_on_startup() -> np.array:
    if os.path.exists("models/movie_ids.npy"):
        return np.load("models/movie_ids.npy")
    else:
        print("movie_ids array not found")

def load_movie_mapping_on_startup() -> dict:
    if os.path.exists("models/movie_id_to_idx.pkl"):
        with open("models/movie_id_to_idx.pkl", "rb") as f:
            mapping: dict = pickle.load(f)
            return mapping
    else:
        print("movie mapping not found")

if __name__ == "__main__":
    train_and_dump_model_on_startup()
