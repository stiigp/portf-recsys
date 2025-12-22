import numpy as np
import pandas as pd
import implicit
import pickle
import os
from postgres_module.db import engine
from scipy.sparse import coo_matrix

def get_ratings():
    ratings = pd.read_sql("SELECT * FROM ratings", engine)
    return ratings

def load_user_ids_and_movie_ids_in_chunks():
    users = set()
    movies = set()

    for chunk in pd.read_sql("SELECT * FROM ratings", engine, chunksize=1_000_000):
        users.update(chunk["userId"].unique())
        movies.update(chunk["movieId"].unique())

    user_ids = np.array(list(users))
    movie_ids = np.array(list(movies))

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
        shape=(len(user_mapping.values()), len(movie_mapping.values()))
    ).tocsr()

    return user_item

def create_and_train_model(user_item, alpha: float):
    # float32 is used for saving memory purposes (half of float64 size)
    confidence = (user_item * alpha).astype("float32")
    model = implicit.als.AlternatingLeastSquares(
        factors=64,
        regularization=0.1,
        iterations=15,
    )

    model.fit(confidence.T)

    return model

def offline_processing():
    user_ids, movie_ids = load_user_ids_and_movie_ids_in_chunks()

    user_id_to_idx, movie_id_to_idx = make_user_and_movie_mappings(user_ids, movie_ids)

    user_item = make_sparse_matrix(user_id_to_idx, movie_id_to_idx)    

    trained_model = create_and_train_model(user_item=user_item, alpha=40.0)

    with open("models/als_model.pkl", "wb") as f:
        pickle.dump(trained_model, f)

if __name__ == "__main__":
    if not os.path.exists("models/als_model.pkl"):
        offline_processing()
        print("trained and dumped model")
