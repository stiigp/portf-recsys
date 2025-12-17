import pandas as pd
import os
from typing import List

def filtering_items_based_on_number_of_ratings(items: pd.DataFrame, ratings: pd.DataFrame, n: int) -> pd.DataFrame:
  ids_filmes_com_mais_de_n_ratings = []
  for (index, row) in items.iterrows():
    numero_avaliacoes = ratings.loc[ratings['movieId'] == row['movieId']].shape[0]

    if numero_avaliacoes > n:
      ids_filmes_com_mais_de_n_ratings.append(row['movieId'])
  
  items_filtrado = pd.DataFrame(columns=items.columns)
  for id in ids_filmes_com_mais_de_n_ratings:
    items_filtrado = pd.concat([items_filtrado, items.loc[items['movieId'] == id]])
  
  return items_filtrado

# versão otimizada utilizando funções do pandas
def filtering_items_based_on_number_of_ratings_otimizado(items: pd.DataFrame, ratings: pd.DataFrame, n: int) -> pd.DataFrame:
  numeros_de_ratings_para_cada_filme = ratings['movieId'].value_counts()

  ids_filmes_com_mais_de_n_ratings = numeros_de_ratings_para_cada_filme[numeros_de_ratings_para_cada_filme > n].index

  return items[items['movieId'].isin(ids_filmes_com_mais_de_n_ratings)]

# essa aqui filtra as avaliações
def filtering_ratings_based_on_items(items: pd.DataFrame, ratings: pd.DataFrame) -> pd.DataFrame:
    # print(ratings[ratings['movieId'].isin(items["movieId"])])
    rt_cln = ratings[ratings['movieId'].isin(items["movieId"])]

    rt_cln.drop('timestamp', axis=1, inplace=True)

    return rt_cln

def limpar_e_salvar_dados():
  movies = pd.read_csv('../dataset/movies.csv')
  ratings = pd.read_csv('../dataset/ratings.csv')

  movies = filtering_items_based_on_number_of_ratings_otimizado(movies, ratings, 5)
  ratings = filtering_ratings_based_on_items(movies, ratings)

  movies.to_csv('../dataset/movies_clean.csv', index=False)
  ratings.to_csv('../dataset/ratings_clean.csv', index=False)

def generate_rating_matrix():
  limpar_e_salvar_dados()
  ratings = pd.read_csv("ratings_clean.csv")

  user_movie_matrix = ratings.pivot_table(index="userId", columns="movieId", values="rating")

  user_movie_matrix.to_parquet("rating_matrix.parquet")

def load_rating_matrix() -> pd.DataFrame:
  return pd.read_parquet('rating_matrix.parquet').fillna(0)

def generate_user_similarity_matrix(rating_matrix: pd.DataFrame):
  similarity_matrix = rating_matrix.T.corr(method="pearson")

  similarity_matrix.to_parquet("similarities_between_users.parquet")

def load_user_similarity_matrix() -> pd.DataFrame:
  return pd.read_parquet('similarities_between_users.parquet')

def most_similar_users(userId: int, user_similarity_matrix: pd.DataFrame) -> List:
  most_similar_users_to_user = user_similarity_matrix.loc[userId].sort_values(ascending=False)

  return most_similar_users_to_user[1:6]

def best_rated_movies_from_x_that_y_hasnt_rated(user_x_id: int, user_y_id: int, n: int, rating_matrix: pd.DataFrame) -> List:
  res = []

  best_rated_movies_x = rating_matrix.loc[user_x_id].sort_values(ascending=False)

  for movie in best_rated_movies_x.index:
    if rating_matrix.loc[user_y_id][movie] == 0:
      res.append(movie)

  return res[:n]

def melhores_avaliacoes_medias_users_similares(user_id: int, user_similarity_matrix: pd.DataFrame):
  similar_users = most_similar_users(user_id, user_similarity_matrix=user_similarity_matrix)

  user_rating_matrix = load_rating_matrix()

  avaliacoes_dos_usuarios_similares = []
  for userId in similar_users.index:
    avaliacoes_dos_usuarios_similares.append(user_rating_matrix.loc[userId])
  
  df_avaliacoes_dos_users_similares = pd.concat(avaliacoes_dos_usuarios_similares, axis=1)

  df_avaliacoes_dos_users_similares = df_avaliacoes_dos_users_similares.mean(axis=1)
  itens_ja_avaliados = user_rating_matrix.loc[userId]
  itens_ja_avaliados = itens_ja_avaliados.loc[itens_ja_avaliados != 0].index
  df_avaliacoes_dos_users_similares = df_avaliacoes_dos_users_similares.drop(itens_ja_avaliados)

  df_avaliacoes_dos_users_similares = df_avaliacoes_dos_users_similares.sort_values(ascending=False)

  return df_avaliacoes_dos_users_similares[:5]

def generate_recommendations_user_based(userId, user_similarity_matrix: pd.DataFrame,) -> List:
  movies = pd.read_csv('../dataset/movies.csv')
  rec_titles = []

  recomendacoes = melhores_avaliacoes_medias_users_similares(userId, user_similarity_matrix=user_similarity_matrix)
  for movieId in recomendacoes.index:
    rec_titles.append(movies.loc[movies['movieId'] == movieId]['title'].iloc[0])
  
  return rec_titles

def generate_movie_similarity_matrix(rating_matrix: pd.DataFrame):
  similarity_matrix = rating_matrix.corr(method="pearson")

  similarity_matrix.to_parquet("similarities_between_movies.parquet")

def load_movie_similarity_matrix() -> pd.DataFrame:
    return pd.read_parquet('similarities_between_movies.parquet').fillna(0)

def generate_recommendations_item_based(movieId: int, movie_similarity_matrix: pd.DataFrame) -> List:
  similar_movies = movie_similarity_matrix.loc[movieId].sort_values(ascending=False)

  movies_df = pd.read_csv('../dataset/movies.csv')

  res = []

  for movieId in similar_movies.iloc[1:6].index:
    res.append((movieId, movies_df.loc[movies_df['movieId'] == movieId]['title'].iloc[0]))

  return res

def generate_recommendations_item_based_receiving_userId(userId: int, movie_similarity_matrix: pd.DataFrame, rating_matrix = pd.DataFrame) -> List:
  favorite_movies = rating_matrix.loc[userId].sort_values(ascending=False).index[0:5]
  movies_df = pd.read_csv('../dataset/movies.csv')

  recommendations = []

  for movieId in favorite_movies:
    for similar_movie in movie_similarity_matrix.loc[movieId].sort_values(ascending=False).index[1:]:
      if rating_matrix.loc[userId][similar_movie] == 0 and similar_movie not in recommendations:
        recommendations.append(int(similar_movie))
        print(movieId, similar_movie)
        break
    
    
  recommendations_with_title = []
  for rec in recommendations:
    recommendations_with_title.append((rec, movies_df.loc[movies_df['movieId'] == rec]['title'].iloc[0]))

  return recommendations_with_title


def get_parquet_file_size_in_MB(file_path):
  if os.path.exists(file_path):
      return os.path.getsize(file_path) / 1024 / 1024
  else:
      print(f"Error: File not found at {file_path}")
      return -1

if __name__ == "__main__":
  limpar_e_salvar_dados()

  rt_cln = pd.read_csv('ratings_clean.csv')
  print(rt_cln.columns)

  ratings = pd.read_csv('../dataset/ratings.csv')
  print(ratings.columns)
  # generate_rating_matrix()
  # rating_matrix = load_rating_matrix()

  # generate_movie_similarity_matrix(rating_matrix=rating_matrix)
  # generate_user_similarity_matrix(rating_matrix=rating_matrix)
