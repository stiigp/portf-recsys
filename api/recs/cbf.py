from deps import es

def get_similar_movies_cbf(movie_id: int, n_movies:int):
    body = {
        "size": n_movies,
        "query": {
            "function_score": {
                "query": {
                    "more_like_this": {
                        "fields": ["title", "genres", "tags"],
                        "like": [{ "_index": "movies", "_id": f"{movie_id}" }],
                        "min_term_freq": 1,
                        "max_query_terms": 25
                    }
                },
                "functions": [
                    {
                        "filter": { "exists": { "field": "imdbrating" } },
                        "field_value_factor": {
                            "field": "imdbrating",
                            "factor": 1.0,
                            "modifier": "sqrt",
                            "missing": 1.0
                        }
                    }
                ],
                "score_mode": "multiply",
                "boost_mode": "multiply"
            }
        }
    }

    resp = es.search(index="movies", body=body)

    return resp

def get_cbf_score_of_movie_with_id(similar_movies_cbf: list, es_id: int) -> float:
    similar_movies_cbf = similar_movies_cbf['hits']['hits']
    for movie in similar_movies_cbf:
        if movie['_source']['movieId'] == es_id:
            return movie['_score']
    
    return 0.0
