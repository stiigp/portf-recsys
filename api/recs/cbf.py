from deps import es

def get_similar_movies_cbf(movie_id: int):
    body = {
        "size": 20,
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
