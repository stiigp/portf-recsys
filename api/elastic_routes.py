from fastapi import APIRouter, HTTPException, status
from elastic_module.dump_movies_into_es import reset_movies_index

elastic_router = APIRouter()

@elastic_router.get("/")
def root():
    return {"ES routes on"}

@elastic_router.get("/reset-movies-index")
def reset_movies_index_api():
    resp = reset_movies_index()

    if not resp:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )
    
    return resp
