from fastapi import FastAPI
from es_connection import es

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "ok"}

# @app.get("/animes/{title}")
# def search_anime(title: str):
#     resp = es.search(
#         index="animes",
#         query={"match": {"title": title}}
#     )
#     return resp
