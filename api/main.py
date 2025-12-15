from fastapi import FastAPI
from elasticsearch import Elasticsearch

es = Elasticsearch("http://es01:9200")

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
