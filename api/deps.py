from postgres_module.db import SessionLocal
from sqlalchemy.orm import Session
from contextlib import contextmanager
from elasticsearch import Elasticsearch

ELASTICSEARCH_HOST = "http://es01:9200"

@contextmanager
def get_db_context():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

es = Elasticsearch(
    ELASTICSEARCH_HOST
)
