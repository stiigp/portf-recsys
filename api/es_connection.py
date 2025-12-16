from elasticsearch import Elasticsearch

ELASTICSEARCH_HOST = "http://es01:9200"

es = Elasticsearch(
    ELASTICSEARCH_HOST
)