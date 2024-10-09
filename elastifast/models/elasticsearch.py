from elasticsearch import Elasticsearch


class ElasticsearchClient:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.client = Elasticsearch(hosts=[f"{host}:{port}"])
