from celery import shared_task
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from models.database import Database
    from models.elasticsearch import ElasticsearchClient


@shared_task
def ingest_data_to_elasticsearch(data: dict, db: Database, es: ElasticsearchClient):
    # Use the db and es clients to ingest the data into Elasticsearch
    es.index(index="my_index", document=data)
