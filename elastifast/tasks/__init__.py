from time import sleep
from celery import shared_task
from celery.app import Celery
from elastifast import settings
from elastifast.models.elasticsearch import ElasticsearchClient

esclient = ElasticsearchClient()

# Create a Celery app and configure it to use the loaded settings
celery = Celery("NSE", broker=str(settings.celery_broker_url), backend=str(settings.celery_result_backend))

@shared_task
def ingest_data_to_elasticsearch(data: dict):
    # Use the db and es clients to ingest the data into Elasticsearch
    sleep(1)
    print(esclient.myname, data)