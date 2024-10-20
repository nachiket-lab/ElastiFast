from time import sleep
import os, sys
from celery import shared_task
from celery import Celery
from elasticapm.contrib.celery import register_exception_tracking, register_instrumentation
from elastifast.app import settings, logger
from elastifast.models.elasticsearch import ElasticsearchClient

esclient = ElasticsearchClient()

if os.environ.get("CELERY_WORKER_RUNNING") is not None or 'worker' in sys.argv:
    register_instrumentation(settings.apm_client)
    register_exception_tracking(settings.apm_client)
    logger.info("ElasticAPM initialized in Celery worker")
else:
    logger.info("ElasticAPM initialized in non-worker mode")
# Register the Celery instrumentation
# register_instrumentation(settings.apm_client)
# register_exception_tracking(settings.apm_client)

# Create a Celery app
celery = Celery("NSE", broker=str(settings.celery_broker_url), backend=str(settings.celery_result_backend))

@shared_task
def ingest_data_to_elasticsearch(data: dict):
    # Use the db and es clients to ingest the data into Elasticsearch
    sleep(1)
    logger.info({"data": data})
