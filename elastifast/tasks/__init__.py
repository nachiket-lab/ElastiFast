from time import sleep
import os, sys
from celery import shared_task, current_task
from celery import Celery
from elasticapm.contrib.starlette import make_apm_client
from elasticapm.contrib.celery import (
    register_exception_tracking,
    register_instrumentation,
)
import elasticapm
from elastifast.config import settings, logger
from elastifast.models.elasticsearch import ElasticsearchClient
from elastifast.tasks.atlassian import get_atlassian_events
from elastifast.tasks.setup_es import ensure_es_deps


esclient = ElasticsearchClient().client

# Register the Celery instrumentation
if os.environ.get("CELERY_WORKER_RUNNING") is not None or "worker" in sys.argv:
    apm_client  = make_apm_client({
            "SERVICE_NAME": settings.elasticapm_service_name,
            "SERVER_URL": settings.elasticapm_server_url,
            "SECRET_TOKEN": settings.elasticapm_secret_token
        })
    register_instrumentation(apm_client)
    register_exception_tracking(apm_client)
    logger.info("ElasticAPM initialized in Celery worker")
else:
    logger.info("ElasticAPM initialized in non-worker mode")

# register_instrumentation(settings.apm_client)
# register_exception_tracking(settings.apm_client)

# Create a Celery app
celery_app = Celery(
    "NSE",
    broker=str(settings.celery_broker_url),
    backend=str(settings.celery_result_backend),
)


@celery_app.on_after_configure.connect
def setup_tasks(sender, **kwargs):
    ensure_es_deps(
        pipeline_id=settings.celery_index_name,
        template_name=settings.celery_index_name,
        index_patterns=settings.celery_index_patterns,
    )


def common_output(res):
    return {
        "message": res.get("message"),
        "transaction": {
            "id": elasticapm.get_transaction_id(),
        },
        "trace": {
            "id": elasticapm.get_trace_id(),
            "name": current_task.name,
        },
        **{k: v for k, v in res.items() if k != "message"},
    }


@shared_task
def ingest_data_to_elasticsearch(data: dict):
    # Use the db and es clients to ingest the data into Elasticsearch
    sleep(1)
    logger.info({"data": data, "esclient": esclient.info()})
    return {"data": data}


@shared_task
def ingest_data_from_atlassian(interval):
    data = get_atlassian_events(
        time_delta=interval,
        secret_token=settings.atlassian_secret_token,
        org_id=settings.atlassian_org_id,
    )
    res = {"data": data, "message": f"Data ingested from Atlassian {len(data)} events"}
    return common_output(res)
