from time import sleep
import os, sys
from celery import shared_task, current_task
from celery import Celery
import elasticapm
from elasticsearch.exceptions import ConnectionError, ConnectionTimeout, TransportError
from elastifast.config import settings, logger
from elastifast.models.elasticsearch import ElasticsearchClient
from elastifast.tasks.atlassian import get_atlassian_events
from elastifast.tasks.setup_es import ensure_es_deps
from elastifast.tasks.ingest_es import index_data


esclient = ElasticsearchClient().client

#client = settings.apm_client
if any("worker" in s for s in sys.argv):
    client = settings.apm_client
else:
    pass

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


@shared_task(autoretry_for=(ConnectionError, TimeoutError, ConnectionTimeout, TransportError), retry_backoff=True, max_retries=5, bind=True)
def ingest_data_to_elasticsearch(self, data: dict, dataset: str, namespace: str):
    index_name = f"logs-{dataset}-{namespace}"
    try:
        res = index_data(esclient=esclient, data=data, index_name=index_name)
        return common_output({"message": str(res)})
    except (ConnectionError, TimeoutError, ConnectionTimeout, TransportError) as e:
        logger.info(f"Error of type {type(e)} occured. Retrying task, attempt number: {self.request.retries}/{self.max_retries}")
        raise
    except Exception as e:
        logger.error(f"Error of type {type(e)} occured while ingesting data: {e}. Exiting now.")

@shared_task(retry_backoff=True, max_retries=5)
def ingest_data_from_atlassian(interval: int, dataset: str, namespace: str):
    try:
        data = get_atlassian_events(
            time_delta=interval,
            secret_token=settings.atlassian_secret_token,
            org_id=settings.atlassian_org_id,
        )
        res = {"data": data, "message": f"Data ingested from Atlassian {len(data)} events"}
    except Exception as e:
        logger.error(f"Error of type {type(e)} occured while polling data from atlassian: {e}. Exiting now.")
    ingest_data_to_elasticsearch.delay(res)
    return common_output(res)
