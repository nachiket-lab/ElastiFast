import re
import sys
from pydoc import cli

import ecs_logging
import elasticapm
from celery import Celery, current_task, shared_task
from celery.signals import after_setup_logger
from elasticsearch.exceptions import (ConnectionError, ConnectionTimeout,
                                      TransportError)

from elastifast.config import logger, settings
from elastifast.models.elasticsearch import ElasticsearchClient
from elastifast.tasks.atlassian import AtlassianAPIClient
from elastifast.tasks.ingest_es import ElasticsearchIngestData
from elastifast.tasks.jira import JiraAuditLogIngestor
from elastifast.tasks.setup_es import ensure_es_deps

esclient = ElasticsearchClient().client

# client = settings.apm_client
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


@after_setup_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    for handler in logger.handlers:
        handler.setFormatter(ecs_logging.StdlibFormatter())


@celery_app.on_after_configure.connect
def setup_tasks(sender, **kwargs):
    ensure_es_deps(
        pipeline_id=settings.celery_index_name,
        template_name=settings.celery_index_name,
        index_patterns=settings.celery_index_patterns,
    )


def common_output(data, object=False):
    if object:
        _d = {"class": data.__class__.__name__, "message": data.message}
    elif type(data) == dict and object is not True:
        _d = {
            "message": data.get("message"),
            **{k: v for k, v in data.items() if k != "message"},
        }
    else:
        _d = {}
    d = {
        "transaction": {
            "id": elasticapm.get_transaction_id(),
        },
        "trace": {
            "id": elasticapm.get_trace_id(),
            "name": current_task.name,
        },
        **_d,
    }
    return d


@shared_task(
    autoretry_for=(ConnectionError, TimeoutError, ConnectionTimeout, TransportError),
    retry_backoff=True,
    max_retries=5,
    bind=True,
)
def ingest_data_to_elasticsearch(self, data: dict, dataset: str, namespace: str):
    index_name = f"logs-{dataset}-{namespace}"
    try:
        client = ElasticsearchIngestData(
            esclient=esclient, data=data, index_name=index_name
        )
        return common_output(data=client, object=True)
    except (ConnectionError, TimeoutError, ConnectionTimeout, TransportError) as e:
        logger.info(
            f"Error of type {type(e)} occured. Retrying task, attempt number: {self.request.retries}/{self.max_retries}"
        )
        raise
    except Exception as e:
        logger.error(
            f"Error of type {type(e)} occured while ingesting data: {e}. Exiting now."
        )
        raise


@shared_task(retry_backoff=True, max_retries=5)
def ingest_data_from_atlassian(interval: int, dataset: str, namespace: str):
    if settings.atlassian_org_id is None or settings.atlassian_secret_token is None:
        raise ValueError(
            "Atlassian credentials not found. Please set ATLASSIAN_ORG_ID and ATLASSIAN_SECRET_TOKEN variables."
        )
    client = AtlassianAPIClient(
        org_id=settings.atlassian_org_id,
        secret_token=settings.atlassian_secret_token,
        interval=interval,
    )
    try:
        client.get_events()
        res = common_output(data=client, object=True)
    except Exception as e:
        logger.error(
            f"Error of type {type(e)} occured while polling data from atlassian: {e}. Exiting now."
        )
    ingest_data_to_elasticsearch.delay(
        data=client.data, dataset=dataset, namespace=namespace
    )
    return res


@shared_task(retry_backoff=True, max_retries=5)
def ingest_data_from_jira(interval: int, dataset: str, namespace: str):
    if (
        settings.jira_url is None
        or settings.jira_username is None
        or settings.jira_api_key is None
    ):
        raise ValueError(
            "Jira credentials not found. Please set JIRA_ORG_ID, JIRA_USERNAME and JIRA_API_KEY variables."
        )
    client = JiraAuditLogIngestor(
        interval=interval,
        url=settings.jira_url,
        username=settings.jira_username,
        password=settings.jira_api_key,
    )
    try:
        client.get_events()
        res = common_output(data=client, object=True)
    except Exception as e:
        logger.error(
            f"Error of type {type(e)} occured while polling data from jira: {e}. Exiting now."
        )
    ingest_data_to_elasticsearch.delay(
        data=client.data, dataset=dataset, namespace=namespace
    )
    return res