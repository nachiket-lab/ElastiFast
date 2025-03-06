import re
import sys
from pydoc import cli

from annotated_types import T
import elasticapm
from celery import Celery, current_task, shared_task
from celery.schedules import crontab
from celery.signals import after_setup_logger
from elasticsearch.exceptions import (ConnectionError, ConnectionTimeout,
                                      TransportError)
import ecs_logging
from elastifast.config.setting import settings
from elastifast.config.logging import logger
from elastifast.models.elasticsearch import ElasticsearchClient
from elastifast.tasks.atlassian import AtlassianAPIClient
from elastifast.tasks.ingest_es import ElasticsearchIngestData
from elastifast.tasks.jira import JiraAuditLogIngestor
from elastifast.tasks.postman import PostmanAuditLogIngestor
from elastifast.tasks.setup_es import ensure_es_deps
from elastifast.tasks.zendesk import ZendeskAuditLogIngestor

esclient = ElasticsearchClient().client

# client = settings.apm_client
if any("worker" in s for s in sys.argv):
    client = settings.apm_client
else:
    pass

# Create a Celery app
celery_app = Celery(
    "ElastiFast",
    broker=str(settings.celery_broker_url),
    backend=str(settings.celery_result_backend),
    broker_transport_options=settings.celery_broker_transport_options,
)
namespace = "default"

if settings.celery_beat_schedule is True:
    celery_app.conf.beat_schedule = {
        "ingest_data_from_atlassian": {
            "task": "elastifast.tasks.ingest_data_from_atlassian",
            "schedule": crontab(minute=f"*/{settings.celery_beat_interval}"),
            "args": (settings.celery_beat_schedule, namespace),
        },
        "ingest_data_from_jira": {
            "task": "elastifast.tasks.ingest_data_from_jira",
            "schedule": crontab(minute=f"*/{settings.celery_beat_interval}"),
            "args": (settings.celery_beat_schedule, namespace),
        },
        "ingest_data_from_zendesk": {
            "task": "elastifast.tasks.ingest_data_from_zendesk",
            "schedule": crontab(minute=f"*/{settings.celery_beat_interval}"),
            "args": (settings.celery_beat_schedule, namespace),
        },
        "ingest_data_from_postman": {
            "task": "elastifast.tasks.ingest_data_from_postman",
            "schedule": crontab(minute=f"*/{settings.celery_beat_interval}"),
            "args": (settings.celery_beat_schedule, namespace),
        },
    }


@after_setup_logger.connect
def setup_task_logger(logger, *args, **kwargs):
    for handler in logger.handlers:
        handler.setFormatter(ecs_logging.StdlibFormatter())


@celery_app.on_after_configure.connect
def setup_tasks(sender, **kwargs):
    celery_index_name = "logs-celery.results"
    celery_index_patterns = ["logs-celery.results-*"]
    celery_logs_index_name = "logs-celery.logs"
    celery_logs_index_patterns = ["logs-celery.beat-*", "logs-celery.fastapi-*", "logs-celery.worker-*"]
    ensure_es_deps(
        unique_id=celery_index_name,
        index_patterns=celery_index_patterns,
    )
    ensure_es_deps(
        unique_id=celery_logs_index_name,
        index_patterns=celery_logs_index_patterns,
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
            esclient=esclient, data=data, dataset=dataset, namespace=namespace
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
def ingest_data_from_atlassian(interval: int, namespace: str, dataset: str = "atlassian.admin"):
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
def ingest_data_from_jira(interval: int, namespace: str, dataset: str = "jira.audit"):
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


@shared_task(retry_backoff=True, max_retries=5)
def ingest_data_from_postman(interval: int, namespace: str, dataset: str = "postman.audit"):
    if settings.postman_secret_token is None:
        raise ValueError(
            "Postman credentials not found. Please set POSTMAN_SECRET_TOKEN variables."
        )
    client = PostmanAuditLogIngestor(
        secret_token=settings.postman_secret_token, interval=interval
    )
    try:
        client.get_events()
        res = common_output(data=client, object=True)
    except Exception as e:
        logger.error(
            f"Error of type {type(e)} occured while polling data from postman: {e}. Exiting now."
        )
    ingest_data_to_elasticsearch.delay(
        data=client.data, dataset=dataset, namespace=namespace
    )
    return res

@shared_task(retry_backoff=True, max_retries=5)
def ingest_data_from_zendesk(interval: int, namespace: str, dataset: str="zendesk.audit"):
    if settings.zendesk_username is None or settings.zendesk_api_key is None:
        raise ValueError(
            "Zendesk credentials not found. Please set ZENDESK_USERNAME and ZENDESK_API_KEY variables."
        )
    client = ZendeskAuditLogIngestor(
        interval=interval,
        username=settings.zendesk_username,
        api_key=settings.zendesk_api_key,
        tenant=settings.zendesk_tenant,
    )
    try:
        client.get_events()
        res = common_output(data=client, object=True)
    except Exception as e:
        logger.error(
            f"Error of type {type(e)} occured while polling data from zendesk: {e}. Exiting now."
        )
    ingest_data_to_elasticsearch.delay(
        data=client.data, dataset=dataset, namespace=namespace
    )
    return res
