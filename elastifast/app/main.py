from fastapi.responses import JSONResponse
from celery.result import AsyncResult
from elastifast.app import logger, settings
from elastifast.tasks import ingest_data_to_elasticsearch, ingest_data_from_atlassian
from elastifast.tasks.monitor import get_celery_tasks
from elastifast.models.elasticsearch import ElasticsearchClient
from typing import Dict, Any
from fastapi import Response, status, FastAPI, Query
from elasticsearch.exceptions import (
    ConnectionError,
    NotFoundError,
    RequestError,
    TransportError,
)
from elasticapm.contrib.starlette import ElasticAPM


app = FastAPI()

if (
    settings.elasticapm_service_name
    and settings.elasticapm_server_url
    and settings.elasticapm_secret_token
):
    try:
        # apm_client  = make_apm_client({
        #     "SERVICE_NAME": settings.elasticapm_service_name,
        #     "SERVER_URL": settings.elasticapm_server_url,
        #     "SECRET_TOKEN": settings.elasticapm_secret_token
        # })
        app.add_middleware(ElasticAPM, client=settings.apm_client)
        logger.info("ElasticAPM initialized")
    except Exception as e:
        logger.error(f"Error initializing ElasticAPM: {e}")
        raise
else:
    logger.info(
        "ElasticAPM not initialized due to missing configuration values under elasticapm_*"
    )


# Define a FastAPI endpoint to trigger the Celery task
@app.post("/ingest_data")
async def ingest_data(data: dict, response: Response):
    """
    Endpoint to trigger the data ingestion task.

    Args:
        data (dict): The data to be ingested.

    Returns:
        A dictionary containing a message indicating that the task has been triggered.
    """
    logger.info(f"Received data: {data}")
    if not data:
        logger.error("Data is null or empty")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": "Data is null or empty"}

    try:
        ingest_data_to_elasticsearch.delay(data)
        response.status_code = status.HTTP_202_ACCEPTED
        return {"message": "Data ingestion task triggered"}
    except Exception as e:
        logger.error(f"Error triggering task: {e}")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"error": f"Error triggering task: {e}"}


@app.get("/healthcheck", response_model=Dict[str, Any])
async def healthcheck(response: Response) -> Dict[str, Any]:
    """
    Health check endpoint to test the connection to Elasticsearch.

    Returns:
        A dictionary containing the health information of the Elasticsearch cluster.
    """
    logger.debug("Health check triggered")
    try:
        es = ElasticsearchClient()
        if es.client is None:
            logger.error("Elasticsearch client is null")
            response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            return {"error": "Elasticsearch client is null"}

        res = es.client.cluster.health()
        return dict(res)
    except ConnectionError as e:
        logger.error(f"Connection error: {e}")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"error": "Failed to connect to Elasticsearch."}
    except NotFoundError as e:
        logger.error(f"Not Found error: {e}")
        response.status_code = status.HTTP_404_NOT_FOUND
        return {"error": "Resource not found."}
    except RequestError as e:
        logger.error(f"Request error: {e}")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": "Bad request made to Elasticsearch."}
    except TransportError as e:
        logger.error(f"Transport error: {e}")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"error": "Transport error occurred in Elasticsearch."}
    except Exception as e:
        logger.error(f"Unexpected error: {e}, Response Object: {str(res)}")
        response.status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        return {"error": "An unexpected error occurred."}


@app.get("/tasks")
async def tasks(response: Response) -> Dict[str, Any]:
    """
    Endpoint to return the list of running tasks.

    Returns:
        A dictionary containing the list of running tasks.
    """
    return get_celery_tasks()


@app.get("/atlassian")
async def atlassian_data(
    response: Response,
    delta: int = Query(5, ge=0, le=360, description="Time delta in minutes (0 to 360)"),
) -> Dict[str, Any]:
    if (
        settings.atlassian_org_id is not None
        and settings.atlassian_secret_token is not None
    ):
        logger.debug("Atlassian credentials found")
        # Trigger the Celery task with the delta value
        task = ingest_data_from_atlassian.delay(delta)

        # Create an AsyncResult object to track the task
        task_result = AsyncResult(task.id)

        # Return the task details
        return {
            "task_id": task.id,
            "task_name": ingest_data_from_atlassian.name,  # Get the task name from the task itself
            "task_status": task_result.status,
            "task_result": task_result.result,  # This will be None if the task hasn't finished yet
        }
    else:
        logger.error("Atlassian credentials not found")
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"error": "Missing Atlassian credentials in settings.yaml"}
