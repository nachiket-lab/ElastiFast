from fastapi.responses import JSONResponse
from elastifast import logger, app
from tasks import ingest_data_to_elasticsearch
from elastifast.models.elasticsearch import ElasticsearchClient
from typing import Dict, Any
from fastapi import Response, status
from elasticsearch.exceptions import ConnectionError, NotFoundError, RequestError, TransportError

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
        return (dict(res))
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