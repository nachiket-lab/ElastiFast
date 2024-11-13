from elasticsearch.helpers import BulkIndexError, bulk

from elastifast.config import logger


def index_data(esclient, data: list, index_name: str):
    for item in data:
        item["_index"] = index_name
        item["_op_type"] = "create"
    try:
        res = bulk(esclient, data)
    except BulkIndexError as e:
        logger.error(f"Indexing error while ingesting data: {e.errors}.")
        raise
    except Exception as e:
        logger.error(f"Error of type {type(e)} occured while ingesting data: {e}.")
        raise
    return {"ingested_events": {"success": res[0], "failure": res[1]}}
