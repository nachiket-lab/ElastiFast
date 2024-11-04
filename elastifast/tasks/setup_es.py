from elastifast.app import logger
from elasticsearch import NotFoundError
from elastifast.models.elasticsearch import ElasticsearchClient

es = ElasticsearchClient().client


def ensure_pipeline(pipeline_id):
    ingest_pipeline = {
        "description": "A pipeline to filter the celery results",
        "processors": [
            {
                "rename": {
                    "field": "result",
                    "target_field": "event.original",
                    "ignore_missing": True,
                }
            },
            {"json": {"field": "event.original", "target_field": "_temp"}},
            {
                "set": {
                    "field": "result",
                    "ignore_empty_value": True,
                    "copy_from": "_temp.result",
                }
            },
            {
                "rename": {
                    "field": "result.message",
                    "target_field": "message",
                    "ignore_missing": True,
                }
            },
            {
                "rename": {
                    "field": "result.trace",
                    "target_field": "trace",
                    "ignore_missing": True,
                }
            },
            {
                "rename": {
                    "field": "result.transaction",
                    "target_field": "transaction",
                    "ignore_missing": True,
                }
            },
            {
                "set": {
                    "field": "traceback",
                    "copy_from": "_temp.traceback",
                    "ignore_empty_value": True,
                }
            },
            {
                "set": {
                    "field": "event.outcome",
                    "copy_from": "_temp.status",
                    "ignore_empty_value": True,
                }
            },
            {"lowercase": {"field": "event.outcome", "ignore_missing": True}},
            {
                "set": {
                    "field": "event.id",
                    "copy_from": "_temp.task_id",
                    "ignore_empty_value": True,
                }
            },
            {
                "set": {
                    "field": "event.created",
                    "ignore_empty_value": True,
                    "copy_from": "_temp.date_done",
                }
            },
            {
                "date": {
                    "field": "event.created",
                    "formats": ["ISO8601"],
                    "target_field": "event.created",
                }
            },
            {"remove": {"field": ["_temp", "result.message"], "ignore_missing": True}},
        ],
    }
    try:
        es.ingest.get_pipeline(id=pipeline_id)
    except NotFoundError as e:
        logger.info(f"Pipeline with {pipeline_id} not found. Creating new pipeline.")
        try: 
            es.ingest.put_pipeline(id=pipeline_id, body=ingest_pipeline)
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
    except Exception as e:
        logger.error(f"Error checking/creating pipeline: {e}")


def ensure_index_template(template_name, pipeline_id, index_patterns):
    index_template = {
        "priority": 200,
        "template": {
            "settings": {"index": {"default_pipeline": pipeline_id}},
            "mappings": {"properties": {"result": {"type": "flattened"}}},
        },
        "index_patterns": index_patterns,
        "data_stream": {"hidden": False, "allow_custom_routing": False},
        "composed_of": ["ecs@mappings"],
        "allow_auto_create": True,
    }
    try:
        if not es.indices.exists_template(name=template_name):
            es.indices.put_index_template(name=template_name, body=index_template)
            logger.info(f"Index template '{template_name}' created.")
        else:
            logger.debug(f"Index template '{template_name}' already exists.")
    except Exception as e:
        print(f"Error checking/creating index template: {e}")

def ensure_es_deps(template_name, pipeline_id, index_patterns):
    ensure_pipeline(pipeline_id)
    ensure_index_template(template_name=template_name, pipeline_id=pipeline_id, index_patterns=index_patterns)
