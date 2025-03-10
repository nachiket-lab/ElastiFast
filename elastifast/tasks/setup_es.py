from elasticsearch import NotFoundError

from elastifast.config.logging import logger
from elastifast.models.elasticsearch import ElasticsearchClient

es = ElasticsearchClient().client


def ensure_pipeline(unique_id):
    ingest_pipeline = {
        "logs-celery.results": {
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
        },
        "logs-celery.logs": {
            "description": "A pipeline to parse the JSON logs generated by the ECS mapper",
            "processors": [
                {
                "json": {
                    "field": "message",
                    "add_to_root": True,
                    "if": "ctx?.message != null",
                    "on_failure": [
                    {
                        "set": {
                        "field": "tags",
                        "value": "json_parse_failure"
                        }
                    }
                    ]
                }
                },
                {
                "json": {
                    "field": "log",
                    "add_to_root": True,
                    "if": "ctx?.log != null",
                    "on_failure": [
                    {
                        "set": {
                        "field": "tags",
                        "value": "json_parse_failure"
                        }
                    }
                    ]
                }
                }
            ],
            "on_failure": [
                {
                "set": {
                    "field": "error.message",
                    "value": "Processor \"{{ _ingest.on_failure_processor_type }}\" with tag \"{{ _ingest.on_failure_processor_tag }}\" in pipeline \"{{ _ingest.on_failure_pipeline }}\" failed with message \"{{ _ingest.on_failure_message }}\""
                }
                }
            ]
        },
    }
    try:
        es.ingest.get_pipeline(id=unique_id)
    except NotFoundError as e:
        logger.info(f"Pipeline with {unique_id} not found. Creating new pipeline.")
        try:
            es.ingest.put_pipeline(id=unique_id, body=ingest_pipeline[unique_id])
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
    except Exception as e:
        logger.error(f"Error checking/creating pipeline: {e}")


def ensure_index_template(unique_id, index_patterns):
    index_template = {
        "logs-celery.results": {
            "priority": 201,
            "template": {
                "settings": {"index": {"default_pipeline": unique_id}},
                "mappings": {"properties": {"result": {"type": "flattened"}}},
            },
            "index_patterns": index_patterns,
            "data_stream": {"hidden": False, "allow_custom_routing": False},
            "composed_of": ["ecs@mappings"],
            "allow_auto_create": True,
        },
        "logs-celery.logs": {
            "priority": 201,
            "template": {
                "settings": {
                "index": {
                    "default_pipeline": unique_id
                }
                }
            },
            "index_patterns": index_patterns,
            "data_stream": {
                "hidden": False,
                "allow_custom_routing": False
            },
            "composed_of": [],
            "ignore_missing_component_templates": [],
            "allow_auto_create": True
        }
    }
    try:
        if not es.indices.exists_template(name=unique_id):
            es.indices.put_index_template(name=unique_id, body=index_template[unique_id])
            logger.info(f"Index template '{unique_id}' created.")
        else:
            logger.debug(f"Index template '{unique_id}' already exists.")
    except Exception as e:
        print(f"Error checking/creating index template: {e}")


def ensure_es_deps(unique_id, index_patterns):
    ensure_pipeline(unique_id)
    ensure_index_template(
        unique_id=unique_id,
        index_patterns=index_patterns,
    )
