from venv import logger
import ecs_logging
import logging

def create_ecs_logger():
    """
    Creates a logger that logs messages in ECS format (https://www.elastic.co/guide/en/ecs/current/index.html).

    Returns:
        logging.Logger: The ecs logger.
    """
    alogger = logging.getLogger(__name__)
    alogger.setLevel(logging.INFO)

    # Configure the logger to use ECS formatter
    handler = logging.StreamHandler()
    handler.setFormatter(ecs_logging.StdlibFormatter())
    alogger.addHandler(handler)

    return alogger

logger = create_ecs_logger()