import logging
import ecs_logging


def create_ecs_logger():
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    # Configure the logger to use ECS formatter
    handler = logging.StreamHandler()
    handler.setFormatter(ecs_logging.StdlibFormatter())
    logger.addHandler(handler)

    return logger


logger = create_ecs_logger()
