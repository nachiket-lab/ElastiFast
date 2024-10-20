from fastapi import FastAPI
from elastifast.config.settings import load_settings, logger
from elasticapm.contrib.starlette import ElasticAPM

logger = logger
settings = load_settings()

app = FastAPI()

if (
    settings.elasticapm_service_name and
    settings.elasticapm_server_url and
    settings.elasticapm_secret_token
):
    try:
        # apm_client  = make_apm_client({
        #     "SERVICE_NAME": settings.elasticapm_service_name,
        #     "SERVER_URL": settings.elasticapm_server_url,
        #     "SECRET_TOKEN": settings.elasticapm_secret_token
        # })
        app.add_middleware(
            ElasticAPM,
            client=settings.apm_client
        )
        logger.info("ElasticAPM initialized")
    except Exception as e:
        logger.error(f"Error initializing ElasticAPM: {e}")
        raise
else:
    logger.info("ElasticAPM not initialized due to missing configuration values under elasticapm_*")