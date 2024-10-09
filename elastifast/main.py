from settings import load_settings, app

# from models.database import Database
from models.elasticsearch import ElasticsearchClient
from services.celery_tasks import ingest_data_to_elasticsearch

# Load settings
settings = load_settings()

# Create database and Elasticsearch clients using the loaded settings
# db = Database(
#     host=settings.db_host,
#     port=settings.db_port,
#     username=settings.db_username,
#     password=settings.db_password,
# )
es = ElasticsearchClient(
    host=settings.elasticsearch_host,
    port=settings.elasticsearch_port,
)

# Create a Celery app and configure it to use the loaded settings
celery = Celery("my_project", broker=settings.celery_broker_url)
celery.conf.result_backend = settings.celery_result_backend


# Define a FastAPI endpoint to trigger the Celery task
@app.post("/ingest_data")
async def ingest_data(data: dict):
    ingest_data_to_elasticsearch.apply_async(args=[data, db, es])
    return {"message": "Data ingestion task triggered"}


# Run the FastAPI app
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
