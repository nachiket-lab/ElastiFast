from elastifast import app
from tasks import ingest_data_to_elasticsearch
from elastifast.models.elasticsearch import ElasticsearchClient

# Define a FastAPI endpoint to trigger the Celery task
@app.post("/ingest_data")
async def ingest_data(data: dict):
    print(ElasticsearchClient().myname)
    ingest_data_to_elasticsearch.delay(data)
    return {"message": "Data ingestion task triggered"}