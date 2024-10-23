import datetime
import requests
from elastifast.app import logger
from elastifast.models.elasticsearch import ElasticsearchClient

def get_atlassian_url(org_id: str, time_delta: int, limit: int = 500):
    start_time_in_milliseconds, end_time_in_milliseconds = calculate_time_window(time_delta)
    url = f"https://api.atlassian.com/admin/v1/orgs/{org_id}/events?from={str(start_time_in_milliseconds)}&to={str(end_time_in_milliseconds)}&limit={str(limit)}"
    return url

def calculate_time_window(delta: int, time: datetime = datetime.datetime.now(datetime.timezone.utc)):
    time = time.replace(second=0, microsecond=0)
    start_time =  time - datetime.timedelta(minutes=delta*2)
    end_time = time - datetime.timedelta(minutes=delta)
    start_time_in_milliseconds = round(start_time.timestamp() * 1000)
    end_time_in_milliseconds = round(end_time.timestamp() * 1000)
    return (start_time_in_milliseconds, end_time_in_milliseconds)

def query_data_from_atlassian(url: str, secret_token: str):
    """query_data_from_atlassian _summary_

    Args:
        query (str): _description_
        start_time_in_milliseconds (int): _description_
        end_time_in_milliseconds (int): _description_
        limit (int, optional): _description_. Defaults to 10000.
    """
    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {secret_token}"
    }
        
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error querying data from Atlassian: {response.text}")
        return None
    

def get_atlassian_events(secret_token: str, org_id: str, time_delta: int = 5):
    """get_atlassian_events _summary_

    Args:
        time_delta (int, optional): _description_. Defaults to 5.
    """
    url = get_atlassian_url(org_id, time_delta)
    data = []
    while True:
        result = query_data_from_atlassian(url=url, secret_token=secret_token)
        if result is not None and "data" in result.keys():
            data += result["data"]
        else:
            logger.error("Data is empty or not found")
            raise ValueError("Data is empty or not found")
        if "next" in result["links"]:
            url = result["links"]["next"]
        else:
            break
    return data