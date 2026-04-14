import requests
import time

def fetch_api_data(config: dict):
    """
    Fetches data from an API

    Args:
        config (dict): Configuration loaded from config.yml.

    Returns:
        dict: A JSON response if the request is successful.

    Raises:
        Exception: If the maximum number of retries is reached or a 500-level server error occurs.
    """

    url = config["api"]["url"]
    params = config["api"]["params"]
    retries = config["pipeline"]["max_retries"]
    delay = config["pipeline"]["retry_delay"]

    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                return response.json()

            elif response.status_code >= 500:
                raise Exception("Internal Server error")

        except Exception as e:
            print(f"Retry {i+1} failed: {e}")
            time.sleep(delay)

    raise Exception("API failed after retries")
