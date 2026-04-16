import requests
import time


def fetch_api_data(url: str, params: dict, retries: int = 2, delay: int = 5):
    """
    Fetches data from an API

    Args:
        url (str): API url to be loaded from config.yml
        params (dict): Params dict
        retries (int): Number of retries, default 2
        delay (int): Delay in seconds, default 5

    Returns:
        dict: A JSON response if the request is successful.

    Raises:
        Exception: If the maximum number of retries is reached or a 500-level server error occurs.
    """

    last_exception = None

    for i in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)

            if response.status_code == 200:
                return response.json()

            elif response.status_code >= 500:
                raise Exception("Internal Server error")

        except Exception as e:
            last_exception = e
            print(f"Retry {i+1} failed: {e}")
            time.sleep(delay)

    raise Exception(f"API failed after retries: {str(last_exception)}")
