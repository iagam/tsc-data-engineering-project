import os
from dotenv import load_dotenv
import yaml


def load_config():
    load_dotenv()

    with open("config/config.yml", "r") as f:
        config = yaml.safe_load(f)

    config["service_account_path"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

    return config
