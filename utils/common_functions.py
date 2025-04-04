import os

import pandas as pd

from src.logger import get_logger
from src.custom_exception import CustomException
import yaml

logger = get_logger(__name__)


def read_yaml(filepath: str):
    try:
        if not os.path.exists(filepath):
            raise FileNotFoundError(f"File is not in path {filepath}")
        with open(filepath, "r") as yaml_file:
            config = yaml.safe_load(yaml_file)
            logger.info(f"Successfully read the yaml file at {filepath}")
            return config
    except Exception as e:
        logger.error(f"Error while reading yaml file at {filepath}")
        raise CustomException("Failed to read YAML file", e)