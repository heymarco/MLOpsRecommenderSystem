import os

import dotenv
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv
from src.logger import get_logger
from src.custom_exception import CustomException
from config.paths_config import *
from utils.common_functions import read_yaml
from utils.constants import GOOGLE_APPLICATION_CREDENTIALS


load_dotenv()
logger = get_logger(__name__)


class DataIngestion:
    def __init__(self, config):
        self.config = config["data_ingestion"]
        self.bucket_name = self.config["bucket_name"]
        self.file_names = self.config["bucket_file_names"]

        os.makedirs(RAW_DIR, exist_ok=True)

        logger.info("Data ingestion started")

    def _download_csv_from_gcp(self):
        try:
            client = storage.Client()
            bucket = client.bucket(self.bucket_name)
            for file_name in self.file_names:
                file_path = os.path.join(RAW_DIR, file_name)
                blob = bucket.blob(file_name)
                blob.download_to_filename(file_path)
                if file_name == "animelist.csv":
                    data = pd.read_csv(file_path, nrows=5_000_000)
                else:
                    data = pd.read_csv(file_path)
                filename_wo_ext, _ = os.path.splitext(file_name)
                storage_path = os.path.join(RAW_DIR, filename_wo_ext + ".parquet")
                data.to_parquet(storage_path, index=False)
                os.remove(file_path)  # remove the downloaded file since we have now stored it as parquet
                logger.info(f"Download of {filename_wo_ext} completed")
        except Exception as e:
            logger.error("Error downloading files from bucket")
            raise CustomException("Downloading files from bucket failed", e)

    def run(self):
        try:
            logger.info("Starting data ingestion")
            self._download_csv_from_gcp()
            logger.info("Data ingestion completed")
        except CustomException as e:
            logger.error(f"CustomException: {str(e)}")
        finally:
            logger.info("Data ingestion done")


if __name__ == '__main__':
    config = read_yaml(CONFIG_PATH)
    data_ingestion = DataIngestion(config)
    data_ingestion.run()
