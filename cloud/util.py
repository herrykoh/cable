from google.cloud import storage
from io import StringIO
import pandas as pd
import os
import time
import logging


class Bucket(object):

    def __init__(self, project, bucket_name):
        self._bucket_name = bucket_name
        self._client = storage.Client(project=project)
        self._bucket = self._client.get_bucket(bucket_name)

    def list(self, folder=None):
        return self._client.list_blobs(self._bucket_name, prefix=folder)

    def get_blob(self, path):
        return self._bucket.get_blob(path)

    def download_blob_as_text(self, blob_path) -> str:
        b = self.get_blob(blob_path)
        return b.download_as_text(encoding='utf-8') if b else None

    def read_csv_blob_as_dataframe(self, blob_path) -> pd.DataFrame:
        """
        Download the blob as a pandas dataframe.
        We make the assumption the blob is a CSV file
        :param blob_path: the path in the cloud to the CSV blob
        :return: a pandas dataframe or None
        """
        txt = self.download_blob_as_text(blob_path)
        return pd.read_csv(StringIO(txt)) if txt else None

    def get_bucket(self):
        return self._bucket


def load_or_download_blob(project, bucket_name, blob_path, file_path, refresh_after_hours=24,
                          force_download=False) -> pd.DataFrame:
    """
    Download the blob into the file if the file was last modified more than
    the specified hours ago
    :param project: name of the Google Cloud project
    :param bucket_name: name of the bucket
    :param blob_path: path of the blob, including the folder
    :param file_path: path of the local file to save
    :param refresh_after_hours: defaults to 24 hours
    :param force_download: disregard the refresh_after_hours and just download from cloud
    :return: the pandas dataframe for the csv file
    """
    cutoff_seconds = time.time() - refresh_after_hours * 60 * 60

    # if file doesn't exist or older than cutoff time, reload.
    if force_download or not os.path.exists(file_path) or os.path.getmtime(file_path) < cutoff_seconds:
        logging.log(logging.INFO, f"Downloading {blob_path} from cloud")
        bucket = Bucket(project, bucket_name)
        t = bucket.download_blob_as_text(blob_path)
        with open(file_path, "w") as f:
            f.write(t)

        return pd.read_csv(StringIO(t))

    logging.info(f"Loading file from {file_path}")
    return pd.read_csv(file_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    refreshed = load_or_download_blob(project="data-attic",
                                      bucket_name="ev-chargers-opencharge",
                                      blob_path="analysis/opencharge.csv",
                                      file_path="../ev/data/analysis.csv",
                                      force_download=False)

    print(f"Got {len(refreshed)} rows")


