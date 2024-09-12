import base64
import functions_framework
import pandas as pd
from google.cloud import storage
from io import StringIO
import logging
from datetime import datetime, date

PROJECT_NAME = "data-attic"
BUCKET_NAME = "ev-chargers-opencharge"

analysis_blob_name = "analysis/opencharge.csv"
loc_analysis_blob_name = 'analysis/by_loc.csv'


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
        return b.download_as_text(encoding='utf-8')

    def read_csv_blob_as_dataframe(self, blob_path) -> pd.DataFrame:
        """
        Download the blob as a pandas dataframe.
        We make the assumption the blob is a CSV file
        :param blob_path: the path in the cloud to the CSV blob
        :return: a pandas dataframe or None
        """
        b = self.get_blob(blob_path)
        return pd.read_csv(StringIO(b.download_as_text(encoding='utf-8'))) if b else None

    def get_bucket(self):
        return self._bucket

    def upload_blob(self, blobname: str, t: pd.DataFrame, content_type="text/plain"):
        self._bucket.blob(blobname).upload_from_string(t.to_csv(), content_type=content_type)


def new_locations_by_date(bucket_=None, limit=None) -> pd.DataFrame:
    bucket = Bucket(PROJECT_NAME, BUCKET_NAME) if not bucket_ else bucket_

    old_analysis = bucket.read_csv_blob_as_dataframe(loc_analysis_blob_name)
    has_old_analysis = old_analysis is not None and len(old_analysis) > 0
    if has_old_analysis:
        old_analysis['import_datestamp'] = pd.to_datetime(old_analysis['import_datestamp']).dt.date

    from_date = old_analysis['import_datestamp'].max() if has_old_analysis else date(2000, 1, 1)

    blob_list = list(bucket.list("data"))

    # sort the blobs by the datestamp on the filename
    sorted(blob_list, key=lambda b: b.name[-17:-4])

    if limit:
        blob_list = blob_list[:limit]

    previous_table = old_analysis

    for b in blob_list:
        blob_date = b.time_created.date()
        if blob_date <= from_date:
            continue

        t = pd.read_csv(StringIO(b.download_as_text()), index_col=0)
        t.sort_values(['operatorName'], inplace=True)
        t = t[t.columns.difference(['lastUpdated', 'dateCreated'])]
        t['import_datestamp'] = blob_date

        t = t[t['isOperational'] == True]
        t = t.drop_duplicates(subset=['operatorName', 'lat', 'lng'], ignore_index=True)

        if previous_table is None:
            previous_table = t
            continue  # first iteration, skip the rest of section, goto next iteration

        orig_cols = previous_table.columns

        m = pd.merge(previous_table, t, on=['operatorName', 'lat', 'lng'], how="outer", suffixes=(None, '_y'))
        m['import_datestamp'] = m['import_datestamp'].fillna(m['import_datestamp_y'])
        m['postcode'] = m['postcode'].fillna(m['postcode_y'])
        m['locationName'] = m['locationName'].fillna(m['locationName_y'])
        m['numConnectors'] = m['numConnectors'].fillna(m['numConnectors_y'])
        m['numFastConnectors'] = m['numFastConnectors'].fillna(m['numFastConnectors_y'])
        m['numOperationalConnectors'] = m['numOperationalConnectors'].fillna(m['numOperationalConnectors_y'])
        m['numOperationalFastConnectors'] = m['numOperationalFastConnectors'].fillna(
            m['numOperationalFastConnectors_y'])
        m['isOperational'] = m['isOperational'].fillna(m['isOperational_y'])
        m['numAC'] = m['numAC'].fillna(m['numAC_y'])
        m['numDC'] = m['numDC'].fillna(m['numDC_y'])

        previous_table = m[orig_cols]

    return previous_table


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    print(base64.b64decode(cloud_event.data["message"]["data"]))

    buck = Bucket(PROJECT_NAME, BUCKET_NAME)
    new_locs = new_locations_by_date(buck)
    logging.info(f"New locs table generated with {len(new_locs)} rows")
    if new_locs is not None and len(new_locs) > 0:
        buck.upload_blob(loc_analysis_blob_name, new_locs, content_type="text/csv")
        logging.info(f"new_locs file uploaded to cloud {loc_analysis_blob_name}")



