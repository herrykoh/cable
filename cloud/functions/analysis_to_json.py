import functions_framework
from google.cloud import storage
import pandas as pd
from io import StringIO
import logging
from datetime import datetime, date

PROJECT_NAME = "data-attic"
BUCKET_NAME = "ev-chargers-opencharge"

analysis_blob_name = "analysis/opencharge.csv"


def print_event(evt_data):
    bucket = evt_data["bucket"]
    name = evt_data["name"]
    metageneration = evt_data["metageneration"]
    timeCreated = evt_data["timeCreated"]
    updated = evt_data["updated"]

    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")


def get_bucket(bucket_name):
    storage_client = storage.Client(project='data-attic')
    return storage_client.get_bucket(bucket_name)


def load_from_bucket(bucket, blob_name) -> pd.DataFrame:
    blob = bucket.blob(blob_name)
    return pd.read_csv(StringIO(blob.download_as_text()))


def analyze_opencharge(t) -> pd.DataFrame:
    groupbyloc = t[['operatorName', 'locationName']]
    loc_count = groupbyloc.groupby(['operatorName'], as_index=True).count()

    groupbysum = t[['operatorName', 'numConnectors', 'numFastConnectors', 'numOperationalConnectors',
                    'numOperationalFastConnectors', 'numAC', 'numDC']]

    # ntwkdups = groupbysum.groupby(['operatorName', 'postcode', 'locationName'], as_index=False).sum()
    conn_count = groupbysum.groupby(['operatorName'], as_index=True).sum()
    # print(conn_count)
    summary_table = loc_count.join(conn_count)

    return summary_table


def create_summary(bucket, blob_name) -> pd.DataFrame:
    t = load_from_bucket(bucket, blob_name)

    summary = analyze_opencharge(t)

    endidx = blob_name.rfind('-')
    datestr = blob_name[endidx - 8:endidx]

    import_date = datetime.strptime(datestr, '%Y%m%d').date()

    print(f"Import date is {import_date}")

    summary['import_date'] = import_date
    summary.set_index(['import_date'], append=True, inplace=True)

    return summary


def load_analysis_from_cloud(bucket, blobname) -> pd.DataFrame:
    table = load_from_bucket(bucket, blobname)

    # table['import_date'] = pd.to_datetime(table['import_date'], format='%Y-%m-%d')
    table.set_index(['operatorName', 'import_date'], inplace=True)

    return table


def save_to_cloud(bucket, blobname, table_to_append):
    blob = bucket.get_blob(blobname)
    blob.upload_from_string(table_to_append.to_csv())


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    print_event(data)

    bucketname = data["bucket"]
    blobname = data["name"]

    # only interested in the data folder csv files, ignore all others
    if not (blobname.startswith('data') and blobname.endswith("csv")):
        return

    bucket = get_bucket(bucketname)

    summary_table = create_summary(bucket, blobname)

    analysis_table = load_analysis_from_cloud(bucket, analysis_blob_name)

    new_table = pd.concat([analysis_table, summary_table])

    logging.info(f"New analysis table length is {len(new_table)}")

    save_to_cloud(bucket, analysis_blob_name, new_table)

    logging.info(f"Saved to cloud")



