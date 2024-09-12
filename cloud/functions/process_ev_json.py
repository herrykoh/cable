import functions_framework

from google.cloud import storage
import pandas as pd
import json


def get_bucket(bucket_name):
    storage_client = storage.Client(project='data-attic')
    return storage_client.get_bucket(bucket_name)


def load_from_bucket(bucket, blob_name):
    blob = bucket.blob(blob_name)
    return json.loads(blob.download_as_text())


def process_each_charger(charger):
    operatorName = charger['OperatorInfo']['Title']
    addr = charger['AddressInfo']
    addr_title = addr.get('Title')
    addr_postcode = addr.get('Postcode')

    # statusTypeId = charger.get('StatusTypeID')
    lastUpdate = charger.get('DateLastStatusUpdate')
    dateCreated = charger.get('DateCreated')

    status = charger.get('StatusType', dict())
    isOperational = status.get('IsOperational', False)

    # distance = addr['Distance']
    lat = addr.get('Latitude')
    lng = addr.get('Longitude')
    # lastVerified = charger.get('DateLastVerified')
    connector_count = 0
    fastconnector_count = 0
    operational_conn_count = 0
    operational_fastconn_count = 0
    numAC = 0
    numDC = 0
    for connector in charger.get('Connections'):

        connStatusType = connector.get('StatusType', dict())
        # isConnOperational = False

        isConnOperational = connStatusType.get('IsOperational', False)
        if isConnOperational:
            operational_conn_count += 1

        currentType = connector.get('CurrentTypeID', 0)
        if currentType == 30:
            numDC += 1
        else:
            numAC += 1

        if connector.get('Level'):
            connector_count += 1
            fastChargeCapable = connector['Level']['IsFastChargeCapable']
            if fastChargeCapable:
                fastconnector_count += 1
                if isConnOperational:
                    operational_fastconn_count += 1
                # power = connector['PowerKW']
                # print(operatorName, ':', distance, '(', lat, ',', lng, ')', ':', power, '-', lastVerified)

    return {'operatorName': operatorName,
            'locationName': addr_title,
            'postcode': addr_postcode,
            # 'statusTypeID': int(statusTypeId) if statusTypeId else -1,
            'lastUpdated': lastUpdate,
            'dateCreated': dateCreated,
            'numConnectors': connector_count,
            'numFastConnectors': fastconnector_count,
            'numOperationalConnectors': operational_conn_count,
            'numOperationalFastConnectors': operational_fastconn_count,
            'isOperational': isOperational,
            'lat': lat, 'lng': lng,
            'numAC': numAC, 'numDC': numDC}


def convert_to_csv(jchargers):
    dict_charger_list = [process_each_charger(c) for c in jchargers]
    t = pd.DataFrame(dict_charger_list)
    return t.to_csv()


def write_to_blob(blob, csv_table):
    blob.upload_from_string(csv_table)


def print_info(data):
    bucket = data["bucket"]
    name = data["name"]
    metageneration = data["metageneration"]
    timeCreated = data["timeCreated"]
    updated = data["updated"]

    print(f"Bucket: {bucket}")
    print(f"File: {name}")
    print(f"Metageneration: {metageneration}")
    print(f"Created: {timeCreated}")
    print(f"Updated: {updated}")


# Triggered by a change in a storage bucket
@functions_framework.cloud_event
def hello_gcs(cloud_event):
    data = cloud_event.data

    event_id = cloud_event["id"]
    event_type = cloud_event["type"]

    print_info(data)

    bucketname = data["bucket"]
    blobname = data["name"]

    if not blobname.endswith('json'):
        return

    bucket = get_bucket(bucketname)

    jchargers = load_from_bucket(bucket, blobname)
    print(f"{len(jchargers)} records downloaded")

    csv_table = convert_to_csv(jchargers)

    csv_name = blobname[blobname.find('/') + 1:blobname.rfind('.')]
    write_to_blob(bucket.blob(f"data/{csv_name}.csv"), csv_table)


