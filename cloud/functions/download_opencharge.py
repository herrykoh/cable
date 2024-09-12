import base64
import functions_framework
import json
import requests as req
from google.cloud import storage
import datetime as dt

bucket_name = "ev-chargers-opencharge"

providers = {'INSTAVOLT': 3296,
             'GRIDSERV': 3430,
             'BPPULSE': 32,
             'IONITY': 3299,
             'MFG': 3471,
             'ELECTRICHIGHWAY': 24,
             'FASTNED': 74,
             'PODPOINT': 3,
             'OSPREY': 203,
             'SHELLRECHARGE': 3392,

             'Swarco E.Connect': 3341,
             'ubitricity': 2244,
             'Chargeplace Scotland': 3315,
             'GeniePoint': 150,
             'Tesla (Tesla-only charging)': 23,
             'Tesla (including non-tesla)': 3534,
             'Evie': 3398,
             'ESB Energy': 3357,
             'evyve': 3587,
             'ChargePoint': 5,
             }

provider_groups = {
    'A': ['INSTAVOLT', 'GRIDSERV', 'BPPULSE', 'IONITY', 'MFG', 'ELECTRICHIGHWAY', 'FASTNED', 'PODPOINT',
          'OSPREY', 'SHELLRECHARGE'],
    'B': ['ChargePoint', 'Chargeplace Scotland', 'ESB Energy', 'Swarco E.Connect', 'ubitricity',
          'Tesla (Tesla-only charging)', 'evyve', 'GeniePoint']
}

headers = {
    'X-API-Key': ""
}


def save_to_bucket(name, jchargers):
    storage_client = storage.Client(project='data-attic')

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(name)

    with blob.open('w') as b:
        b.write(json.dumps(jchargers, indent=2))


def get_provider_codes(evt_msg):
    if evt_msg.startswith('group'):
        groupname = evt_msg[-1]
        if groupname in provider_groups:
            providers_to_download = provider_groups[groupname]
            print(f"Downloading providers {' - '.join(providers_to_download)}")
            provider_ids = [str(providers[a]) for a in providers_to_download]
            provider_id_str = ','.join(provider_ids)

            return provider_id_str

    # return default values
    # return "3299,3296,3430,32,3471,24,74,3,203,3392"
    return "3471,74,3296"


# Triggered from a message on a Cloud Pub/Sub topic.
@functions_framework.cloud_event
def hello_pubsub(cloud_event):
    # Print out the data from Pub/Sub, to prove that it worked
    evt_msg = base64.b64decode(cloud_event.data["message"]["data"]).decode('utf-8')
    print(evt_msg)

    country_code = 'GB'
    max_results = 16800

    provider_codes = get_provider_codes(evt_msg)
    print(f"Provider IDs to download: {provider_codes}")

    poi_url = f"https://api.openchargemap.io/v3/poi?countrycode={country_code}" + \
              f"&verbose=false&maxresults={max_results}&operatorid={provider_codes}"

    print(f"URL: {poi_url}")

    r = req.get(poi_url, headers=headers)
    jchargers = r.json()

    print(f"Downloaded {len(jchargers)}")

    todaystr = dt.datetime.today().strftime('%Y%m%d-%H%M')

    json_filename = f"downloads/opencharge-{evt_msg}-{todaystr}.json"
    save_to_bucket(json_filename, jchargers)

    print(f"Written to bucket {bucket_name}/{json_filename}")


