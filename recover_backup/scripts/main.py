from flask import Flask, request
from flask_api import status
import json
import os

from datetime import timedelta
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage

from utils import *
from configs import *

app = Flask(__name__)

@app.route('/')
def main():
  table_id = request.args.get('table_id')
  rollback_days = int(request.args.get('rollback_days'))
  url_token = request.args.get('url_token')

  if url_token != 'ash6ae51ha6e87j3w':
    return 'Invalid token', status.HTTP_401_UNAUTHORIZED

  project_id = table_id.split('.')[0]
  dataset_name = table_id.split('.')[1]

  SERVICE_ACCOUNT_JSON = json.loads(os.environ['GCP_SERVICE_ACCOUNT'], strict= False)

  gcp_credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

  storage_client = storage.Client(project = BACKUP_PROJECT, credentials = gcp_credentials)
  bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

  bucket = storage_client.get_bucket(BUCKET_NAME)

  bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

  rollback_days = timedelta(rollback_days)
  rollback_date = datetime.now() - rollback_days
  rollback_date_string = rollback_date.strftime('%F').replace('-', '')

  print(rollback_date_string)

  reroll_one_dataset = len(table_id.split('.')) == 3

  blobs_received = 0

  if reroll_one_dataset:
    print("Rollback one table")
    try:
      blob = bucket.blob(f'{project_id}/{dataset_name}/{rollback_date_string}--{table_id}.json')
      blobs_received = create_table_from_blob(bq_client, blob, blobs_received)
    except:
      print(f'There is no backup of date {rollback_date_string} for {table_id} table/view.')
    return 'Success!', status.HTTP_200_OK
  #end if

  print("Rollback whole dataset")

  dataset_token = request.args.get('dataset_token')
  if dataset_token != 'datageeks':
    print("Invalid Token. Ignoring request.")
    return 'Invalid Token', status.HTTP_403_FORBIDDEN
  #end if

  try:
    bq_client.get_dataset(table_id)
  except:
    bq_client.create_dataset(table_id)

  views_blobs = []

  dataset_blob_list = bucket.list_blobs(prefix=f'{project_id}/{dataset_name}/{rollback_date_string}')
  for blob in dataset_blob_list:
    with blob.open("r") as f:
      blobs_received += 1
      json_data = json.loads(f.read())

      if json_data['sourceFormat'] == 'VIEW':
        views_blobs.append(blob)
        continue

      blobs_received = create_table_from_blob(bq_client, blob, blobs_received)
    #end with
  #end for

  for blob in views_blobs:
    try:
      blobs_received = create_table_from_blob(bq_client, blob, blobs_received)
    except Exception as e:
      print(e)
  #end for

    if blobs_received == 0:
      print(f'There is no backup of date {rollback_date_string} for {table_id} dataset.')
    #end if
  #end if

  return 'Success!', status.HTTP_200_OK