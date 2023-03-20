from flask import Flask, request
from flask_api import status
import json
import os

from utils import *
from configs import *

app = Flask(__name__)

@app.route('/')
def main():
  project_id = request.args.get('project_id')

  SERVICE_ACCOUNT_JSON = json.loads(os.environ['GCP_SERVICE_ACCOUNT'], strict= False)
  print(project_id)

  gcp_credentials = service_account.Credentials.from_service_account_info(SERVICE_ACCOUNT_JSON, scopes=SCOPES)

  storage_client = storage.Client(project = BACKUP_PROJECT, credentials = gcp_credentials)
  bq_client = bigquery.Client(credentials=gcp_credentials, project=project_id)

  datasets = list(bq_client.list_datasets(project_id))
  generate_datasets_json(bq_client, project_id, datasets)

  print('Uploading files...')
  upload_json_into_bucket(storage_client)
  print('Done!')

  return 'Success!', status.HTTP_200_OK