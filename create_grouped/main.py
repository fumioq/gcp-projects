from flask import Flask, request
from flask_api import status

from utils import *

import json
import os

app = Flask(__name__)

@app.route('/', methods=['POST'])
def main():

  configs = json.loads(request.data)

  project_ids = configs['project_ids']

  scopes = [
      "https://www.googleapis.com/auth/bigquery",
      "https://www.googleapis.com/auth/spreadsheets",
  ]

  service_account_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'], strict=False)

  gcp_credentials = service_account.Credentials.from_service_account_info(service_account_json, scopes=scopes)

  sheet_client = auth_sheets(service_account_json, scopes=scopes)

  ss = sheet_client.open_by_url('https://docs.google.com/spreadsheets/d/1B_89ft5yLNlAL3xB0DF6IqOgiQ7dgeGG9oQrjnbXaAc/edit#gid=0')

  ws = ss.worksheet_by_title('grouped')
  structure_df_standard = ws.get_as_df()
  structure_df_standard.set_index("fields", inplace = True)

  # project_ids = ["agency1-367013", "agency2-367013", "agency3"]

  for project_id in project_ids:
    print(project_id)
    bigquery_client = auth_bigquery(service_account_json, project_id, scopes)
    dataset_list = bigquery_client.list_datasets(project_id)
    dataset_name_list = [dataset.dataset_id for dataset in dataset_list]

    for dataset_name in dataset_name_list:
      print(dataset_name)
      sufix_in_common_list = get_table_sufix_common_to_dataset_and_sheets(bigquery_client, dataset_name, structure_df_standard)

      customer_structure_df = create_customer_structure_df(structure_df_standard, sufix_in_common_list)

      customer_structure_df = prepare_df_to_string(customer_structure_df, sufix_in_common_list)

      group_by_dimensions = ',\n  '.join(customer_structure_df[customer_structure_df['field_type'] == 'dimension'].index.tolist())

      customer_structure_df.drop('field_type', inplace=True, axis = 1)

      grouped_query = create_grouped_query(customer_structure_df, f'{project_id}.{dataset_name}.{dataset_name}', group_by_dimensions)

      create_or_update_view(bigquery_client, grouped_query, f'{project_id}.{dataset_name}.{dataset_name}')

