from google.cloud import bigquery
from google.oauth2 import service_account
from flask import Flask, request
from flask_api import status
import pygsheets
import re

import json
import os

app = Flask(__name__)

@app.route('/', methods=['GET'])
def main():

  token = request.args.get('token')
  case_when_sheet_url = request.args.get('case_when_sheet_url')
  table_id = request.args.get('table_id')

  if token != 'tws6h8srt1u9eg19':
    return status.HTTP_403_FORBIDDEN
    
  if any(param == '' for param in [case_when_sheet_url, table_id]):
    return status.HTTP_412_PRECONDITION_FAILED

  scopes = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/spreadsheets",
  ]

  service_account_json = json.loads(os.environ['GCP_SERVICE_ACCOUNT'], strict= False)

  gcp_credentials = service_account.Credentials.from_service_account_info(service_account_json, scopes=scopes)

  bq_client = bigquery.Client(credentials=gcp_credentials, project=gcp_credentials.project_id)
  sheet_client = pygsheets.authorize(custom_credentials=gcp_credentials)

  try:
    
    ss_case_when = sheet_client.open_by_url(case_when_sheet_url)
    worksheets = ss_case_when.worksheets()

    complete_case_when = ''

    for wks in worksheets:
      case_when_df = wks.get_as_df()
      calculated_field_name = wks.title

      case_when_df = case_when_df[case_when_df['WHEN'] != '']
      case_when_df['when_then'] = case_when_df['WHEN'] + ' ' + case_when_df['THEN']
      complete_case_when += "CASE\n" + case_when_df['when_then'].str.cat(sep='\n') + "\nEND AS " + calculated_field_name + ",\n\n"

    consolidated_view = bq_client.get_table(table_id)

    new_view_query = re.sub(r'CASE WHEN FROM SHEETS(.*)--END OF CASE WHEN FROM SHEETS',
                            f'CASE WHEN FROM SHEETS\n\n{complete_case_when}\n\n--END OF CASE WHEN FROM SHEETS',
                            consolidated_view.view_query,  count=1, flags=re.S)

    consolidated_view.view_query = new_view_query
    bq_client.update_table(consolidated_view, ["view_query"])

    return 'Success!', status.HTTP_200_OK

  except Exception as e:
    print(f'Error encountered: {e}')
    return 'Something has failed', status.HTTP_500_INTERNAL_SERVER_ERROR


