from configs import *
import json
from google.cloud import bigquery
from google.cloud import storage
from google.cloud import exceptions


def create_table_from_blob(bq_client : bigquery.Client, blob : storage.Blob, blobs_received : int) -> int:
  """Create table of view from sotrage blob of the given date (if exists)

  Args:
      gcp_credentials (bigquery.Client): credentials for 
      blob (storage.Blob): _description_

  Returns:
      int: blobs counter
  """

  type_dictionary = {
    'VIEW' : create_view_from_json,
    'GOOGLE_SHEETS' : create_googlesheets_from_json,
    'STORAGE' : create_storage_table_from_json,
  }

  with blob.open("r") as f:
    blobs_received += 1
    json_data = json.loads(f.read())

    request_table_id = blob.name.split('--')[1].replace('.json', '')

    print('_________________________________________________')
    print(request_table_id)

    type_dictionary[json_data['sourceFormat']](bq_client, json_data, request_table_id)
  #end with

  return blobs_received


def create_view_from_json(bq_client : bigquery.Client, json_data : object, request_table_id : str) -> None:
  """Create view from json file data

  Args:
      bq_client (bigquery.Client): service account authenticated bigquery client
      json_data (object): view restore information (view_query)
      request_table_id (str): table_id in project_id.dataset_name.table_name format
  """
  print(f'Creating {json_data["sourceFormat"]}')

  table = bigquery.Table(f'{request_table_id}')
  table.view_query = json_data['viewQuery']

  bq_client.delete_table(f'{request_table_id}', not_found_ok=True)

  try:
    table = bq_client.create_table(table)

  except exceptions.NotFound:
    print(f'View referenciando uma tabela que não existe ainda. View comentada criada.')
    table.view_query = '--Recuperar Backup Log - Alguma tabela utilizada nessa view não foi encontrada. Corrija e descomente a view abaixo.\n\n'+'SELECT 1\n' + json_data['viewQuery'].replace('\n', '\n--')
    table = bq_client.create_table(table)


def generate_schema_from_json(json_schema : object) -> list[bigquery.SchemaField]:
  """Transform field objeto into bigquery.Schemafield

  Args:
      json_schema (object): field information object

  Returns:
      list: list of fields
  """
  schema = []
  for field_json in json_schema:
    schema.append(bigquery.SchemaField(field_json['name'], field_json['type'], mode = 'NULLABLE'))
  # end for

  return schema


def create_googlesheets_from_json(bq_client : bigquery.Client, json_data : object, request_table_id : str) -> None:
  """Create external table from json file data

  Args:
      bq_client (bigquery.Client): service account authenticated bigquery client
      json_data (object): google sheets restore information
      request_table_id (str): table_id in project_id.dataset_name.table_name format
  """
  print(f'Creating {json_data["sourceFormat"]}')
  table = bigquery.Table(f'{request_table_id}', schema=generate_schema_from_json(json_data['schema']['fields']))

  external_config = bigquery.ExternalConfig("GOOGLE_SHEETS")
  external_config.source_uris = json_data['sourceUris']
  external_config.options.skip_leading_rows = json_data['googleSheetsOptions']['skipLeadingRows']
  external_config.options.range = json_data['googleSheetsOptions']['range']
  table.external_data_configuration = external_config

  bq_client.delete_table(f'{request_table_id}', not_found_ok=True)
  table = bq_client.create_table(table)


def create_storage_table_from_json(bq_client : bigquery.Client, json_data : object, request_table_id : str) -> None:
  """Create external table from json file data

  Args:
      bq_client (bigquery.Client): service account authenticated bigquery client
      json_data (object): google sheets restore information
      request_table_id (str): table_id in project_id.dataset_name.table_name format
  """
  print(f'Creating {json_data["sourceFormat"]}')
  table = bigquery.Table(f'{request_table_id}')

  external_config = bigquery.ExternalConfig("AVRO")
  external_config.source_uris = json_data['sourceUris']
  external_config.autodetect = True
  table.external_data_configuration = external_config

  bq_client.delete_table(f'{request_table_id}', not_found_ok=True)
  table = bq_client.create_table(table)
