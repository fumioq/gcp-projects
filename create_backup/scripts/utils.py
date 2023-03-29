from configs import *
import json
from datetime import timedelta
from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud import storage


def generate_datasets_json(bq_client : bigquery.Client, project_id : str, datasets : list[bigquery.Dataset]) -> None:
  """Generate all files of all datasets of the project

  Args:
      bq_client (bigquery.Client): service account authenticated bigquery client
      project_id (str): project_id to be backed up
      datasets (list): dataset list with all project's datasets
  """
  for dataset in datasets:
    dataset_name = dataset.dataset_id
    print(dataset_name)
    tables = list(bq_client.list_tables(dataset))
    generate_tables_json(bq_client, project_id, dataset_name, tables)
  #end for


def generate_tables_json(bq_client : bigquery.Client, project_id : str, dataset_name : str, tables : list[bigquery.Table]) -> None:
  """Generate all files of all tables of the dataset

  Args:
      bq_client (bigquery.Client): service account authenticated bigquery client
      project_id (str): project_id to be backed up
      dataset_name (str): dataset name of current dataset
      tables (list): tables of current dataset
  """
  for table in tables:
    if table.table_type == 'TABLE':
      continue
    #end if

    table_bq = bq_client.get_table(f'{project_id}.{dataset_name}.{table.table_id}')

    if table.table_type == 'VIEW':
      table_json = generate_view_json(table_bq, TODAY)
      BACKUP_LIST.append(table_json)
      continue
    #end if

    if table_bq.external_data_configuration.source_format == 'AVRO':
      table_json = generate_external_table_json(table_bq, TODAY)
      BACKUP_LIST.append(table_json)
      continue
    #end if

    if table_bq.external_data_configuration.source_format == 'GOOGLE_SHEETS':
      table_json = generate_sheets_json(table_bq, TODAY)
      BACKUP_LIST.append(table_json)
      continue
    #end if
  #end for

def generate_sheets_json(table : bigquery.Table, today : str) -> object:
  """Get table info and store it into an object

  Args:
      table (bigquery.Table): google sheets external table
      today (str): today string in YYYYMMDD format

  Returns:
      object: object with filename and json information
  """
  table_id = f'{table.project}.{table.dataset_id}.{table.table_id}'

  export_data = {}

  export_data['sourceFormat'] = "GOOGLE_SHEETS"

  export_data['schema'] = {}
  export_data['schema']['fields'] = []

  for schema_field in table.schema:
    field = {
        'name' : schema_field.name,
        'type' : schema_field.field_type,
    }
    export_data['schema']['fields'].append(field)
  # end for

  export_data['googleSheetsOptions'] = {
      'range' : table.external_data_configuration.google_sheets_options.range,
      'skipLeadingRows' : table.external_data_configuration.google_sheets_options.skip_leading_rows,
  }

  export_data['sourceUris'] = table.external_data_configuration.source_uris

  file_schema = json.dumps(export_data)

  return {
      'file_name' : f'{table.project}/{table.dataset_id}/{today}--{table_id}.json',
      'json' : file_schema,
  }


def generate_external_table_json(table : bigquery.Table, today : str) -> object:
  """Get external URI and store it into an object

  Args:
      table (bigquery.Table): view table
      today (str): today string in YYYYMMDD format

  Returns:
      object: object with filename and json information
  """
  table_id = f'{table.project}.{table.dataset_id}.{table.table_id}'

  export_data = {}

  export_data['sourceFormat'] = "STORAGE"

  export_data['sourceUris'] = table.external_data_configuration.source_uris

  json_view = json.dumps(export_data)

  return {
      'file_name' : f'{table.project}/{table.dataset_id}/{today}--{table_id}.json',
      'json' : json_view,
  }


def generate_view_json(table : bigquery.Table, today : str) -> object:
  """Get view query and store it into an object

  Args:
      table (bigquery.Table): view table
      today (str): today string in YYYYMMDD format

  Returns:
      object: object with filename and json information
  """
  table_id = f'{table.project}.{table.dataset_id}.{table.table_id}'

  export_data = {}

  export_data['sourceFormat'] = "VIEW"

  export_data['viewQuery'] = table.view_query

  json_view = json.dumps(export_data)

  return {
      'file_name' : f'{table.project}/{table.dataset_id}/{today}--{table_id}.json',
      'json' : json_view,
  }



def upload_json_into_bucket(storage_client : storage.Client) -> None:
  """Upload all files into bucket

  Args:
      storage_client (storage.Client): service account authenticated google storage client
  """
  try:
    bucket = storage_client.get_bucket(BUCKET_NAME)

  except:
    bucket = storage_client.create_bucket(BUCKET_NAME)

  finally:
    for json in BACKUP_LIST:
      blob = bucket.blob(json['file_name'])

      with blob.open("w") as f:
        f.write(json['json'])
      #end with
    # end for
  # end finnaly

