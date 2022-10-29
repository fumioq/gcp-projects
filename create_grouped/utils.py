from google.cloud import bigquery
from google.oauth2 import service_account
import pygsheets
import pandas as pd

def auth_sheets(service_account_json, scopes):
  gcp_credentials = service_account.Credentials.from_service_account_info(service_account_json, scopes = scopes)
  client = pygsheets.authorize(custom_credentials = gcp_credentials)

  return client

def auth_bigquery(service_account_json, project_id, scopes):
  gcp_credentials = service_account.Credentials.from_service_account_info(service_account_json, scopes = scopes)
  client = bigquery.Client(credentials=gcp_credentials, project=project_id)

  return client

def get_tables_sufix_list(bigquery_client, dataset_name):
  tables_in_dataset_list = bigquery_client.list_tables(dataset_name)
  
  table_name_in_dataset_list = [table_in_dataset.table_id for table_in_dataset in tables_in_dataset_list]

  table_sufix_in_dataset_list = [table_name.replace(dataset_name, '') for table_name in table_name_in_dataset_list]

  return table_sufix_in_dataset_list

def get_table_sufix_common_to_dataset_and_sheets(bigquery_client, dataset_name, structure_df_standard):
  table_sufix_in_sheets_list = [table_sufix for table_sufix in list(structure_df_standard.columns) if table_sufix not in ['fields', 'field_type']]

  table_sufix_in_dataset_list = get_tables_sufix_list(bigquery_client, dataset_name)

  get_table_sufix_common_to_dataset_and_sheets = [table_sufix_in_dataset for table_sufix_in_dataset in table_sufix_in_dataset_list if table_sufix_in_dataset in table_sufix_in_sheets_list]
  
  return get_table_sufix_common_to_dataset_and_sheets

def create_customer_structure_df(structure_df_standard, sufix_in_common_list):
  customer_structure_df = structure_df_standard.copy()

  customer_structure_df = customer_structure_df[sufix_in_common_list + ['field_type']]

  customer_structure_df = customer_structure_df.loc[(customer_structure_df[sufix_in_common_list]!=0).any(axis=1)]

  customer_structure_df = customer_structure_df.loc[(customer_structure_df[sufix_in_common_list]!='-').any(axis=1)]

  customer_structure_df = customer_structure_df.replace('-', 'CAST(NULL AS STRING)')
  
  return customer_structure_df

def prepare_df_to_string(customer_structure_df, sufix_in_common_list):
  for platform_table in sufix_in_common_list:
    customer_structure_df[platform_table] = customer_structure_df[platform_table].astype(str) + ' AS ' + customer_structure_df.index
  
  return customer_structure_df

def create_grouped_query(customer_structure_df, table_prefix, group_by):
  grouped_query = ''

  for table in list(customer_structure_df.columns):
    if grouped_query:
      grouped_query += '\n\nUNION ALL\n\n'

    table_query = customer_structure_df[table].str.cat(sep=',\n  ')

    grouped_query += 'SELECT\n  ' + table_query + f'\nFROM\n  `{table_prefix}{table}`\n GROUP BY\n  ' + group_by

  return grouped_query

def create_view(bigquery_client, grouped_query, table_prefix):
  view_name = f'{table_prefix}_grouped'
  view = bigquery.Table(view_name)
  view.view_query = grouped_query

  bigquery_client.create_table(view)

def update_view(bigquery_client, grouped_query, table_prefix):
  view = bigquery_client.get_table(f'{table_prefix}_grouped')
  current_view_query = view.view_query

  if current_view_query != grouped_query:
    view.view_query = grouped_query

    bigquery_client.update_table(view, ['view_query'])

def create_or_update_view(bigquery_client, grouped_query, table_prefix):
  try:
    create_view(bigquery_client, grouped_query, table_prefix)
    print(f'View {table_prefix}_grouped created!')

  except:
    update_view(bigquery_client, grouped_query, table_prefix)
    print(f'View {table_prefix}_grouped updated!')
