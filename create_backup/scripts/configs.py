from datetime import datetime

BUCKET_NAME = "tables_backup_bigquery"

BACKUP_PROJECT = 'gcp-project-365616'

BACKUP_LIST = []

TODAY = datetime.now().strftime('%F').replace('-', '')

SCOPES = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
]

