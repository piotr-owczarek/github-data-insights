import os
import gzip
import json
import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Set environment variable for GCP auth
creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.environ.get("BUCKET_NAME")

current_date = '{{data_interval_start.strftime(\'%Y-%m-%d\')}}'
current_run = '{{data_interval_start.strftime(\'%Y-%m-%d-%-H\')}}'

def file_exists_in_gcs(current_date, current_run):
    client = storage.Client()
    blob_name = f"{current_date}/{current_run}.parquet"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    return blob.exists()

def download_file(current_date, current_run):
    print(f"Processing {current_run} UTC")
    if file_exists_in_gcs(current_date, current_run):
        print("File already exists in GCS. Skipping download.")
        return

    url = f"https://data.gharchive.org/{current_run}.json.gz"
    response = requests.get(url, stream=True)

    with open(f"/tmp/{current_run}.json.gz", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def extract_relevant_fields(event):
    return {
        "id": event.get("id"),
        "type": event.get("type"),
        "public": event.get("public"),
        "created_at": event.get("created_at"),
        "actor_login": event.get("actor", {}).get("login"),
        "repo_name": event.get("repo", {}).get("name"),
        "org_login": event.get("org", {}).get("login")
    }

def to_parquet(current_run):
    file_path = f"/tmp/{current_run}.json.gz"
    output_path = f"/tmp/{current_run}.parquet"

    data = []
    with gzip.open(file_path, 'rt', encoding='utf-8') as f:
        for line in f:
            try:
                raw_event = json.loads(line)
                event = extract_relevant_fields(raw_event)
                data.append(event)
            except json.JSONDecodeError as e:
                print(f"Skipping bad line: {e}")

    df = pd.DataFrame(data)

    df.to_parquet(
        output_path,
        engine='pyarrow',
        compression='snappy',
        index=False
    )

def upload_to_gcs(current_date, current_run):
    client = storage.Client()
    blob_name = f"{current_date}/{current_run}.parquet"
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(
        f"/tmp/{current_run}.parquet", 
        content_type='application/octet-stream', 
        timeout=60 * 3
        )


with DAG(
    dag_id='gharchive_etl_to_gcs',
    default_args = {
        'owner': 'piotr',
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
        },
    start_date=datetime.today() - timedelta(days=1),
    schedule_interval='15 * * * *',
    catchup=True,  # Enables backfilling
    max_active_runs=1, # Ensures only one DAG run is active at a time, so it doesn't purge other runs' files
    tags=['gharchive', 'gcs'],
) as dag:

    download_file_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
        op_kwargs={
            'current_date': current_date,
            'current_run': current_run
        }
    )

    to_parquet_task = PythonOperator(
        task_id='to_parquet',
        python_callable=to_parquet,
        op_kwargs={
            'current_run': current_run
        }
    )

    upload_to_gcs_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        op_kwargs={
            'current_date': current_date,
            'current_run': current_run
        }
    )

    purge_files_task = BashOperator(
        task_id='purge_files',
        bash_command='rm -f /tmp/*.json.gz && rm -f /tmp/*.parquet',
    )

    download_file_task >> to_parquet_task >> upload_to_gcs_task >> purge_files_task