import os
import json
import gzip
import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Connection
from airflow import settings

# Set environment variables
creds = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
bucket_name = os.environ.get("BUCKET_NAME")
cluster_name = os.environ.get("CLUSTER_NAME")
cluster_region = os.environ.get("CLUSTER_REGION")
fact_user_activity = os.environ.get("FACT_USER_ACTIVITY")
fact_repo_popularity = os.environ.get("FACT_REPO_POPULARITY")
fact_hourly_activity = os.environ.get("FACT_HOURLY_ACTIVITY")
bq_dataset = os.environ.get("BQ_DATASET")
project_id = os.environ.get("GCP_PROJECT_ID")

# Set Airflow variables
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
        raise AirflowSkipException("File already exists in GCS. Skipping download.")
    
    url = f"https://data.gharchive.org/{current_run}.json.gz"
    response = requests.get(url, stream=True)

    with open(f"/tmp/{current_run}.json.gz", "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

def extract_relevant_fields(event):
    return {
        "id": event.get("id"),
        "type": event.get("type"),
        "created_at": event.get("created_at"),
        "user": event.get("actor", {}).get("login"),
        "repository": event.get("repo", {}).get("name"),
        "organization": event.get("org", {}).get("login"),
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

def create_gcp_connection():
    conn_id = "my_gcp_conn"
    conn = Connection(
        conn_id=conn_id,
        conn_type="google_cloud_platform",
        extra={
            "project": project_id,
            "key_path": creds
        }
    )
    session = settings.Session()
    if not session.query(Connection).filter(Connection.conn_id == conn_id).first():
        session.add(conn)
        session.commit()


with DAG(
    dag_id='gharchive_etl_to_gcs',
    default_args = {
        'owner': 'piotr',
        'depends_on_past': True,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        },
    start_date=datetime.today() - timedelta(days=7), #Backfilling for 7 days, change as needed
    schedule_interval='15 * * * *', #Hourly at 15 minutes past the hour
    catchup=True,  # Enables backfilling
    max_active_runs=5, # Allows multiple runs, change to lower number if local resources are limited
    tags=['gharchive'],
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
        bash_command=f'rm -f /tmp/{current_run}.json.gz && rm -f /tmp/{current_run}.parquet',
    )

    gcp_connection_task = PythonOperator(
        task_id='create_gcp_connection',
        python_callable=create_gcp_connection,
    )

    dataproc_job = {
        "reference": {"project_id": project_id},
        "placement": {"cluster_name": cluster_name},
        "pyspark_job": {
            "main_python_file_uri": f'gs://{bucket_name}/dataproc/gharchive_transform.py',
            "args": [
                '--input_path', f'gs://{bucket_name}/{current_date}/{current_run}.parquet',
                '--fact_user_activity', f'{bq_dataset}.{fact_user_activity}',
                '--fact_repo_popularity', f'{bq_dataset}.{fact_repo_popularity}',
                '--fact_hourly_activity', f'{bq_dataset}.{fact_hourly_activity}'
            ]
        },
    }

    dataproc_processing_task = DataprocSubmitJobOperator(
        task_id="dataproc_processing",
        job=dataproc_job,
        region=cluster_region,
        project_id=project_id,
        gcp_conn_id="my_gcp_conn"
    )


    download_file_task >> to_parquet_task >> upload_to_gcs_task >> purge_files_task >> gcp_connection_task >> dataproc_processing_task 
