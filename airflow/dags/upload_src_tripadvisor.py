import os
from datetime import timedelta

from google.cloud import storage
from utils.gcp import upload_file_to_gcs

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
TABLE_NAME = "src_attraction"
BLOB_NAME = f"{TABLE_NAME}/src_tripadvisor.csv"

GCS_CLIENT = storage.Client()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="upload_src_tripadvisor",
    default_args=default_args,
    description="上傳Tripadvisor網站原始資料",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["src"],
)

def upload_src_tripadvisor_to_gcs_task():
    @task
    def upload_src_tripadvisor_to_gcs():
        return upload_file_to_gcs(client=GCS_CLIENT, bucket_name=RAW_BUCKET, blob_name=BLOB_NAME, source_filepath="/opt/airflow/dags/src_tripadvisor.csv")

    upload_src_tripadvisor_to_gcs()

upload_src_tripadvisor_to_gcs_task()
