import os
from datetime import datetime

from google.cloud import bigquery, storage
from utils.gcp import build_bq_from_gcs, upload_file_to_gcs

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_SRC_DATASET", "trips_data_all")
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
# TARGET_DATE = '{{ execution_date.strftime(\'%Y-%m\') }}'
TARGET_DATE = "2022-{{ execution_date.strftime('%m') }}"
TARGET_FILE = "yellow_tripdata_" + TARGET_DATE + ".parquet"
URL_TEMPLATE = URL_PREFIX + "/" + TARGET_FILE
OUTPUT_FILE_TEMPLATE = os.path.join(AIRFLOW_HOME, TARGET_FILE)

GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

with DAG(
    dag_id="d_data_ingestion_gcs",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2023, 1, 1),
    tags=["example"],
) as dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_file_to_gcs,
        op_kwargs={
            "client": GCS_CLIENT,
            "bucket_name": BUCKET,
            "blob_name": f"yellow_taxi/{TARGET_FILE}",
            "source_filepath": OUTPUT_FILE_TEMPLATE,
        },
    )

    bigquery_external_table_task = PythonOperator(
        task_id="bigquery_external_table_task",
        python_callable=build_bq_from_gcs,
        op_kwargs={
            "client": BQ_CLIENT,
            "dataset_name": BIGQUERY_DATASET,
            "table_name": TARGET_FILE.replace(".parquet", ""),
            "bucket_name": BUCKET,
            "blob_name": f"yellow_taxi/{TARGET_FILE}",
        },
    )

    (download_dataset_task >> local_to_gcs_task >> bigquery_external_table_task)
