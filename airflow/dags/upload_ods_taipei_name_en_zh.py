import os
from datetime import timedelta

from google.cloud import bigquery, storage
from utils.gcp import upload_file_to_gcs, build_bq_from_gcs, query_bq

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
TABLE_NAME = "ods_attraction"
BLOB_NAME = f"{TABLE_NAME}/ods_taipei_name_en_zh.csv"

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
EXTABLE_NAME = "ods_taipei_name_en_zh"

GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

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
    dag_id="upload_ods_taipei_name_en_zh",
    default_args=default_args,
    description="上傳交通部的台北景點中英對照表",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)

def upload_ods_taipei_name_en_zh_to_gcs_task_datapipline():
    @task
    def upload_ods_taipei_name_en_zh_to_gcs():
        return upload_file_to_gcs(client=GCS_CLIENT, bucket_name=PROCESSED_BUCKET, blob_name=BLOB_NAME, source_filepath="/opt/airflow/dags/ods_taipei_name_en_zh.csv")

    @task
    def create_ods_taipei_name_en_zh_bq_external_table(
        bucket_name, blob_name, dataset_name, table_name, filetype
    ):
        """
        Create BigQuery external table.

        Args:
            bucket_name (str): bucket name.
            blob_name (str): blob name.
            dataset_name (str): dataset name.
            table_name (str): table name.
        """
        build_bq_from_gcs(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            bucket_name=bucket_name,
            blob_name=blob_name,
            schema=[
                bigquery.SchemaField("name_zh", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name_en", "STRING", mode="NULLABLE"),
            ],
            filetype=filetype
        )


    upload_data_to_gcs_task = upload_ods_taipei_name_en_zh_to_gcs()
    create_bq_external_table_task = create_ods_taipei_name_en_zh_bq_external_table(
        PROCESSED_BUCKET, BLOB_NAME, BQ_ODS_DATASET, EXTABLE_NAME, "csv"
    )

    # Set dependencies
    create_bq_external_table_task.set_upstream(upload_data_to_gcs_task)


upload_ods_taipei_name_en_zh_to_gcs_task_datapipline()
