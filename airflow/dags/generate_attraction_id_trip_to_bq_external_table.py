import os
from datetime import timedelta

import pandas as pd
import hashlib
from google.cloud import bigquery, storage
from utils.gcp import (
    download_df_from_gcs,
    upload_df_to_bq,
)

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")

TABLE_NAME = "ods_attraction"
BLOB_NAME = f"{TABLE_NAME}/ods_tripadvisor.csv"
EXTABLE_NAME = "ods_attraction_generate_id_trip"

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")

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
    dag_id="generate_attraction_id_trip_to_bq_ods_dataset",
    default_args=default_args,
    description="生成tripadvisor景點ID到bigquery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)
def generate_attraction_id_trip_to_bq_dataset_task_pipeline():
    @task
    def load_ods_tripadvisor_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=PROCESSED_BUCKET, blob_name=BLOB_NAME, filetype="csv"
        )

    @task
    def generate_attraction_id_trip(table_tripadvisor) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        # 使用python做一些轉換
        trip_generate_id_dataframe = table_tripadvisor.iloc[:, [2]]
        table_tripadvisor["name"] = table_tripadvisor["name"].apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
        trip_generate_id_dataframe.loc[:, "attraction_id"] = table_tripadvisor.iloc[:, [2]]
        return trip_generate_id_dataframe

    @task
    def upload_generate_attraction_id_trip_data_to_bq(
        df, dataset_name, table_name
    ):
        """
        Upload data to bigquery.

        Args:
            df (pd.DataFrame): dataframe.
            dataset_name (str): dataset name.
            table_name (str): table name.
        """
        upload_df_to_bq(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            df=df,
            schema=[
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
            ],
        )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，建立BigQuery的Exteral Table
    table_tripadvisor = load_ods_tripadvisor_from_gcs()
    trip_generate_id_dataframe = generate_attraction_id_trip(table_tripadvisor)
    upload_generate_attraction_id_trip_data_to_bq(
        trip_generate_id_dataframe, BQ_ODS_DATASET, EXTABLE_NAME
    )

generate_attraction_id_trip_to_bq_dataset_task_pipeline()
