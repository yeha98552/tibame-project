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
BLOB_NAME = f"{TABLE_NAME}/ods_taipei_name_en_zh.csv"
EXTABLE_NAME = "ods_attraction_generate_id_taipei"

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
    dag_id="generate_attraction_id_taipei_to_bq_ods_dataset",
    default_args=default_args,
    description="生成交通部的台北景點ID到bigquery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)
def generate_attraction_id_taipei_to_bq_dataset_task_pipeline():
    @task
    def load_ods_taipei_name_en_zh_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=PROCESSED_BUCKET, blob_name=BLOB_NAME, filetype="csv"
        )

    @task
    def generate_attraction_id_taipei(table_taipei_name_en_zh) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        # 使用python做一些轉換
        table_taipei_name_en_zh["name_en"] = table_taipei_name_en_zh["name_zh"].apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest())
        table_taipei_name_en_zh.rename(columns={"name_en": "attraction_id", "name_zh": "name"}, inplace=True)
        table_name_generate_id_reloc = table_taipei_name_en_zh.iloc[:, [1, 0]]
        return table_name_generate_id_reloc
    @task
    def upload_generate_attraction_id_taipei_data_to_bq(
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
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
            ],
        )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，建立BigQuery的Exteral Table
    table_taipei_name_en_zh = load_ods_taipei_name_en_zh_from_gcs()
    table_name_generate_id_reloc = generate_attraction_id_taipei(table_taipei_name_en_zh)
    upload_generate_attraction_id_taipei_data_to_bq(
        table_name_generate_id_reloc, BQ_ODS_DATASET, EXTABLE_NAME
    )

generate_attraction_id_taipei_to_bq_dataset_task_pipeline()
