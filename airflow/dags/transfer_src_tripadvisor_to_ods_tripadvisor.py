import os
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery, storage
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    upload_df_to_gcs,
)

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")

TABLE_NAME_SRC = "src_attraction"
BLOB_NAME_SRC = f"{TABLE_NAME_SRC}/src_tripadvisor.csv"
TABLE_NAME_ODS = "ods_attraction"
BLOB_NAME_ODS = f"{TABLE_NAME_ODS}/ods_tripadvisor.csv"
EXTABLE_NAME_ODS = "ods_tripadvisor"

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
    dag_id="transfer_src_tripadvisor_to_ods_tripadvisor",
    default_args=default_args,
    description="清洗tripadvisor原始資料並Tabular",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)
def transfer_src_tripadvisor_to_gcs_task_pipeline():
    @task
    def load_src_tripadvisor_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=RAW_BUCKET, blob_name=BLOB_NAME_SRC, filetype="csv"
        )

    @task
    def clean_src_tripadvisor(table_tripadvisor) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        # 使用python做一些轉換
        table_tripadvisor.rename(columns={"景點介紹": "info", "景點照": "photo_url", "景點名": "name", "評分": "score", "評論數": "crit", "類型": "type"}, inplace=True)
        trip_dropna = table_tripadvisor.dropna(subset=["info", "photo_url", "score", "crit", "type"])
        trip_dropcol = trip_dropna.drop(["Unnamed: 6"], axis=1)
        trip_dropcol = trip_dropcol.drop_duplicates(subset=["name"])
        trip_dropcol = trip_dropcol.drop([462])
        trip_dropcol["crit"] = trip_dropcol["crit"].astype("int")
        trip_dropcol["score"] = trip_dropcol["score"].apply(lambda x: x.split(" ")[0].strip())
        trip_dropcol["score"] = trip_dropcol["score"].astype("float")
        trip_dropcol["type"] = trip_dropcol["type"].apply(lambda x: [i.strip() for i in x.split("•")])
        trip_explode = trip_dropcol.explode("type").reset_index(drop=True)
        return trip_explode

    @task
    def upload_transformed_src_tripadvisor_to_gcs_as_ods_tripadvisor(
        df, bucket_name, blob_name, filetype
    ):
        """
        Upload transformed data to GCS.

        Args:
            df (pd.DataFrame): transformed dataframe.
            bucket_name (str): bucket name.
            blob_name (str): blob name.
        """
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
            filetype=filetype,
        )

    @task
    def create_ods_tripadvisor_bq_external_table(
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
                bigquery.SchemaField("info", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("photo_url", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("score", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("crit", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("type", "STRING", mode="NULLABLE"),
            ],
            filetype=filetype,
        )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，再次上傳到GCS，最後建立BigQuery的Exteral Table
    table_tripadvisor = load_src_tripadvisor_from_gcs()
    trip_explode = clean_src_tripadvisor(table_tripadvisor)
    upload_transformed_data_task = upload_transformed_src_tripadvisor_to_gcs_as_ods_tripadvisor(
        trip_explode, PROCESSED_BUCKET, BLOB_NAME_ODS, "csv"
    )
    create_bq_external_table_task = create_ods_tripadvisor_bq_external_table(
        PROCESSED_BUCKET, BLOB_NAME_ODS, BQ_ODS_DATASET, EXTABLE_NAME_ODS, "csv"
    )

    # Set dependencies
    create_bq_external_table_task.set_upstream(upload_transformed_data_task)

transfer_src_tripadvisor_to_gcs_task_pipeline()
