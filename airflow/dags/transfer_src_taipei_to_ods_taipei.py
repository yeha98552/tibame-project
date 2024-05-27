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
BLOB_NAME_SRC = f"{TABLE_NAME_SRC}/src_taipei.csv"
TABLE_NAME_ODS = "ods_attraction"
BLOB_NAME_ODS = f"{TABLE_NAME_ODS}/ods_taipei_number_people.csv"
EXTABLE_NAME_ODS = "ods_taipei_number_people"

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
    dag_id="transfer_src_taipei_to_ods_taipei_number_people",
    default_args=default_args,
    description="清洗交通部的台北原始資料並Tabular",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)
def transfer_src_taipei_to_gcs_task_pipeline():
    @task
    def load_src_taipei_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=RAW_BUCKET, blob_name=BLOB_NAME_SRC, filetype="csv"
        )

    @task
    def clean_src_taipei(table_taipei) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        # 使用python做一些轉換
        table_taipei_eng_name = table_taipei.iloc[[0], 2:].transpose().reset_index()
        table_taipei_eng_name.rename(columns={"index": "name_zh", 0: "name_en"}, inplace=True)
        table_taipei_eng_name.to_csv("/opt/airflow/dags/ods_taipei_name_en_zh.csv", index=0, encoding="utf-8")
        taipei_droprow = table_taipei.drop([0]).reset_index(drop=True)
        taipei_droprow.rename(columns={"年度": "year", "月份": "month"}, inplace=True)
        taipei_droprow["year"] = taipei_droprow["year"].apply(lambda x: x.split("(")[1].split(")")[0].strip())
        taipei_droprow = taipei_droprow.dropna()
        taipei_melt = pd.melt(taipei_droprow, id_vars=["year", "month"], var_name="name", value_name="number_people")
        taipei_melt["number_people"] = taipei_melt["number_people"].astype("int")
        taipei_melt["year"] = taipei_melt["year"].astype("int")
        taipei_melt["month"] = taipei_melt["month"].astype("int")
        return taipei_melt


    @task
    def upload_transformed_src_taipei_to_gcs_as_ods_taipei_number_people(
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
    def create_ods_taipei_number_people_bq_external_table(
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
                bigquery.SchemaField("year", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("month", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("number_people", "INT64", mode="NULLABLE"),
            ],
            filetype=filetype,
        )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，再次上傳到GCS，最後建立BigQuery的Exteral Table
    table_taipei_number_people = load_src_taipei_from_gcs()
    taipei_melt = clean_src_taipei(table_taipei_number_people)
    upload_transformed_data_task = upload_transformed_src_taipei_to_gcs_as_ods_taipei_number_people(
        taipei_melt, PROCESSED_BUCKET, BLOB_NAME_ODS, "csv"
    )
    create_bq_external_table_task = create_ods_taipei_number_people_bq_external_table(
        PROCESSED_BUCKET, BLOB_NAME_ODS, BQ_ODS_DATASET, EXTABLE_NAME_ODS, "csv"
    )

    # Set dependencies
    create_bq_external_table_task.set_upstream(upload_transformed_data_task)

transfer_src_taipei_to_gcs_task_pipeline()
