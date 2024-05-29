import os
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery, storage
from utils.common import rename_place_id
from utils.gcp import build_bq_from_gcs, download_df_from_gcs, upload_df_to_gcs

from airflow.decorators import dag, task

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BLOB_NAME = "tripadvisor/src_tripadvisor.csv"
GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval=None,  # Because this is triggered by another DAG
    catchup=False,
    tags=["tripadvisor"],
)
def d_tripadvisor_src_to_ods():
    @task
    def e_download_tripadvisor_from_gcs(
        bucket_name: str, blob_name: str
    ) -> pd.DataFrame:
        return download_df_from_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            filetype="csv",
        )

    @task
    def t_remove_tripadvisor_unnamed_column(df: pd.DataFrame) -> pd.DataFrame:
        # Assuming the unnamed column follows the pattern 'Unnamed: X' where X is a number
        df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
        return df

    @task
    def t_rename_tripadvisor_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "景點介紹": "info",
                "景點照": "photo",
                "景點名": "attraction_name",
                "評分": "rating",
                "評論數": "total_reviews",
                "類型": "categories",
            },
            inplace=True,
        )
        return df

    @task
    def t_remove_tripadvisor_null_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.dropna(how="all")

    @task
    def t_remove_tripadvisor_duplicate_rows(df: pd.DataFrame) -> pd.DataFrame:
        return df.drop_duplicates(subset=["attraction_name", "rating"], keep="first")

    @task
    def t_rename_tripadvisor_rating_values(df: pd.DataFrame) -> pd.DataFrame:
        # rating to float, if can't convert, set NaN
        df["rating"] = pd.to_numeric(
            df["rating"].str.split("分 ").str[0], errors="coerce"
        )
        # remove rows with NaN rating
        df = df.dropna(subset=["rating"])
        return df

    @task
    def t_remove_tripadvisor_total_reviews_values(df: pd.DataFrame) -> pd.DataFrame:
        df["total_reviews"] = pd.to_numeric(df["total_reviews"], errors="coerce")
        df = df.dropna(subset=["total_reviews"])
        return df

    @task
    def t_convert_tripadvisor_categories_to_list(df: pd.DataFrame) -> pd.DataFrame:
        # if type is not str, drop the row
        df = df[df["categories"].apply(lambda x: isinstance(x, str))]

        df["categories"] = df["categories"].apply(
            lambda x: x.replace(" • ", ",") if " • " in x else x
        )
        return df

    @task
    def t_add_attraction_id(df: pd.DataFrame) -> pd.DataFrame:
        df["attraction_id"] = df["attraction_name"].apply(
            lambda x: rename_place_id(str(x))
        )
        return df

    @task
    def l_upload_tripadvisor_to_gcs(df: pd.DataFrame, bucket_name: str, blob_name: str):
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=bucket_name,
            blob_name=blob_name,
            df=df,
        )

    @task
    def l_create_tripadvisor_bq_external_table(
        dataset_name: str, table_name: str, bucket_name: str, blob_name: str
    ):
        build_bq_from_gcs(
            client=BQ_CLIENT,
            dataset_name=dataset_name,
            table_name=table_name,
            bucket_name=bucket_name,
            blob_name=blob_name,
            schema=[
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("info", "STRING"),
                bigquery.SchemaField("photo", "STRING"),
                bigquery.SchemaField("attraction_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("rating", "FLOAT"),
                bigquery.SchemaField("total_reviews", "INTEGER"),
                bigquery.SchemaField("categories", "STRING"),
            ],
        )

    t1 = e_download_tripadvisor_from_gcs(
        bucket_name=RAW_BUCKET,
        blob_name="src_attraction/src_tripadvisor.csv",
    )
    t2 = t_remove_tripadvisor_unnamed_column(t1)
    t3 = t_rename_tripadvisor_columns(t2)
    t4 = t_remove_tripadvisor_null_rows(t3)
    t5 = t_remove_tripadvisor_duplicate_rows(t4)
    t6 = t_rename_tripadvisor_rating_values(t5)
    t7 = t_remove_tripadvisor_total_reviews_values(t6)
    t8 = t_convert_tripadvisor_categories_to_list(t7)
    t9 = t_add_attraction_id(t8)
    l_upload_tripadvisor_to_gcs(
        t9,
        PROCESSED_BUCKET,
        "tripadvisor/ods_tripadvisor_info.parquet",
    ) >> l_create_tripadvisor_bq_external_table(
        dataset_name=BQ_ODS_DATASET,
        table_name="ods_tripadvisor_info",
        bucket_name=PROCESSED_BUCKET,
        blob_name="tripadvisor/ods_tripadvisor_info.parquet",
    )


d_tripadvisor_src_to_ods()
