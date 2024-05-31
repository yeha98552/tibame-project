import os
import pandas as pd
import requests 

from airflow.utils.dates import days_ago
from airflow.decorators import dag, task
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from google.cloud import bigquery, storage
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    query_bq,
    query_bq_to_df,
    upload_df_to_bq,
    upload_df_to_gcs,
)

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET") #沒有處理過的資料GCS
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET") #處理過的資料GCS
TABLE_NAME = "weather"
BLOB_NAME = f"{TABLE_NAME}/weather_stand_source1.parquet" #GCS的BLOB
BLOB_name = f"{TABLE_NAME}/weather_stand_DW1.parquet" #GCS的BLOB
BQ_SRC_DATASET = os.environ.get("BIGQUERY_SRC_DATASET")
BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")

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
    dag_id="d_weather_stand_src_to_ods",
    default_args=default_args,
    description="d_weather",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)

def d_weather_stand_src_to_ods():
    @task
    def e_download_data_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=RAW_BUCKET, blob_name=BLOB_NAME
        )

    @task
    def t_data_clean(df : pd.DataFrame) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        
        df.dropna(inplace=True)
        print(df.columns)
        df = df[['站號','站名','經度','緯度','城市','地址']]
        df = df.rename({'站號':'Station_No',
                  '站名':'station_name',
                  '經度':'lat',
                  '緯度':'lon',
                  '城市':'city',
                  '地址':'address'},axis=1)

        return df
    

    @task
    def l_upload_transformed_data_to_gcs(
        df: pd.DataFrame, bucket_name: str, blob_name: str
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
        )
        print(f'file saved to {RAW_BUCKET+'/'+BLOB_NAME}')
        return 

    df = e_download_data_from_gcs()
    df1 = t_data_clean(df)
    l_upload_transformed_data_to_gcs(df1,PROCESSED_BUCKET,BLOB_name)

d_weather_stand_src_to_ods()
