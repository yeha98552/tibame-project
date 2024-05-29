import os
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery, storage
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    query_bq,
    query_bq_to_df,
    upload_df_to_bq,
    upload_df_to_gcs,
)

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

RAW_BUCKET = os.environ.get("GCP_GCS_RAW_BUCKET")
PROCESSED_BUCKET = os.environ.get("GCP_GCS_PROCESSED_BUCKET")
TABLE_NAME = "yellow_taxi"
BLOB_NAME = f"{TABLE_NAME}/yellow_tripdata_2021-05.parquet"
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
    dag_id="d_example_data_pipeline",
    default_args=default_args,
    description="An example data pipeline",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["example"],
)
def d_example_data_pipeline():
    @task
    def e_download_data_from_gcs():
        return download_df_from_gcs(
            client=GCS_CLIENT, bucket_name=RAW_BUCKET, blob_name=BLOB_NAME
        )

    @task
    def t_data_clean(df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform data (drop duplicates, drop na, drop columns).

        Args:
            df (pd.DataFrame): input dataframe.

        Returns:
            pd.DataFrame: transformed dataframe.
        """
        # 使用python做一些轉換
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df.drop(
            columns=[
                "airport_fee",
                "congestion_surcharge",
                "improvement_surcharge",
                "extra",
                "mta_tax",
                "store_and_fwd_flag",
            ],
            inplace=True,
        )
        df.reset_index(drop=True, inplace=True)
        return df[df["fare_amount"] < 10]

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

    @task
    def l_create_bq_external_table(
        bucket_name: str, blob_name: str, dataset_name: str, table_name: str
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
                bigquery.SchemaField(
                    "VendorID", "INTEGER", mode="REQUIRED"
                ),  # NOT NULL
                bigquery.SchemaField(
                    "tpep_pickup_datetime", "TIMESTAMP", mode="NULLABLE"
                ),
                bigquery.SchemaField(
                    "tpep_dropoff_datetime", "TIMESTAMP", mode="NULLABLE"
                ),
                bigquery.SchemaField("passenger_count", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("trip_distance", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("RatecodeID", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("PULocationID", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("DOLocationID", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("payment_type", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("fare_amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("tip_amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("tolls_amount", "FLOAT", mode="NULLABLE"),
                bigquery.SchemaField("total_amount", "FLOAT", mode="NULLABLE"),
            ],
        )

    @task
    def t_lookup_data(dataset_name: str, table_name: str):
        """
        Lookup data from bigquery.

        Args:
            dataset_name (str): dataset name.
            table_name (str): table name.
        """
        # 通常應該不會用來查詢，只是簡單示範可以執行sql，可能可以替換成Insert、Update之類的，沒有要取得回傳值成pd.DataFrame的
        query = f"SELECT * FROM `{dataset_name}.{table_name}` WHERE fare_amount = 1"
        results = query_bq(client=BQ_CLIENT, query=query)
        # 執行結果輸出
        for row in results:
            print(row)

    @task
    def t_join_data() -> pd.DataFrame:
        """
        Join data from bigquery.

        Args:
            dataset_name (str): dataset name.

        Returns:
            pd.DataFrame: joined dataframe.
        """
        # 這裡使用兩個相同的資料表名稱, 只是示範，應該是要兩個不同的資料表名稱操作才有意義
        dataset1 = dataset2 = BQ_ODS_DATASET
        table_name1 = table_name2 = f"ods_{TABLE_NAME}"
        query = f"""
        SELECT t1.tpep_pickup_datetime, t1.trip_distance, t1.fare_amount
        FROM `{dataset1}.{table_name1}` AS t1
        INNER JOIN `{dataset2}.{table_name2}` AS t2
        ON t1.VendorID = t2.VendorID
        WHERE t1.fare_amount = 1
        """
        return query_bq_to_df(client=BQ_CLIENT, query=query)

    @task
    def l_upload_joined_data_to_bq(
        df: pd.DataFrame, dataset_name: str, table_name: str
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
                bigquery.SchemaField("tpep_pickup_datetime", "TIMESTAMP"),
                bigquery.SchemaField("trip_distance", "FLOAT"),
                bigquery.SchemaField("fare_amount", "FLOAT"),
            ],
        )

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，再次上傳到GCS，最後建立BigQuery的Exteral Table
    data = e_download_data_from_gcs()
    transformed_data = t_data_clean(data)
    upload_transformed_data_task = l_upload_transformed_data_to_gcs(
        transformed_data, PROCESSED_BUCKET, f"{TABLE_NAME}_processed"
    )
    create_bq_external_table_task = l_create_bq_external_table(
        PROCESSED_BUCKET, f"{TABLE_NAME}_processed", BQ_ODS_DATASET, f"ods_{TABLE_NAME}"
    )
    # 查詢BigQuery的Exteral Table
    lookup_data_task = t_lookup_data(BQ_ODS_DATASET, f"ods_{TABLE_NAME}")
    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    join_data_task = t_join_data()
    upload_to_bigquery_task = l_upload_joined_data_to_bq(
        join_data_task, BQ_DIM_DATASET, "dim_taxi"
    )

    # Set dependencies
    create_bq_external_table_task.set_upstream(upload_transformed_data_task)
    lookup_data_task.set_upstream(create_bq_external_table_task)
    join_data_task.set_upstream(create_bq_external_table_task)
    upload_to_bigquery_task.set_upstream(join_data_task)


d_example_data_pipeline()
