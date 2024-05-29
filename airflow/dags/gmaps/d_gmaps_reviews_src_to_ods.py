from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from utils.common import load_config
from utils.gcp import build_bq_from_gcs, query_bq

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator

config = load_config()
RAW_BUCKET = config["gcp"]["bucket"]["raw"]
BQ_SRC_DATASET = config["gcp"]["bigquery"]["src_dataset"]
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
current_date = datetime.now().strftime("%Y-%m-%d")
SRC_BLOB_NAME = f"{config["gcp"]["blob"]["gmaps"]["reviews"]}/{current_date}/*.parquet"
TABLE_NAME = config["gcp"]["table"]["gmaps-reviews"]
SRC_TABLE_NAME = "src-" + TABLE_NAME
ODS_TABLE_NAME = "ods-" + TABLE_NAME

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
    tags=["gmaps"],
)
def d_gmaps_reviews_src_to_ods():
    @task
    def e_create_external_table():
        build_bq_from_gcs(
            client=BQ_CLIENT,
            dataset_name=BQ_SRC_DATASET,
            table_name=SRC_TABLE_NAME,
            bucket_name=RAW_BUCKET,
            blob_name=SRC_BLOB_NAME,
        )

    @task
    def t_process_src_table() -> pd.DataFrame:
        query = f"""
        CREATE TEMP FUNCTION convertDate(published_at STRING, extracted_at TIMESTAMP)
        AS (
            CASE
                WHEN ENDS_WITH(published_at, '分鐘前') THEN CAST(TIMESTAMP_SUB(extracted_at, INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*分鐘前') AS INT64) MINUTE) AS DATE)
                WHEN ENDS_WITH(published_at, '小時前') THEN CAST(TIMESTAMP_SUB(extracted_at, INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*小時前') AS INT64) HOUR) AS DATE)
                WHEN ENDS_WITH(published_at, '天前') THEN CAST(TIMESTAMP_SUB(extracted_at, INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*天前') AS INT64) DAY) AS DATE)
                WHEN ENDS_WITH(published_at, '週前') THEN CAST(TIMESTAMP_SUB(extracted_at, INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*週前') AS INT64) * 7 DAY) AS DATE)
                WHEN ENDS_WITH(published_at, '個月前') THEN DATE_SUB(DATE(extracted_at), INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*個月前') AS INT64) MONTH)
                WHEN ENDS_WITH(published_at, '年前') THEN DATE_SUB(DATE(extracted_at), INTERVAL SAFE_CAST(REGEXP_EXTRACT(published_at, r'(\d+)\s*年前') AS INT64) YEAR)
                ELSE NULL
            END
        );
        CREATE OR REPLACE TABLE `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        PARTITION BY `published_at`
        AS
        SELECT
            `place_name`,
            `review_id`,
            `rating`,
            `review_text`,
            convertDate(`published_at`, TIMESTAMP(`extracted_at`)) AS `published_at`,
            `user_name`,
            `user_url`,
        FROM
          `{BQ_SRC_DATASET}.{SRC_TABLE_NAME}`
        WHERE
            `place_name` IS NOT NULL
            AND `review_id` IS NOT NULL
            AND `published_at` IS NOT NULL
        """
        return query_bq(BQ_CLIENT, query)

    trigger_d_gmaps_dim_time = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_dim_time",
        trigger_dag_id="d_gmaps_dim_time",
    )

    trigger_d_gmaps_dim_users = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_dim_users",
        trigger_dag_id="d_gmaps_dim_users",
    )

    trigger_d_gmaps_fact_reviews = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_fact_reviews",
        trigger_dag_id="d_gmaps_fact_reviews",
    )

    t1 = e_create_external_table()
    t2 = t_process_src_table()

    t1 >> t2
    t2 >> trigger_d_gmaps_dim_time
    t2 >> trigger_d_gmaps_dim_users
    t2 >> trigger_d_gmaps_fact_reviews


d_gmaps_reviews_src_to_ods()
