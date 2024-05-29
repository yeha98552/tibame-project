from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.common import load_config, table_name_with_env
from utils.gcp import query_bq

from airflow.decorators import dag, task

config = load_config()
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
BQ_DIM_DATASET = config["gcp"]["bigquery"]["dim_dataset"]
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-reviews"]
DIM_TABLE_NAME = table_name_with_env("dim-time", config["env"])

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
def d_gmaps_dim_time():
    @task
    def l_dim_time():
        query = f"""
        CREATE OR REPLACE TABLE `{BQ_DIM_DATASET}.{DIM_TABLE_NAME}` AS
        SELECT
          DISTINCT
          `published_at` AS date,
          EXTRACT(YEAR FROM `published_at`) AS year,
          EXTRACT(MONTH FROM `published_at`) AS month,
          EXTRACT(DAY FROM `published_at`) AS day,
          CASE
            WHEN EXTRACT(MONTH FROM `published_at`) IN (1, 2, 3) THEN 1
            WHEN EXTRACT(MONTH FROM `published_at`) IN (4, 5, 6) THEN 2
            WHEN EXTRACT(MONTH FROM `published_at`) IN (7, 8, 9) THEN 3
            WHEN EXTRACT(MONTH FROM `published_at`) IN (10, 11, 12) THEN 4
          END AS quarter
        FROM
          `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        """
        query_bq(BQ_CLIENT, query)
        return f"{DIM_TABLE_NAME} created."

    l_dim_time()


d_gmaps_dim_time()
