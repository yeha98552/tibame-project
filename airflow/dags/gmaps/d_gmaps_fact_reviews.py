from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from utils.common import load_config, table_name_with_env
from utils.gcp import query_bq

from airflow.decorators import dag, task

config = load_config()
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
BQ_FACT_DATASET = config["gcp"]["bigquery"]["fact_dataset"]
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-reviews"]
FACT_TABLE_NAME = table_name_with_env("fact-gmaps-reviews", config["env"])

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
def d_gmaps_fact_reviews():
    @task
    def etl_load_reviews() -> pd.DataFrame:
        query = f"""
        CREATE OR REPLACE TABLE `{BQ_FACT_DATASET}.{FACT_TABLE_NAME}`
        PARTITION BY `published_at`
        AS
        SELECT DISTINCT
          `review_id`,
          `place_name`,
          `user_name`,
          `rating`,
          `published_at`,
          `review_text`,
        FROM
          `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        """
        query_bq(client=BQ_CLIENT, sql_query=query)
        return f"{FACT_TABLE_NAME} created."

    etl_load_reviews()


d_gmaps_fact_reviews()
