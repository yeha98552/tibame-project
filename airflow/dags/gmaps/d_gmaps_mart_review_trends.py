from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.common import load_config, table_name_with_env
from utils.gcp import query_bq

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor

config = load_config()
BQ_DIM_DATASET = config["gcp"]["bigquery"]["dim_dataset"]
BQ_FACT_DATASET = config["gcp"]["bigquery"]["fact_dataset"]
BQ_MART_DATASET = config["gcp"]["bigquery"]["mart_dataset"]
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-reviews"]
fact_reviews_table = table_name_with_env("fact-gmaps-reviews", config["env"])
dim_places_table = table_name_with_env("dim-gmaps-places", config["env"])
dim_time_table = table_name_with_env("dim-time", config["env"])
MART_TABLE_NAME = table_name_with_env("mart-reviews-trends", config["env"])

BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


@dag(
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["gmaps"],
)
def d_gmaps_mart_review_trends():
    wait_for_fact_reviews = ExternalTaskSensor(
        task_id="wait_for_fact_reviews",
        external_dag_id="d_gmaps_fact_reviews",
        external_task_id="etl_load_reviews",
        poke_interval=120,  # 每 120 秒檢查一次
        timeout=3600,  # 總等待時間為 3600 秒
        mode="poke",
    )

    wait_for_dim_users = ExternalTaskSensor(
        task_id="wait_for_dim_users",
        external_dag_id="d_gmaps_dim_users",
        external_task_id="l_dim_users",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    wait_for_dim_time = ExternalTaskSensor(
        task_id="wait_for_dim_time",
        external_dag_id="d_gmaps_dim_time",
        external_task_id="l_dim_time",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    # Dummy task to synchronize after all sensors
    all_sensors_complete = DummyOperator(task_id="all_sensors_complete")

    @task
    def l_mart_review_trends():
        query = f"""
        CREATE OR REPLACE TABLE `{BQ_MART_DATASET}.{MART_TABLE_NAME}` AS
        SELECT
          p.`city`,
          p.`region`,
          p.`place_id`,
          p.`place_name`,
          p.`main_category`,
          p.`latitude`,
          p.`longitude`,
          t.`year`,
          t.`month`,
          t.`quarter`,
          t.`date`,
          COUNT(r.`review_id`) AS `total_reviews`,
          ROUND(AVG(r.`rating`), 2) AS `avg_rating`,
        FROM
          `{BQ_FACT_DATASET}.{fact_reviews_table}` r
        JOIN
          `{BQ_DIM_DATASET}.{dim_places_table}` p
          ON r.`place_name` = p.`place_name`
        JOIN
          `{BQ_DIM_DATASET}.{dim_time_table}` t
          ON r.`published_at` = t.`date`
        GROUP BY
          p.`city`,
          p.`region`,
          p.`place_id`,
          p.`place_name`,
          p.`main_category`,
          p.`latitude`,
          p.`longitude`,
          t.`year`,
          t.`month`,
          t.`quarter`,
          t.`date`
        """
        query_bq(BQ_CLIENT, query)
        return f"{MART_TABLE_NAME} created."

    (
        [wait_for_fact_reviews, wait_for_dim_users, wait_for_dim_time]
        >> all_sensors_complete
        >> l_mart_review_trends()
    )


d_gmaps_mart_review_trends()
