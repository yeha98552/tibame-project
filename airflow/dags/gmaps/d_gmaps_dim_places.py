from datetime import datetime, timedelta

from google.cloud import bigquery
from utils.common import load_config, table_name_with_env
from utils.gcp import query_bq

from airflow.decorators import dag, task

config = load_config()
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
BQ_DIM_DATASET = config["gcp"]["bigquery"]["dim_dataset"]
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-places"]
DIM_TABLE_NAME = table_name_with_env("dim-gmaps-places", config["env"])

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
def d_gmaps_dim_places():
    @task
    def l_dim_places():
        query = f"""
        CREATE OR REPLACE TABLE `{BQ_DIM_DATASET}.{DIM_TABLE_NAME}` AS
        SELECT DISTINCT
          `place_id`,
          `place_name`,
          `detailed_address`.`country_code` AS `country`,
          REGEXP_EXTRACT(`detailed_address`.`state`, r"^(.*?[市|縣])") AS city,
          REGEXP_EXTRACT(`detailed_address`.`state`, r"(?:[市|縣])(.*)$") AS region,
          `coordinates`.`latitude` AS `latitude`,
          `coordinates`.`longitude` AS `longitude`,
          `address`,
          `google_place_url`,
          `main_category`,
        FROM
          `{BQ_ODS_DATASET}.{ODS_TABLE_NAME}`
        """
        query_bq(BQ_CLIENT, query)
        return f"{DIM_TABLE_NAME} created."

    l_dim_places()


d_gmaps_dim_places()
