import os
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery
from utils.gcp import (
    query_bq_to_df,
    upload_df_to_bq,
)

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")
BQ_FACT_DATASET = os.environ.get("BIGQUERY_FACT_DATASET")

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
    dag_id="create_fact_attraction_history",
    default_args=default_args,
    description="建立景點歷史人次表",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["fact"],
)
def create_fact_attraction_history_pipeline():
    wait_for_ods_taipei_number_people = ExternalTaskSensor(
        task_id="wait_for_ods_taipei_number_people",
        external_dag_id="transfer_src_taipei_to_ods_taipei_number_people",
        external_task_id="create_ods_taipei_number_people_bq_external_table",
        poke_interval=120,  # 每 120 秒檢查一次
        timeout=3600,  # 總等待時間為 3600 秒
        mode="poke",
    )

    wait_for_ods_attraction_generate_id_taipei = ExternalTaskSensor(
        task_id="wait_for_ods_attraction_generate_id_taipei",
        external_dag_id="generate_attraction_id_taipei_to_bq_ods_dataset",
        external_task_id="upload_generate_attraction_id_taipei_data_to_bq",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    all_sensors_complete = DummyOperator(task_id="all_sensors_complete")

    @task
    def generate_fact_attraction_history_data() -> pd.DataFrame:
        """
        Join data from bigquery.

        Args:
            dataset_name (str): dataset name.

        Returns:
            pd.DataFrame: joined dataframe.
        """
        dataset = BQ_ODS_DATASET
        table_name1 = "ods_taipei_number_people"
        table_name2 = "ods_attraction_generate_id_taipei"

        query = f"""
        select attraction_id, year, month, number_people from `{dataset}.{table_name1}`
        join `{dataset}.{table_name2}`
        using (name)
        where month is not null;
        """
        return query_bq_to_df(client=BQ_CLIENT, sql_query=query)

    @task
    def create_fact_attraction_history_bq_external_table(
        df, dataset_name, table_name
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
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("year", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("month", "INT64", mode="NULLABLE"),
                bigquery.SchemaField("number_people", "INT64", mode="NULLABLE"),
            ],
        )

    all_sensors_complete.set_upstream([wait_for_ods_taipei_number_people, wait_for_ods_attraction_generate_id_taipei])
    
    generate_fact_attraction_history_data().set_upstream(all_sensors_complete)

    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    generate_fact_attraction_history_data_task = generate_fact_attraction_history_data()
    create_fact_attraction_history_bq_external_table_task = create_fact_attraction_history_bq_external_table(
        generate_fact_attraction_history_data_task, BQ_FACT_DATASET, "fact_attraction_history"
    )

    # Set dependencies
    create_fact_attraction_history_bq_external_table_task.set_upstream(generate_fact_attraction_history_data_task)

create_fact_attraction_history_pipeline()
