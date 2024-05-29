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
BQ_DIM_DATASET = os.environ.get("BIGQUERY_DIM_DATASET")

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
    dag_id="create_dim_attraction_type",
    default_args=default_args,
    description="建立景點類型查詢表",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dim"],
)
def create_dim_attraction_type_pipeline():
    wait_for_ods_tripadvisor = ExternalTaskSensor(
        task_id="wait_for_ods_tripadvisor",
        external_dag_id="transfer_src_tripadvisor_to_ods_tripadvisor",
        external_task_id="create_ods_tripadvisor_bq_external_table",
        poke_interval=120,  # 每 120 秒檢查一次
        timeout=3600,  # 總等待時間為 3600 秒
        mode="poke",
    )

    wait_for_dim_attraction_hashtag = ExternalTaskSensor(
        task_id="wait_for_dim_attraction_hashtag",
        external_dag_id="create_dim_attraction_hashtag",
        external_task_id="update_and_insert_data_to_dim_attraction_hashtag",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    wait_for_dim_type = ExternalTaskSensor(
        task_id="wait_for_dim_type",
        external_dag_id="create_dim_type",
        external_task_id="create_dim_type_bq_external_table",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    all_sensors_complete = DummyOperator(task_id="all_sensors_complete")

    @task
    def generate_dim_attraction_type_data() -> pd.DataFrame:
        """
        Join data from bigquery.

        Args:
            dataset_name (str): dataset name.

        Returns:
            pd.DataFrame: joined dataframe.
        """
        dataset1 = BQ_ODS_DATASET
        dataset2 = BQ_DIM_DATASET
        table_name1 = "ods_tripadvisor"
        table_name2 = "dim_attraction_hashtag"
        table_name3 = "dim_type"

        query = f"""
        select distinct attraction_id, type_id
        from `{dataset2}.{table_name2}`
        left join `{dataset1}.{table_name1}`
        using (name)
        left join `{dataset2}.{table_name3}`
        on `{dataset1}.{table_name1}`.type = `{dataset2}.{table_name3}`.name
        where type_id is not null;
        """
        return query_bq_to_df(client=BQ_CLIENT, sql_query=query)

    @task
    def create_dim_attraction_type_bq_external_table(
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
                bigquery.SchemaField("type_id", "STRING", mode="REQUIRED"),
            ],
        )

    all_sensors_complete.set_upstream([wait_for_ods_tripadvisor, wait_for_dim_attraction_hashtag, wait_for_dim_type])
    
    generate_dim_attraction_type_data().set_upstream(all_sensors_complete)

    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    generate_dim_attraction_type_data_task = generate_dim_attraction_type_data()
    create_dim_attraction_type_bq_external_table_task = create_dim_attraction_type_bq_external_table(
        generate_dim_attraction_type_data_task, BQ_DIM_DATASET, "dim_attraction_type"
    )

    # Set dependencies
    create_dim_attraction_type_bq_external_table_task.set_upstream(generate_dim_attraction_type_data_task)

create_dim_attraction_type_pipeline()
