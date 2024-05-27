import os
from datetime import timedelta

import pandas as pd
from google.cloud import bigquery
from utils.gcp import (
    query_bq_to_df,
    upload_df_to_bq,
    query_bq,
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
    dag_id="create_dim_attraction_detail",
    default_args=default_args,
    description="建立景點介紹表",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dim"],
)
def create_dim_attraction_detail_pipeline():
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

    all_sensors_complete = DummyOperator(task_id="all_sensors_complete")

    @task
    def generate_dim_attraction_detail_data() -> pd.DataFrame:
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

        query = f"""
        select distinct attraction_id, info, photo_url
        from `{dataset2}.{table_name2}`
        left join `{dataset1}.{table_name1}`
        using (name);
        """
        return query_bq_to_df(client=BQ_CLIENT, sql_query=query)

    @task
    def create_dim_attraction_detail_bq_external_table(
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
                bigquery.SchemaField("info", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("photo_url", "STRING", mode="NULLABLE"),
            ],
        )

    @task
    def delete_data_from_dim_attraction_detail():
        """
        Lookup data from bigquery.

        Args:
            dataset_name (str): dataset name.
            table_name (str): table name.
        """
        dataset = BQ_DIM_DATASET
        table_name = "dim_attraction_detail"

        query = f"""
        delete from `{dataset}.{table_name}`
        where attraction_id = "7dade2c9ab2e673b24dd1e0df4c4e5cf"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "d63de8ae472edab6b8139e00c46e5b7c"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "f269baf7d30b322b935c39375df673a5"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "aee9144e107d102cc6be59e09b554569"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "0087526cbae9425e079f97df85049652"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "c293892599dc4be7f35c229376203796"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "12b744a0dfc83b80549d8870770af9d2"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "c1c7a1bab25a9a9d26f3700c9ae50dd8"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "2dc990b1124bd6cadb57c9941c2316e9"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "fc73810e75cbcb35391b0f013cfabeab"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "1e3f7c73861a7f3e1e67b75694d12dd6"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "d792ff6717863a1aee8a567ecae503c2"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "7b4cdc4d6bb2fd9fac8e962fc5fcc927"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "3915592d5ee993e9b4e4442cc973cd95"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "1d56e443604354ed396ea153f8b55846"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "c124cd7a866f61e3bda0a2eadcff2c46"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "351bad3e4c95534ee9b4a21084bb4f52"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "5bafa293b96ff560a87283e618e83858"
        and info is null;

        delete from `{dataset}.{table_name}`
        where attraction_id = "7a4324592bc3a407cb17135133c6e0b3"
        and info is null;
        """
        query_bq(client=BQ_CLIENT, sql_query=query)

    all_sensors_complete.set_upstream([wait_for_ods_tripadvisor, wait_for_dim_attraction_hashtag])
    
    generate_dim_attraction_detail_data().set_upstream(all_sensors_complete)

    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    generate_dim_attraction_detail_data_task = generate_dim_attraction_detail_data()
    create_dim_attraction_detail_bq_external_table_task = create_dim_attraction_detail_bq_external_table(
        generate_dim_attraction_detail_data_task, BQ_DIM_DATASET, "dim_attraction_detail"
    )
    delete_data_from_dim_attraction_detail_task = delete_data_from_dim_attraction_detail()

    # Set dependencies
    create_dim_attraction_detail_bq_external_table_task.set_upstream(generate_dim_attraction_detail_data_task)
    delete_data_from_dim_attraction_detail_task.set_upstream(create_dim_attraction_detail_bq_external_table_task)

create_dim_attraction_detail_pipeline()
