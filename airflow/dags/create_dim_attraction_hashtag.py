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
    dag_id="create_dim_attraction_hashtag",
    default_args=default_args,
    description="建立景點hashtag查詢表",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["dim"],
)
def create_dim_attraction_hashtag_pipeline():
    wait_for_ods_tripadvisor = ExternalTaskSensor(
        task_id="wait_for_ods_tripadvisor",
        external_dag_id="transfer_src_tripadvisor_to_ods_tripadvisor",
        external_task_id="create_ods_tripadvisor_bq_external_table",
        poke_interval=120,  # 每 120 秒檢查一次
        timeout=3600,  # 總等待時間為 3600 秒
        mode="poke",
    )

    wait_for_ods_generate_attraction_id_taipei = ExternalTaskSensor(
        task_id="wait_for_ods_generate_attraction_id_taipei",
        external_dag_id="generate_attraction_id_taipei_to_bq_ods_dataset",
        external_task_id="upload_generate_attraction_id_taipei_data_to_bq",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    wait_for_ods_generate_attraction_id_trip = ExternalTaskSensor(
        task_id="wait_for_ods_generate_attraction_id_trip",
        external_dag_id="generate_attraction_id_trip_to_bq_ods_dataset",
        external_task_id="upload_generate_attraction_id_trip_data_to_bq",
        poke_interval=120,
        timeout=3600,
        mode="poke",
    )

    all_sensors_complete = DummyOperator(task_id="all_sensors_complete")
    
    @task
    def generate_dim_attraction_hashtag_data() -> pd.DataFrame:
        """
        Join data from bigquery.

        Args:
            dataset_name (str): dataset name.

        Returns:
            pd.DataFrame: joined dataframe.
        """
        dataset = BQ_ODS_DATASET
        table_name1 = "ods_tripadvisor"
        table_name2 = "ods_attraction_generate_id_taipei"
        table_name3 = "ods_attraction_generate_id_trip"

        query = f"""
        select name, attraction_id from `{dataset}.{table_name2}`
        union distinct
        select distinct name, attraction_id from `{dataset}.{table_name1}`
        join `{dataset}.{table_name3}`
        using (name);
        """
        return query_bq_to_df(client=BQ_CLIENT, sql_query=query)

    @task
    def create_dim_attraction_hashtag_bq_external_table(
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
                bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("attraction_id", "STRING", mode="REQUIRED"),
            ],
        )

    @task
    def update_and_insert_data_to_dim_attraction_hashtag():
        """
        Lookup data from bigquery.

        Args:
            dataset_name (str): dataset name.
            table_name (str): table name.
        """
        dataset1 = BQ_ODS_DATASET
        dataset2 = BQ_DIM_DATASET
        table_name1 = "ods_attraction_generate_id_hashtag"
        table_name2 = "dim_attraction_hashtag"

        query = f"""
        update `{dataset2}.{table_name2}`
        set attraction_id = "7dade2c9ab2e673b24dd1e0df4c4e5cf"
        where name = "龍鳳谷硫磺谷遊憩區";

        update `{dataset2}.{table_name2}`
        set attraction_id = "d63de8ae472edab6b8139e00c46e5b7c"
        where name = "陽明山冷水坑";

        update `{dataset2}.{table_name2}`
        set attraction_id = "f269baf7d30b322b935c39375df673a5"
        where name = "陽明山擎天崗";

        update `{dataset2}.{table_name2}`
        set attraction_id = "aee9144e107d102cc6be59e09b554569"
        where name = "台北市立美術館";

        update `{dataset2}.{table_name2}`
        set attraction_id = "0087526cbae9425e079f97df85049652"
        where name = "忠烈祠";

        update `{dataset2}.{table_name2}`
        set attraction_id = "c293892599dc4be7f35c229376203796"
        where name = "國立台灣科學教育館";

        update `{dataset2}.{table_name2}`
        set attraction_id = "12b744a0dfc83b80549d8870770af9d2"
        where name = "台北市立天文科學教育館";

        update `{dataset2}.{table_name2}`
        set attraction_id = "c1c7a1bab25a9a9d26f3700c9ae50dd8"
        where name = "國父紀念館";

        update `{dataset2}.{table_name2}`
        set attraction_id = "2dc990b1124bd6cadb57c9941c2316e9"
        where name = "自來水園區";

        update `{dataset2}.{table_name2}`
        set attraction_id = "fc73810e75cbcb35391b0f013cfabeab"
        where name = "中正紀念堂";

        update `{dataset2}.{table_name2}`
        set attraction_id = "1e3f7c73861a7f3e1e67b75694d12dd6"
        where name = "台北孔廟";

        update `{dataset2}.{table_name2}`
        set attraction_id = "d792ff6717863a1aee8a567ecae503c2"
        where name = "臺北市兒童新樂園";

        update `{dataset2}.{table_name2}`
        set attraction_id = "7b4cdc4d6bb2fd9fac8e962fc5fcc927"
        where name = "白石湖";

        insert into `{dataset2}.{table_name2}`
        select name, attraction_id from `{dataset1}.{table_name1}`
        where name not in (
          select name from `{dataset2}.{table_name2}`);
        """
        query_bq(client=BQ_CLIENT, query=query)


    all_sensors_complete.set_upstream([wait_for_ods_tripadvisor, wait_for_ods_generate_attraction_id_taipei, wait_for_ods_generate_attraction_id_trip])
    
    generate_dim_attraction_hashtag_data().set_upstream(all_sensors_complete)
    
    # 對兩個BigQuery的Exteral Table做join，最後上傳到BigQuery
    generate_dim_attraction_hashtag_data_task = generate_dim_attraction_hashtag_data()
    create_dim_attraction_hashtag_bq_external_table_task = create_dim_attraction_hashtag_bq_external_table(
        generate_dim_attraction_hashtag_data_task, BQ_DIM_DATASET, "dim_attraction_hashtag"
    )
    update_and_insert_data_to_dim_attraction_hashtag_task = update_and_insert_data_to_dim_attraction_hashtag()

    # Set dependencies
    create_dim_attraction_hashtag_bq_external_table_task.set_upstream(generate_dim_attraction_hashtag_data_task)
    update_and_insert_data_to_dim_attraction_hashtag_task.set_upstream(create_dim_attraction_hashtag_bq_external_table_task)

create_dim_attraction_hashtag_pipeline()
