import os
from datetime import timedelta

import pandas as pd
import hashlib
from google.cloud import bigquery, storage
from utils.gcp import upload_df_to_bq

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

EXTABLE_NAME = "ods_attraction_generate_id_hashtag"

BQ_ODS_DATASET = os.environ.get("BIGQUERY_ODS_DATASET")

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
    dag_id="generate_attraction_id_hashtag_to_bq_ods_dataset",
    default_args=default_args,
    description="生成hashtag景點ID到bigquery",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    tags=["ods"],
)
def generate_attraction_id_hashtag_to_bq_dataset_task_pipeline():
    @task
    def generate_hashtag_table() -> pd.DataFrame:
        hashtag_dict = {"name":["孫中山紀念館", "國父紀念館", "中華民國國父紀念館", "孫中山紀念堂", "國父紀念堂",
                                "兒童新樂園", "臺北兒童樂園", "臺北新樂園", "臺北兒童新樂園", "臺北遊樂場",
                                "華山1914", "華山1914文創園區", "華山1914文化創意園區", "華山1914文化創意基地", "華山1914文創產業基地",
                                "中正紀念堂", "中正紀念館", "臺灣民主紀念館", "國立臺灣民主紀念館", "民主紀念館",
                                "動物園", "臺北動物園", "臺北市動物園", "台北市立動物園", "台北動物園",
                                "臺北101", "101大樓", "台北101大樓", "台北摩天大樓", "台灣101",
                                "士林官邸", "士林花園", "官邸公園", "士林官邸花園", "官邸花園",
                                "台灣科學教育館", "科教館", "臺灣科學教育館", "臺灣科教館", "台灣科教館",
                                "故宮博物院", "故宮", "臺灣故宮", "台灣故宮", "臺灣故宮博物院",
                                "松山文化創意園區", "松山文創", "松山創意園區", "松山文化園區", "松山創意文化園區"]}
        hashtag_dataframe = pd.DataFrame(hashtag_dict)
        return hashtag_dataframe

    @task
    def give_hashtag_id(hashtag_dataframe) -> pd.DataFrame:
        id_array = ["c1c7a1bab25a9a9d26f3700c9ae50dd8", "d792ff6717863a1aee8a567ecae503c2",
                    "3915592d5ee993e9b4e4442cc973cd95", "fc73810e75cbcb35391b0f013cfabeab",
                    "351bad3e4c95534ee9b4a21084bb4f52", "1d56e443604354ed396ea153f8b55846",
                    "5bafa293b96ff560a87283e618e83858", "c293892599dc4be7f35c229376203796",
                    "7a4324592bc3a407cb17135133c6e0b3", "c124cd7a866f61e3bda0a2eadcff2c46"]
        count = 0
        for i in id_array:
            for j in range(5):
                hashtag_dataframe.loc[count, "attraction_id"] = i
                count += 1
        return hashtag_dataframe

    @task
    def upload_generate_attraction_id_hashtag_data_to_bq(
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

    # 從GCS下載成pd.DataFrame，使用pandas做一些資料處理，建立BigQuery的Exteral Table
    hashtag_dataframe = generate_hashtag_table()
    hashtag_dataframe = give_hashtag_id(hashtag_dataframe)
    upload_generate_attraction_id_hashtag_data_to_bq(
        hashtag_dataframe, BQ_ODS_DATASET, EXTABLE_NAME
    )

generate_attraction_id_hashtag_to_bq_dataset_task_pipeline()
