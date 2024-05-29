import os
from datetime import datetime, timedelta

from docker.types import Mount
from google.cloud import bigquery
from utils.common import load_config
from utils.gcp import query_bq_to_df

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.providers.docker.operators.docker import DockerOperator

config = load_config()
RAW_BUCKET = config["gcp"]["bucket"]["raw"]
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
BLOB_NAME = config["gcp"]["blob"]["gmaps"]["prefix"]
CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH = os.environ.get(
    "CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH"
)

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
def d_gmaps_crawler_to_src():
    @task
    def get_attraction_list(top_n: int = 1500) -> list[list[dict]]:
        query = f"""
            SELECT DISTINCT
                attraction_id,
                attraction_name
            FROM
                `{BQ_ODS_DATASET}.ods_tripadvisor_info`
        """
        df = query_bq_to_df(BQ_CLIENT, query)[:top_n]
        # random df to avoid task 1 always crawler the attraction with more reviews
        df = df.sample(frac=1).reset_index(drop=True)
        attractions = df.to_dict(orient="records")
        # batch
        batch_size = 200  # default max_map_length is 1024
        return [
            attractions[i : i + batch_size]
            for i in range(0, len(attractions), batch_size)
        ]

    @task
    def e_gmaps_crawler(batch: list[dict]):
        for attraction in batch:
            print(f"crawling attraction: {attraction}")
            attraction_name = attraction["attraction_name"]
            attraction_id = attraction["attraction_id"]
            crawler_task = DockerOperator(
                task_id=f"crawl_{attraction_id}",
                image="gmaps-scraper",
                api_version="auto",
                auto_remove=True,
                environment={
                    "ATTRACTION_ID": attraction_id,
                    "ATTRACTION_NAME": attraction_name,
                    "GCS_BUCKET_NAME": RAW_BUCKET,
                    "GCS_BLOB_NAME": BLOB_NAME,
                },
                command="make run",
                mounts=[
                    Mount(
                        source=CRAWER_GOOGLE_CREDENTIALS_LOCAL_PATH,
                        target="/app/crawler_gcp_keyfile.json",
                        type="bind",
                        read_only=True,
                    ),
                ],
                mount_tmp_dir=False,
                mem_limit="24g",
                shm_size="2g",
                docker_url="tcp://docker-proxy:2375",
                network_mode="bridge",
            )
            crawler_task.execute({})

    trigger_d_gmaps_places_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_places_src_to_ods",
        trigger_dag_id="d_gmaps_places_src_to_ods",
    )

    trigger_d_gmaps_reviews_src_to_ods = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_reviews_src_to_ods",
        trigger_dag_id="d_gmaps_reviews_src_to_ods",
    )

    batches = get_attraction_list()
    crawl_tasks = e_gmaps_crawler.expand(batch=batches)
    crawl_tasks >> trigger_d_gmaps_places_src_to_ods
    crawl_tasks >> trigger_d_gmaps_reviews_src_to_ods


d_gmaps_crawler_to_src()
