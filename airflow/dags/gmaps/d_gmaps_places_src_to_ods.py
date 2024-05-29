from datetime import datetime, timedelta
from typing import List

import pandas as pd
from google.cloud import bigquery, storage
from utils.common import load_config
from utils.gcp import (
    build_bq_from_gcs,
    download_df_from_gcs,
    upload_df_to_gcs,
)

from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator

config = load_config()
RAW_BUCKET = config["gcp"]["bucket"]["raw"]
PROCESSED_BUCKET = config["gcp"]["bucket"]["processed"]
BQ_ODS_DATASET = config["gcp"]["bigquery"]["ods_dataset"]
current_date = datetime.now().strftime("%Y-%m-%d")
BLOB_NAME = f"{config["gcp"]["blob"]["gmaps"]["places"]}/{current_date}/"
ODS_TABLE_NAME = "ods-" + config["gcp"]["table"]["gmaps-places"]

GCS_CLIENT = storage.Client()
BQ_CLIENT = bigquery.Client()

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 5, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


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
def d_gmaps_places_src_to_ods():
    @task
    def e_get_places_bloblist() -> List[List[str]]:
        blobs = storage.Client().list_blobs(RAW_BUCKET, prefix=BLOB_NAME)
        sorted_blobs = sorted(blobs, key=lambda x: x.name)
        blob_names = [blob.name for blob in sorted_blobs]
        batch_size = 200  # default max_map_length is 1024
        blob_batches = [
            blob_names[i : i + batch_size]
            for i in range(0, len(blob_names), batch_size)
        ]
        return blob_batches

    def t_convert_place_id(df: pd.DataFrame, place_id: str) -> pd.DataFrame:
        if "place_id" not in df.columns:
            print("place_id not in columns")
            print(df)
        else:
            df["place_id_raw"] = df["place_id"]
        df["place_id"] = place_id
        return df

    def t_remove_unused_columns(df: pd.DataFrame) -> pd.DataFrame:
        columns_to_select = [
            "status",
            "featured_question",
            "cid",
            "phone",
            "owner",
            "plus_code",
            "data_id",
            "closed_on",
        ]
        # Check if all columns are present
        if not all(col in df.columns for col in columns_to_select):
            print(
                "Missing columns:",
                [col for col in columns_to_select if col not in df.columns],
            )
            print(df)
        else:
            df.drop(
                columns=columns_to_select,
                inplace=True,
            )
        return df

    def t_rename_columns(df: pd.DataFrame) -> pd.DataFrame:
        df.rename(
            columns={
                "name": "place_name",
                "link": "google_place_url",
                "reviews": "total_reviews",
                "rating": "avg_rating",
            },
            inplace=True,
        )
        return df

    @task
    def et_get_places_df_from_gcs(blob_batch: List[str]) -> pd.DataFrame:
        for blob_name in blob_batch:
            df = download_df_from_gcs(
                client=GCS_CLIENT,
                bucket_name=RAW_BUCKET,
                blob_name=blob_name,
                filetype="jsonl",
            )
            # Add place_id
            df = t_convert_place_id(
                df=df, place_id=blob_name.split("/")[-1].split(".")[0]
            )
            # Remove unused columns
            df = t_remove_unused_columns(df)
            # Rename columns
            df = t_rename_columns(df)

            # Upload to GCS
            l_upload_transformed_places_to_gcs(df, blob_name)

    # @task
    def l_upload_transformed_places_to_gcs(df: pd.DataFrame, blob_name: str) -> bool:
        upload_df_to_gcs(
            client=GCS_CLIENT,
            bucket_name=PROCESSED_BUCKET,
            blob_name=blob_name,
            df=df,
            filetype="jsonl",
        )

    @task
    def l_create_bq_external_table() -> bool:
        build_bq_from_gcs(
            client=BQ_CLIENT,
            bucket_name=PROCESSED_BUCKET,
            blob_name=BLOB_NAME + "*.jsonl",
            dataset_name=BQ_ODS_DATASET,
            table_name=ODS_TABLE_NAME,
            schema=[
                bigquery.SchemaField("place_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_id_raw", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("place_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("description", "STRING"),
                bigquery.SchemaField("total_reviews", "INTEGER"),
                bigquery.SchemaField("avg_rating", "FLOAT"),
                bigquery.SchemaField("can_claim", "BOOLEAN"),
                bigquery.SchemaField("featured_image", "STRING"),
                bigquery.SchemaField("main_category", "STRING"),
                bigquery.SchemaField("categories", "STRING", mode="REPEATED"),
                bigquery.SchemaField("google_place_url", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("workday_timing", "STRING"),
                bigquery.SchemaField("address", "STRING"),
                bigquery.SchemaField(
                    "review_keywords",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("keyword", "STRING"),
                        bigquery.SchemaField("count", "INTEGER"),
                    ],
                ),
                bigquery.SchemaField("link", "STRING"),
                bigquery.SchemaField("status", "STRING"),
                bigquery.SchemaField("price_range", "STRING"),
                bigquery.SchemaField("reviews_link", "STRING"),
                bigquery.SchemaField(
                    "reviews_per_rating",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("rating_1", "INTEGER"),
                        bigquery.SchemaField("rating_2", "INTEGER"),
                        bigquery.SchemaField("rating_3", "INTEGER"),
                        bigquery.SchemaField("rating_4", "INTEGER"),
                        bigquery.SchemaField("rating_5", "INTEGER"),
                    ],
                ),
                bigquery.SchemaField(
                    "coordinates",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("latitude", "FLOAT"),
                        bigquery.SchemaField("longitude", "FLOAT"),
                    ],
                ),
                bigquery.SchemaField(
                    "detailed_address",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("ward", "STRING"),
                        bigquery.SchemaField("street", "STRING"),
                        bigquery.SchemaField("city", "STRING"),
                        bigquery.SchemaField("postal_code", "STRING"),
                        bigquery.SchemaField("state", "STRING"),
                        bigquery.SchemaField("country_code", "STRING"),
                    ],
                ),
                bigquery.SchemaField("time_zone", "STRING"),
                bigquery.SchemaField(
                    "menu",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField("link", "STRING"),
                        bigquery.SchemaField("source", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "reservations",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("link", "STRING"),
                        bigquery.SchemaField("source", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "order_online_links",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("link", "STRING"),
                        bigquery.SchemaField("source", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "about",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("id", "STRING"),
                        bigquery.SchemaField("name", "STRING"),
                        bigquery.SchemaField(
                            "options",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("name", "STRING"),
                                bigquery.SchemaField("enabled", "BOOLEAN"),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField(
                    "images",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("about", "STRING"),
                        bigquery.SchemaField("link", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "hours",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("day", "STRING"),
                        bigquery.SchemaField("times", "STRING", mode="REPEATED"),
                    ],
                ),
                bigquery.SchemaField(
                    "most_popular_times",
                    "RECORD",
                    mode="REPEATED",
                    fields=[
                        bigquery.SchemaField("hour_of_day", "INTEGER"),
                        bigquery.SchemaField("average_popularity", "FLOAT"),
                        bigquery.SchemaField("time_label", "STRING"),
                    ],
                ),
                bigquery.SchemaField(
                    "popular_times",
                    "RECORD",
                    fields=[
                        bigquery.SchemaField(
                            "Monday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Tuesday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Wednesday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Thursday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Friday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Saturday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                        bigquery.SchemaField(
                            "Sunday",
                            "RECORD",
                            mode="REPEATED",
                            fields=[
                                bigquery.SchemaField("hour_of_day", "INTEGER"),
                                bigquery.SchemaField("time_label", "STRING"),
                                bigquery.SchemaField(
                                    "popularity_percentage", "INTEGER"
                                ),
                                bigquery.SchemaField(
                                    "popularity_description", "STRING"
                                ),
                            ],
                        ),
                    ],
                ),
                bigquery.SchemaField("is_spending_on_ads", "BOOLEAN"),
            ],
            filetype="jsonl",
        )

    trigger_d_gmaps_dim_places = TriggerDagRunOperator(
        task_id="trigger_d_gmaps_dim_places",
        trigger_dag_id="d_gmaps_dim_places",
    )

    blob_batches = e_get_places_bloblist()
    t1 = et_get_places_df_from_gcs.expand(blob_batch=blob_batches)
    t2 = l_create_bq_external_table()

    t1 >> t2 >> trigger_d_gmaps_dim_places


dag_instance = d_gmaps_places_src_to_ods()
