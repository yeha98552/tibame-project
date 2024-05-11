import os
from google.cloud import storage
from google.cloud import bigquery
from google.api_core.exceptions import NotFound


# Set the path to the service account key JSON file
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "your key json file"


def create_bucket_if_not_exists(bucket_name):
    """Creates a GCS bucket if it does not exist."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    if not bucket.exists():
        bucket = storage_client.create_bucket(bucket, location="asia-east1")
        print(f"Bucket {bucket_name} created.")
    else:
        print(f"Bucket {bucket_name} already exists.")


def create_dataset_if_not_exists(dataset_id, location="asia-east1"):
    """Creates a BigQuery dataset if it does not exist."""
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset {dataset_id} already exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = location
        dataset = client.create_dataset(dataset, timeout=30)  # 30 seconds timeout
        print(f"Dataset {dataset_id} created.")


def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)
    print(f"File {source_file_name} uploaded to {destination_blob_name}.")
    return f"gs://{bucket_name}/{destination_blob_name}"


def load_csv_from_gcs(dataset_id, table_id, gcs_uri):
    """Loads a CSV file from GCS into a BigQuery table using schema auto-detection."""
    client = bigquery.Client()
    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the header row in the CSV file
    job_config.autodetect = True  # Enable schema auto-detection
    job_config.write_disposition = (
        bigquery.WriteDisposition.WRITE_TRUNCATE
    )  # Replace the table

    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    print(f"Starting job {load_job.job_id}")
    load_job.result()  # Waits for table load to complete.
    print("Job finished.")

    destination_table = client.get_table(table_ref)
    print(f"Loaded {destination_table.num_rows} rows into {dataset_id}.{table_id}")


# Configuration and Execution
bucket_name = "scraper_tir101"
source_file_name = "normalized/Normalization_中正紀念堂_2024-05-08.csv"
destination_blob_name = "Normalization_中正紀念堂_2024-05-08.csv"
dataset_id = "scraper_tir101"
table_id = "attraction_social_article"

# Create bucket and dataset if they don't exist
create_bucket_if_not_exists(bucket_name)
create_dataset_if_not_exists(dataset_id)

# Upload to GCS
gcs_uri = upload_to_gcs(bucket_name, source_file_name, destination_blob_name)

# Load to BigQuery
load_csv_from_gcs(dataset_id, table_id, gcs_uri)
