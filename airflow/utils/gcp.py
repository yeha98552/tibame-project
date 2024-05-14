from io import BytesIO
from typing import List

import pandas as pd
from google.cloud import bigquery, storage
from google.cloud.exceptions import NotFound

# WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
# (Ref: https://github.com/googleapis/python-storage/issues/74)
storage.blob._MAX_MULTIPART_SIZE = 1024 * 1024  # 1 MB
storage.blob._DEFAULT_CHUNKSIZE = 1024 * 1024  # 1 MB
# End of Workaround


def upload_df_to_gcs(
    client: storage.Client,
    bucket_name: str,
    blob_name: str,
    df: pd.DataFrame,
    filetype: str = "parquet",
) -> bool:
    """
    Upload a pandas dataframe to GCS.

    Args:
        client (storage.Client): The client to use to upload to GCS.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        df (pd.DataFrame): The dataframe to upload.
        filetype (str): The type of the file to download. Default is "parquet".
                        Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists in GCP.")
        return False
    try:
        if filetype == "parquet":
            blob.upload_from_string(
                df.to_parquet(index=False), content_type="application/octet-stream"
            )
        elif filetype == "csv":
            blob.upload_from_string(df.to_csv(index=False), content_type="text/csv")
        elif filetype == "jsonl":
            blob.upload_from_string(
                df.to_json(orient="records", lines=True),
                content_type="application/jsonl",
            )
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload pd.DataFrame to GCS, reason: {e}")


def upload_file_to_gcs(
    client: storage.Client, bucket_name: str, blob_name: str, source_filepath: str
) -> bool:
    """
    Upload a file to GCS.

    Args:
        client (storage.Client): The client to use to upload to GCS.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        source_filepath (str): The path to the file to upload.

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if blob.exists():
        print("File already exists.")
        return False
    try:
        blob.upload_from_filename(source_filepath)
        print("Upload successful.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload file to GCS, reason: {e}")


def download_df_from_gcs(
    client: storage.Client, bucket_name: str, blob_name: str, filetype: str = "parquet"
) -> pd.DataFrame:
    """
    Download a pandas dataframe from GCS.

    Args:
        client (storage.Client): The client to use to download from GCS.
        bucket_name (str): The name of the bucket to download from.
        blob_name (str): The name of the blob to download from.
        filetype (str): The type of the file to download. Default is "parquet".
                        Can be "parquet" or "csv" or "jsonl".

    Returns:
        pd.DataFrame: The dataframe downloaded from GCS.
    """
    bucket = client.bucket(bucket_name)

    blob = bucket.blob(blob_name)
    if not blob.exists():
        raise FileNotFoundError(f"file {blob_name} not found in bucket {bucket_name}")

    bytes_data = blob.download_as_bytes()
    data_io = BytesIO(bytes_data)

    if filetype == "csv":
        return pd.read_csv(data_io)
    elif filetype == "parquet":
        return pd.read_parquet(data_io)
    elif filetype == "jsonl":
        return pd.read_json(data_io, lines=True)
    else:
        raise ValueError(
            f"Invalid filetype: {filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
        )


def build_bq_from_gcs(
    client: bigquery.Client,
    dataset_name: str,
    table_name: str,
    bucket_name: str,
    blob_name: str,
    schema: List[bigquery.SchemaField] = None,
    filetype: str = "parquet",
) -> bool:
    """
    Build a bigquery external table from a file in GCS.

    Args:
        client (bigquery.Client): The client to use to create the external table.
        dataset_name (str): The name of the dataset to create.
        table_name (str): The name of the table to create.
        bucket_name (str): The name of the bucket to upload to.
        blob_name (str): The name of the blob to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).
        filetype (str): The type of the file to download. Default is "parquet". Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    # Construct the fully-qualified BigQuery table ID
    table_id = f"{client.project}.{dataset_name}.{table_name}"

    try:
        client.get_table(table_id)  # Attempt to get the table
        print(f"Table {table_id} already exists.")
        return False
    except NotFound:
        # Define the external data source configuration
        if filetype == "parquet":
            external_config = bigquery.ExternalConfig("PARQUET")
        elif filetype == "csv":
            external_config = bigquery.ExternalConfig("CSV")
        elif filetype == "jsonl":
            external_config = bigquery.ExternalConfig("JSONL")
        else:
            raise ValueError(
                f"Invalid filetype: {filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
            )
        external_config.source_uris = [f"gs://{bucket_name}/{blob_name}"]
        if schema:
            external_config.schema = schema
        # Create a table with the external data source configuration
        table = bigquery.Table(table_id)
        table.external_data_configuration = external_config

        try:
            client.create_table(table)  # API request to create the external table
            print(f"External table {table.table_id} created.")
            return True
        except Exception as e:
            raise Exception(f"Failed to create external table, reason: {e}")
    except Exception as e:
        raise Exception(f"An error occurred while checking if the table exists: {e}")


def query_bq(client: bigquery.Client, sql_query: str) -> bigquery.QueryJob:
    """
    Query bigquery and return results. (可以用在bigquery指令，例如Insert、Update，但沒有要取得資料表的資料)

    Args:
        client (bigquery.Client): The client to use to query bigquery.
        sql_query (str): The SQL query to execute.

    Returns:
        bigquery.QueryJob: The result of the query.
    """
    try:
        query_job = client.query(sql_query)
        return query_job.result()  # Return the results for further processing
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def query_bq_to_df(client: bigquery.Client, sql_query: str) -> pd.DataFrame:
    """
    Executes a BigQuery SQL query and directly loads the results into a DataFrame
    using the BigQuery Storage API.  (可以用在bigquery指令，然後取得資料表的資料成為DataFrame)

    Args:
        client (bigquery.Client): The client to use to query bigquery.
        query (str): SQL query string.

    Returns:
        pd.DataFrame: The query results as a Pandas DataFrame.
    """
    try:
        query_job = client.query(sql_query)
        return query_job.to_dataframe()  # Convert result to DataFrame
    except Exception as e:
        raise Exception(f"Failed to query bigquery table, reason: {e}")


def upload_df_to_bq(
    client: bigquery.Client,
    df: pd.DataFrame,
    dataset_name: str,
    table_name: str,
    schema: List[bigquery.SchemaField] = None,
    filetype: str = "parquet",
) -> bool:
    """
    Upload a pandas dataframe to bigquery.

    Args:
        client (bigquery.Client): The client to use to upload to bigquery.
        df (pd.DataFrame): The dataframe to upload.
        dataset_name (str): The name of the dataset to upload to.
        table_name (str): The name of the table to upload to.
        schema (List[bigquery.SchemaField], optional): The schema of the table to upload to. Default is None.
                                                        If None, use the default schema (automatic-detect).
        filetype (str): The type of the file to download. Default is "parquet". Can be "parquet" or "csv" or "jsonl".

    Returns:
        bool: True if the upload was successful, False otherwise.
    """
    dataset_id = client.dataset(dataset_name)
    table_id = dataset_id.table(table_name)

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    if filetype == "parquet":
        job_config.source_format = bigquery.SourceFormat.PARQUET
    elif filetype == "csv":
        job_config.source_format = bigquery.SourceFormat.CSV
    elif filetype == "jsonl":
        job_config.source_format = bigquery.SourceFormat.JSONL
    else:
        raise ValueError(
            f"Invalid filetype: {filetype}. Please specify 'parquet' or 'csv' or 'jsonl'."
        )
    if schema:
        job_config.schema = schema

    try:
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # Wait for the job to complete
        table = client.get_table(table_id)
        print(f"Table {table.table_id} created with {table.num_rows} rows.")
        return True
    except Exception as e:
        raise Exception(f"Failed to upload df to bigquery, reason: {e}")


def delete_blob(client, bucket_name, blob_name):
    """
    Delete a blob from GCS.

    Args:
        client (storage.Client): The client to use to interact with Google Cloud Storage.
        bucket_name (str): The name of the bucket.
        blob_name (str): The name of the blob to delete.
    """
    try:
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        if not blob.exists():
            print(f"Blob {blob_name} does not exist in bucket {bucket_name}.")
            return False
        blob.delete()
        print(f"Blob {blob_name} deleted from bucket {bucket_name}.")
        return True
    except Exception as e:
        raise Exception(f"Failed to delete blob, reason: {e}")


def delete_table(client: bigquery.Client, dataset_name: str, table_name: str) -> bool:
    """
    Delete a bigquery table.

    Args:
        client (bigquery.Client): The client to use to delete the table.
        dataset_name (str): The name of the dataset to delete the table from.
        table_name (str): The name of the table to delete.

    Returns:
        bool: True if the deletion was successful, False otherwise.
    """
    table_id = f"{dataset_name}.{table_name}"
    try:
        client.delete_table(table_id)
        print(f"Table {table_id} deleted.")
    except NotFound:
        print(f"Table {table_id} not found.")
        return False
    return True


def rename_blob(
    client: storage.Client, bucket_name: str, blob_name: str, new_blob_name: str
) -> bool:
    """
    Rename a blob in GCS by copying it to a new name and then deleting the original.

    Args:
        client (storage.Client): The client to use with GCS.
        bucket_name (str): The name of the bucket where the blob is stored.
        blob_name (str): The current name of the blob.
        new_blob_name (str): The new name for the blob.

    Returns:
        bool: True if the rename was successful, False otherwise.
    """
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    new_blob = bucket.blob(new_blob_name)

    if not blob.exists():
        print(f"Blob {blob_name} does not exist in bucket {bucket_name}.")
        return False

    if new_blob.exists():
        print(f"Blob {new_blob_name} already exists in bucket {bucket_name}.")
        return False

    # Copy the blob to the new location
    bucket.copy_blob(blob, bucket, new_blob_name)

    # Delete the original blob
    blob.delete()

    print(f"Blob {blob_name} renamed to {new_blob_name} in bucket {bucket_name}.")
    return True
