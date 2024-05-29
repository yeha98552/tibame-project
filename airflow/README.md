# Airflow


## Usage
1. Copy `gcp_keyfile.json` to `airflow/`

2. Copy `.env` to `airflow/`

3. Generate `AIRFLOW_SECRET_KEY` in `.env`

Use below command to generate a random secret key.
```sh
openssl rand -hex 24
```
Paste the result to `AIRFLOW_SECRET_KEY` in `.env`.

4. Run
```sh
docker-compose build
docker-compose up
```

5. Navigate to `localhost:8080`


## Example

1. [d_data_ingestion_gcs.py](./dags/d_data_ingestion_gcs.py): upload file to GCS, and create bigquery external table.

2. [d_example_data_pipeline.py](./dags/d_example_data_pipeline.py): simple data pipeline.
- download data from gcs, and transform data, then upload to gcs and create external table.
- query bigquery and create new table.

## Reference

- [TIR101 Group 2](https://github.com/harryhowiefish/TIR101_Group2): Refer to Airflow settings and Upload to GCS.
