FROM apache/airflow:2.9.1

COPY requirements.txt .
COPY gcp_keyfile.json .

ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow/utils"

RUN pip install -r requirements.txt
