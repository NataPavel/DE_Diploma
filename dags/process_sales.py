from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_params import gcp_params, gcp_params_external

with DAG(
    dag_id="process_sales_dag",
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 10, 1),
    catchup=True,
    schedule_interval="0 1 * * *",
    tags=['final_project'],
    max_active_runs=1
) as dag:

    transfer_to_bronze = BigQueryInsertJobOperator(
        task_id='transfer_to_bronze',
        **gcp_params_external,
        configuration={
            "query": {
                "query": "{% include 'sql/sales_to_bronze.sql' %}",
                "useLegacySql": False
            }
        },
    )

    transfer_bronze_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_bronze_to_silver',
        **gcp_params,
        configuration={
            "query": {
                "query": "{% include 'sql/transfer_from_bronze_to_silver.sql' %}",
                "useLegacySql": False
            }
        },
    )

    transfer_to_bronze >> transfer_bronze_to_silver