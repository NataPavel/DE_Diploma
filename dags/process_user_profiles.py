from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_params import gcp_params

with DAG(
    dag_id='process_user_profiles',
    tags=['final_project'],
    max_active_runs=1,
    start_date=datetime.now()
) as dag:

    copy_to_bronze = GCSToBigQueryOperator(
        task_id='copy_to_bronze',
        schema_fields=[
            {'name': 'email', 'type': 'STRING'},
            {'name': 'full_name', 'type': 'STRING'},
            {'name': 'state', 'type': 'STRING'},
            {'name': 'birth_date', 'type': 'DATE'},
            {'name': 'phone_number', 'type': 'STRING'}
        ],
        bucket='de-final-project-np',
        source_objects=['raw/user_profiles/*'],
        source_format='NEWLINE_DELIMITED_JSON',
        destination_project_dataset_table='de-finalproject-np.bronze.user_profiles',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1
    )

    transfer_bronze_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_bronze_to_silver',
        **gcp_params,
        configuration={
            "query": {
                "query": "{% include 'sql/up_to_silver.sql' %}",
                "useLegacySql": False
            }
        },
    )

    copy_to_bronze >> transfer_bronze_to_silver

