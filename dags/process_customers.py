from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from gcp_params import gcp_params


with DAG(
    dag_id="process_customers",
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 6),
    schedule_interval="0 1 * * *",
    catchup=True,
    max_active_runs=1,
    tags=['final_project']
) as dag:

    copy_to_bronze = GCSToBigQueryOperator(
        task_id='copy_to_bronze',
        schema_fields=[
            {'name': 'Id', 'type': 'INTEGER'},
            {'name': 'FirstName', 'type': 'STRING'},
            {'name': 'LastName', 'type': 'STRING'},
            {'name': 'Email', 'type': 'STRING'},
            {'name': 'RegistrationDate', 'type': 'DATE'},
            {'name': 'State', 'type': 'STRING'}
        ],
        bucket='de-final-project-np',
        source_objects=['raw/customers/{{ ds }}/*'],
        source_format='CSV',
        destination_project_dataset_table='de-finalproject-np.bronze.customers',
        create_disposition='CREATE_IF_NEEDED',
        write_disposition='WRITE_APPEND',
        skip_leading_rows=1
    )

    transfer_bronze_to_silver = BigQueryInsertJobOperator(
        task_id='transfer_bronze_to_silver',
        **gcp_params,
        configuration={
            "query": {
                "query": "{% include 'sql/clear_customers.sql' %}",
                "useLegacySql": False
            }
        },
    )

    copy_to_bronze >> transfer_bronze_to_silver
