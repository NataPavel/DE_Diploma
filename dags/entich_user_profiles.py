from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from gcp_params import gcp_params

with DAG(
    dag_id='enrich_user_profiles',
    tags=['final_project'],
    max_active_runs=1,
    start_date=datetime.now()
) as dag:

    enrich_to_gold = BigQueryInsertJobOperator(
        task_id='enrich_to_gold',
        **gcp_params,
        configuration={
            "query": {
                "query": "{% include 'sql/fill_customers_from_up.sql' %}",
                "useLegacySql": False
            }
        },
    )

    enrich_to_gold