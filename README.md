# DE Diploma project description
The project includes data processing of sales, customers, user_profiles tables: transferring from GCS to BigQuery (bronze layer), cleaning, and transferring to silver. From Customers, the data is enriched with information from User_profiles and transferred to gold. All stages are automated through Airflow.
