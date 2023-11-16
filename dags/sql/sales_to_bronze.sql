CREATE OR REPLACE EXTERNAL TABLE `{{ params.project_id }}.bronze.sales`(
    CustomerId STRING,
    PurchaseDate STRING,
    Product STRING,
    Price STRING
) OPTIONS (
    format = 'CSV',
    uris = ['gs://{{ params.data_raw_bucket }}/raw/sales/*.csv'],
    skip_leading_rows = 1
);





