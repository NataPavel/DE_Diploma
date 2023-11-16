CREATE OR REPLACE TABLE `{{ params.project_id }}.silver.sales`
PARTITION BY
    purchase_date
AS
SELECT
    CAST(CustomerId AS INTEGER) AS client_id,
    CAST(FORMAT_DATE('%F', DATE (CAST(REPLACE(REPLACE(PurchaseDate, 'Aug', '09'), '/', '-') AS DATE))) AS DATE) AS purchase_date,
    Product AS product_name,
    CAST(REPLACE(REPLACE(Price, '$', ''), 'USD', '') AS INTEGER) AS price
FROM `{{ params.project_id }}.bronze.sales`;
