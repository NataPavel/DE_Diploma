CREATE OR REPLACE TABLE `{{ params.project_id }}.silver.customers` AS
SELECT
    Id AS client_id,
    FirstName AS first_name,
    LastName AS last_name,
    Email AS email,
    RegistrationDate AS registration_date,
    State AS state
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY Id) AS row_num
    FROM `{{ params.project_id }}.bronze.customers`
)
WHERE row_num = 1;
