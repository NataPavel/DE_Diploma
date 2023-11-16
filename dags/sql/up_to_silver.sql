CREATE OR REPLACE TABLE `{{ params.project_id }}.silver.user_profiles` AS
SELECT
    email,
    full_name,
    state,
    birth_date,
    phone_number
FROM `{{ params.project_id }}.bronze.user_profiles_bronze`