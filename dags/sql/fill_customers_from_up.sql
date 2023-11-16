CREATE OR REPLACE TABLE `{{ params.project_id }}.gold.enriched_user_profiles` AS
SELECT
    c.client_id,
    c.email,
    c.registration_date,
    up.full_name,
    up.state,
    up.birth_date,
    up.phone_number
FROM
    `{{ params.project_id }}.silver.customers` c
JOIN
    `{{ params.project_id }}.silver.user_profiles` up
ON
    c.email = up.email