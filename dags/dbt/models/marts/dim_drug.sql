{{ config(materialized='table') }}

WITH DRUG_DATA AS (
    SELECT DISTINCT
        drug_id,
        drug_name,
        medicinalproduct as medicinal_product
    FROM {{ ref('stg_adverse_events') }}
)

SELECT
    drug_id,
    drug_name,
    medicinal_product    
FROM DRUG_DATA