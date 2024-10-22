{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    ae.age_group,
    COUNT(DISTINCT ae.safety_report_id) AS event_count
FROM {{ ref('adverse_event_obt') }} ae
GROUP BY ae.drug_name, ae.age_group
ORDER BY ae.drug_name, COUNT(DISTINCT ae.safety_report_id) DESC
