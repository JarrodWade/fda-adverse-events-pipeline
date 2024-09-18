{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    p.age_group,
    COUNT(DISTINCT ae.safety_report_id) AS event_count
FROM {{ ref('fct_adverse_events') }} ae
JOIN {{ ref('dim_patient') }} p ON ae.patient_id = p.patient_id
GROUP BY ae.drug_name, p.age_group
ORDER BY ae.drug_name, COUNT(DISTINCT ae.safety_report_id) DESC
