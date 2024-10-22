-- models/marts/fct_time_based_analysis.sql
{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    ae.receive_date_year,
    ae.receive_date_month,
    COUNT(DISTINCT ae.safety_report_id) AS monthly_event_count
FROM {{ ref('adverse_event_obt') }} ae
GROUP BY ae.drug_name, ae.receive_date_year, ae.receive_date_month
ORDER BY ae.drug_name, ae.receive_date_year, ae.receive_date_month  