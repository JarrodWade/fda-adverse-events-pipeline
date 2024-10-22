{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    ae.receive_date_year AS date_year,
    ae.receive_date_month AS date_month,
    COUNT(DISTINCT ae.safety_report_id) AS total_events,
    COUNT(*) as total_drug_mentions,
    COUNT(DISTINCT ae.reporter_country) AS affected_countries,
    AVG(ae.calculated_age_in_years) AS avg_age,
    MIN(ae.receive_date_day) AS first_report_date,
    MAX(ae.receive_date_day) AS last_report_date,
    SUM(CASE WHEN ae.is_serious = '1' THEN 1 ELSE 0 END) AS serious_events_count,
    SUM(CASE WHEN ae.seriousness_death = '1' THEN 1 ELSE 0 END) AS death_events_count
FROM {{ ref('adverse_event_obt') }} ae
GROUP BY ae.drug_name, ae.receive_date_year, ae.receive_date_month
ORDER BY ae.drug_name, ae.receive_date_year, ae.receive_date_month