{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    SUM(CASE WHEN ae.is_serious = '1' THEN 1 ELSE 0 END) AS serious_count,
    SUM(CASE WHEN ae.seriousness_death = '1' THEN 1 ELSE 0 END) AS death_count,
    SUM(CASE WHEN ae.seriousness_hospitalization = '1' THEN 1 ELSE 0 END) AS hospitalization_count,
    SUM(CASE WHEN ae.seriousness_life_threatening = '1' THEN 1 ELSE 0 END) AS life_threatening_count,
    SUM(CASE WHEN ae.seriousness_disabling = '1' THEN 1 ELSE 0 END) AS disabling_count,
    SUM(CASE WHEN ae.seriousness_congenital_anomaly = '1' THEN 1 ELSE 0 END) AS congenital_anomaly_count,
    SUM(CASE WHEN ae.seriousness_other = '1' THEN 1 ELSE 0 END) AS other_serious_count,
    COUNT(DISTINCT ae.safety_report_id) AS total_events,
    SUM(CASE WHEN ae.is_serious = '1' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) AS serious_percentage
FROM {{ ref('fct_adverse_events') }} ae
GROUP BY ae.drug_name