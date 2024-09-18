{{ config(materialized='table') }}

SELECT
    ae.drug_name,
    ae.reaction_meddra_pt,
    ae.receive_date_year, 
    ae.receive_date_month,
    COUNT(DISTINCT ae.safety_report_id) AS reaction_count,
    COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY ae.drug_name) AS percentage
FROM {{ ref('fct_adverse_events') }} ae
    GROUP BY ae.drug_name, ae.reaction_meddra_pt, ae.receive_date_year, ae.receive_date_month
    HAVING COUNT(*) > 10  -- Filter out rare reactions
    ORDER BY ae.drug_name, ae.receive_date_year, ae.receive_date_month, reaction_count DESC