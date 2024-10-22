{{ config(materialized='table') }}

SELECT
    ae.safety_report_id,
    ae.drug_name,
    ae.drug_product,
    ae.drug_characterization,
    ae.drug_indication,
    ae.patient_age,
    ae.patient_age_unit,
    ae.patient_sex,
    ae.patient_weight_kg,
    ae.calculated_age_in_years,
    ae.age_group,
    ae.patient_weight_lbs,
    ae.reporter_country,
    ae.reaction_meddra_pt,
    ae.reaction_outcome_cd,
    ae.mapped_outcome,
    ae.receipt_date,
    ae.receive_date,
    dt.year as receive_date_year,
    dt.month as receive_date_month,
    dt.quarter as receive_date_quarter,
    ae.is_serious,
    ae.seriousness_death,
    ae.seriousness_hospitalization,
    ae.seriousness_life_threatening,
    ae.seriousness_disabling,
    ae.seriousness_congenital_anomaly,
    ae.seriousness_other,
    ae.date_pulled
FROM {{ ref('stg_adverse_events') }} ae
JOIN {{ ref('dim_date') }} dt ON ae.receive_date = dt.date_key
