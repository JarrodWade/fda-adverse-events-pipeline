{{ config(materialized='table') }}

SELECT
    ae.safetyreportid AS safety_report_id,
    ae.drug_id,
    d.drug_name,
    ae.patient_id,
    ae.reportercountry AS reporter_country,
    ae.reactionmeddrapt AS reaction_meddra_pt,
    ae.reactionoutcome AS reaction_outcome,
    ae.mapped_outcome AS mapped_outcome,
    ae.receiptdate AS receipt_date,
    ae.receivedate AS receive_date,
    ae.serious AS is_serious,
    ae.seriousnessdeath AS seriousness_death,
    ae.seriousnesshospitalization AS seriousness_hospitalization,
    ae.seriousnesslifethreatening AS seriousness_life_threatening,
    ae.seriousnessdisabling AS seriousness_disabling,
    ae.seriousnesscongenitalanomali AS seriousness_congenital_anomaly,
    ae.seriousnessother AS seriousness_other,
    dt.date_day AS receive_date_day,
    dt.year AS receive_date_year,
    dt.month AS receive_date_month,
    ae.date_pulled AS date_pulled
FROM {{ ref('stg_adverse_events') }} ae
JOIN {{ ref('dim_drug') }} d ON ae.drug_id = d.drug_id
JOIN {{ ref('dim_patient') }} p ON ae.patient_id = p.patient_id
JOIN {{ ref('dim_date') }} dt ON ae.receivedate = dt.date_day