-- models/staging/stg_adverse_events.sql
{{ config(materialized='view') }}

SELECT
    safetyreportid as safety_report_id,
    drugname as drug_name,
    medicinalproduct as drug_product,
    drugcharacterization as drug_characterization,
    drugindication as drug_indication,
    reportercountry as reporter_country,
    qualification as qualification,
    reactionmeddrapt as reaction_meddra_pt,
    reactionoutcome as reaction_outcome_cd,
    receiptdate as receipt_date,
    receivedate as receive_date,
    transmissiondate as transmission_date,
    patientonsetage as patient_age,
    patientonsetageunit as patient_age_unit,
    patientweight as patient_weight_kg,
    patientsex as patient_sex,
    serious as is_serious,
    seriousnessdeath as seriousness_death,
    seriousnesshospitalization as seriousness_hospitalization,
    seriousnesslifethreatening as seriousness_life_threatening,
    seriousnessdisabling as seriousness_disabling,
    seriousnesscongenitalanomali as seriousness_congenital_anomaly,
    seriousnessother as seriousness_other,
    occurcountry as occur_country,
    primarysourcecountry as primary_source_country,
    fulfillexpeditecriteria as fullfill_expedite_criteria,
    reporttype as report_type,
    datepulled as date_pulled
FROM {{ source('raw', 'ADVERSE_EVENTS_RAW') }}