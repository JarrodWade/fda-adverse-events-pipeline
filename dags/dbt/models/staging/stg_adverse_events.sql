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
    {{ map_outcome('reaction_outcome_cd') }} AS mapped_outcome, -- map outcome from code
    receiptdate as receipt_date,
    receivedate as receive_date,
    transmissiondate as transmission_date,
    patientonsetage as patient_age,
    patientonsetageunit as patient_age_unit,
    {{ calculate_age_in_years('patient_age', 'patient_age_unit') }} AS calculated_age_in_years, -- calculate age in years
    patientweight as patient_weight_kg, -- currently in kg
    patientsex as patient_sex,
    {{ determine_age_group('calculated_age_in_years') }} AS age_group, --determine age group
    {{ kg_to_lbs('patient_weight_kg') }} AS patient_weight_lbs, -- convert kg to lbs
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
    datepulled as date_pulled --timestamp
FROM {{ source('raw', 'ADVERSE_EVENTS_RAW') }}