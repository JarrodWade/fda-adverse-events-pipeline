{{ config(materialized='view') }}

SELECT
    safetyreportid,
    drug_name,
    medicinalproduct,
    {{ dbt_utils.generate_surrogate_key(['drug_name', 'medicinalproduct']) }} AS drug_id,
    drugcharacterization,
    drugindication,
    reportercountry,
    qualification,
    reactionmeddrapt,
    reactionoutcome,
    {{ map_outcome('reactionoutcome') }} AS mapped_outcome,
    receiptdate,
    receivedate,
    transmissiondate,
    patientonsetage,
    patientonsetageunit,
    {{ calculate_age_in_years('patientonsetage', 'patientonsetageunit') }} AS calculated_age_in_years,
    patientweight,
    patientsex,
    {{ dbt_utils.generate_surrogate_key(['patientonsetage', 'patientonsetageunit', 'patientsex', 'patientweight']) }} AS patient_id,
    -- Note: This is a simulated patient ID for learning / project purposes. 
    -- Ideally, in a production business environment, this would be a real unique identifier.
    serious,
    seriousnessdeath,
    seriousnesshospitalization,
    seriousnesslifethreatening,
    seriousnessdisabling,
    seriousnesscongenitalanomali,
    seriousnessother,
    occurcountry,
    primarysourcecountry,
    fulfillexpeditecriteria,
    reporttype,
    date_pulled
FROM {{ source('raw', 'ADVERSE_EVENTS_RAW') }}