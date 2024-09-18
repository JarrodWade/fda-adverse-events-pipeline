{{ config(materialized='table') }}

WITH PATIENT_DATA AS (
    SELECT DISTINCT
        patient_id,
        patientonsetage AS patient_onset_age,
        patientonsetageunit AS patient_onset_age_unit,
        patientsex AS patient_sex,
        patientweight AS patient_weight_kg,
        {{ calculate_age_in_years('patientonsetage', 'patientonsetageunit') }} AS calculated_age_in_years,
        {{ determine_age_group('calculated_age_in_years') }} AS age_group,
        {{ kg_to_lbs('patientweight') }} AS patient_weight_lbs
    FROM {{ ref('stg_adverse_events') }}
)

SELECT
    patient_id,
    patient_onset_age,
    patient_onset_age_unit,
    patient_sex,
    patient_weight_kg,
    patient_weight_lbs,
    calculated_age_in_years,
    age_group
FROM PATIENT_DATA