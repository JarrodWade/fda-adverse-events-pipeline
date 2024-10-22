-- tests/assert_weight_conversion_accurate.sql

-- This test checks if the Weight_lbs is accurately calculated
SELECT
    patient_id,
    patient_weight_kg,
    patient_weight_lbs,
    ABS((patient_weight_kg * 2.20462) - patient_weight_lbs) as conversion_diff
FROM {{ ref('stg_adverse_events') }}
WHERE conversion_diff > 0.01  -- allowing for small floating point discrepancies