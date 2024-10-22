SELECT * 
FROM {{ ref('adverse_events_obt') }}
WHERE receipt_date > current_date() 
OR receipt_date < '1900-01-01'