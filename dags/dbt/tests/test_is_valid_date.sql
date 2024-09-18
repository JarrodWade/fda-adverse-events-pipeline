SELECT * 
FROM {{ ref('fct_adverse_events') }}
WHERE receipt_date > current_date() 
OR receipt_date < '1900-01-01'