{% macro map_outcome(outcome_code) %}
    CASE {{ outcome_code }}
        WHEN 1 THEN 'Recovered/resolved'
        WHEN 2 THEN 'Recovering/resolving'
        WHEN 3 THEN 'Not recovered/not resolved'
        WHEN 4 THEN 'Recovered/resolved with sequelae'
        WHEN 5 THEN 'Fatal'
        WHEN 6 THEN 'Unknown'
        ELSE 'Invalid Outcome Code'
    END
{% endmacro %}