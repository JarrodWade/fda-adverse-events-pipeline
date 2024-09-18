{% macro get_outcome_values() %}
    {{ return(
        [
            (1, 'Recovered/resolved'),
            (2, 'Recovering/resolving'),
            (3, 'Not recovered/not resolved'),
            (4, 'Recovered/resolved with sequelae'),
            (5, 'Fatal'),
            (6, 'Unknown')
        ]
    ) }}
{% endmacro %}