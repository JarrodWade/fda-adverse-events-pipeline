{% macro determine_age_group(age_in_years) %}
    CASE
        WHEN {{ age_in_years }} < 0.08 THEN 'Neonate'
        WHEN {{ age_in_years }} < 2 THEN 'Infant'
        WHEN {{ age_in_years }} < 12 THEN 'Child'
        WHEN {{ age_in_years }} < 18 THEN 'Adolescent'
        WHEN {{ age_in_years }} < 65 THEN 'Adult'
        ELSE 'Elderly'
    END
{% endmacro %}