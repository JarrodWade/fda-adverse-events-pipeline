{% macro calculate_age_in_years(age_value, age_unit) %}
    CASE
        WHEN {{ age_unit }} = '800' THEN {{ age_value }}::float * 10  -- Decade
        WHEN {{ age_unit }} = '801' THEN {{ age_value }}::float  -- Year
        WHEN {{ age_unit }} = '802' THEN {{ age_value }}::float / 12  -- Month
        WHEN {{ age_unit }} = '803' THEN {{ age_value }}::float / 52  -- Week
        WHEN {{ age_unit }} = '804' THEN {{ age_value }}::float / 365  -- Day
        WHEN {{ age_unit }} = '805' THEN {{ age_value }}::float / (365 * 24)  -- Hour
        ELSE NULL
    END
{% endmacro %}