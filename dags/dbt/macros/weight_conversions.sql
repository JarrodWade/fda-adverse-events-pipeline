-- macros/weight_conversions.sql

{% macro kg_to_lbs(kg_column) %}
    ({{ kg_column }} * 2.20462)
{% endmacro %}