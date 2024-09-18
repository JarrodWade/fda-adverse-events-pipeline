{% macro test_unique_combination(model, combination) %}
    select
        {{ combination | join(', ') }},
        count(*)
    from {{ model }}
    group by {{ combination | join(', ') }}
    having count(*) > 1
{% endmacro %}