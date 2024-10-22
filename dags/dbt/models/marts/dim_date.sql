-- models/marts/dim_date.sql
{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast(current_date() as date)"
    )
    }}
)

SELECT
    date_key,
    EXTRACT(YEAR FROM date_key) AS year,
    EXTRACT(MONTH FROM date_key) AS month,
    EXTRACT(DAY FROM date_key) AS day,
    EXTRACT(DAYOFWEEK FROM date_key) AS day_of_week,
    EXTRACT(QUARTER FROM date_key) AS quarter
FROM date_spine