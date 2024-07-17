{{ config(
    materialized='table'
) }}

with source_data as (
    select * from {{ source('postgres', 'transactions') }}
)

select * from source_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}