{{ config(
    materialized='table'
) }}

with dim_forex_rates_data as (
    select
        "date" as date_rates,
        usd,
        eur,
        jpy,
        cad,
        aud,
        chf,
        cny,
        sek,
        nzd,
        mxn
    from {{ source('postgres', 'forex_rates') }}
)

select * from dim_forex_rates_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}