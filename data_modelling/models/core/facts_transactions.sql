{{ config(
    materialized='table'
) }}

with transactions as (
    select
        t.id as transaction_id,
        t.created as transaction_date,
        t.amount,
        t.amount_captured,
        t.amount_refunded,
        t.currency,
        t.customer,
        t.payment_intent,
        t.payment_method,
        t.status,
        fr.usd as forex_usd,
        fr.eur as forex_eur,
        fr.jpy as forex_jpy,
        fr.cad as forex_cad,
        fr.aud as forex_aud,
        fr.chf as forex_chf,
        fr.cny as forex_cny,
        fr.sek as forex_sek,
        fr.nzd as forex_nzd,
        fr.mxn as forex_mxn
    from {{ ref('stg_transactions') }} t
    left join {{ ref('dim_forex_rates') }} fr on cast(t.created as date) = cast(fr.date_rates as date)
)

select * from transactions

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}
