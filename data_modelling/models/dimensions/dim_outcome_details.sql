{{ config(
    materialized='table'
) }}

with dim_outcome_details_data as (
    select
        id,
        outcome_network_status as network_status,
        outcome_reason as reason,
        outcome_risk_level as risk_level,
        outcome_risk_score as risk_score,
        outcome_seller_message as seller_message,
        outcome_type
    from {{ ref('stg_transactions') }}
)

select * from dim_outcome_details_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}