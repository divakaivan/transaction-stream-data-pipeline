{{ config(
    materialized='table'
) }}

with dim_billing_details_data as (
    select
        id,
        billing_details_address_city as address_city,
        billing_details_address_country as address_country, 
        billing_details_address_line1 as address_line1,
        billing_details_address_line2 as address_line2,
        billing_details_address_postal_code as address_postal_code, 
        billing_details_address_state as address_state,
        billing_details_email as email, 
        billing_details_name as "name", 
        billing_details_phone as phone 
    from {{ ref('stg_transactions') }}
)

select * from dim_billing_details_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}