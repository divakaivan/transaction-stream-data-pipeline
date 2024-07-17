{{ config(
    materialized='table'
) }}

with dim_source_details_data as (
    select
        id,
        source_address_city as address_city,
        source_address_country as address_country,
        source_address_line1 as address_line1,
        source_address_line1_check as address_line1_check,
        source_address_line2 as address_line2,
        source_address_state as address_state,
        source_address_zip as address_zip,
        source_address_zip_check as address_zip_check,
        source_brand as brand,
        source_country as country,
        source_customer as customer,
        source_cvc_check as cvc_check,
        source_dynamic_last4 as dynamic_last4,
        source_exp_month as exp_month,
        source_exp_year as exp_year,
        source_fingerprint as fingerprint,
        source_funding as funding,
        source_id as source_id,
        source_last4 as last4,
        source_name as "name",
        source_object as object_type,
        source_tokenization_method as tokenization_method,
        source_wallet as wallet,
        source_transfer as transfer
    from {{ ref('stg_transactions') }}
)

select * from dim_source_details_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}