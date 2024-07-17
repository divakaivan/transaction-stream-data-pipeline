{{ config(
    materialized='table'
) }}

with dim_payment_method_details_data as (
    select
        id,
        payment_method_details_card_amount_authorized as card_amount_authorized,
        payment_method_details_card_brand as card_brand,
        payment_method_details_card_checks_address_line1_check as card_checks_address_line1_check,
        payment_method_details_card_checks_address_postal_code_check as card_checks_address_postal_code_check,
        payment_method_details_card_checks_cvc_check as card_checks_cvc_check,
        payment_method_details_card_country as card_country,
        payment_method_details_card_exp_month as card_exp_month,
        payment_method_details_card_exp_year as card_exp_year,
        payment_method_details_card_extended_authorization_status as card_extended_authorization_status,
        payment_method_details_card_fingerprint as card_fingerprint,
        payment_method_details_card_funding as card_funding,
        payment_method_details_card_incremental_authorization_status as card_incremental_authorization_status,
        payment_method_details_card_installments as card_installments,
        payment_method_details_card_last4 as card_last4,
        payment_method_details_card_mandate as card_mandate,
        payment_method_details_card_multicapture_status as card_multicapture_status,
        payment_method_details_card_network as card_network,
        payment_method_details_card_network_token_used as card_network_token_used,
        payment_method_details_card_overcapture_maximum_amount as card_overcapture_maximum_amount,
        payment_method_details_card_overcapture_status as card_overcapture_status,
        payment_method_details_card_three_d_secure as card_three_d_secure,
        payment_method_details_card_wallet as card_wallet,
        payment_method_details_type as "type"
    from {{ ref('stg_transactions') }}
)

select * from dim_payment_method_details_data

{% if var("is_dev_run", default=true) %} limit 100 {% endif %}