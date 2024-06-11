-- Here we add together for each order fees from all our gateways, in our case Shopify, Paypal, and Klarna



{{
   config(
    materialized='table'
) 
}}


with paypal_fee as (
    select * from {{ ref('int_paypal_fees_from_shop_transactions') }}
), shop_fee as (
    select * from {{ ref('int_shop_fees_from_shop_transactions') }}
), klarna_fee as (
    select * from {{ ref('int_klarna_fees_from_shop_transactions') }}
)

select 
    --sf.order_id as order_id_shopi,
    --pf.order_id as order_id_payp,
    coalesce(sf.order_id, pf.order_id, kf.order_id) as o_id,
    IFNULL(pf.total_paypal_fees_EUR,0) as total_paypal_fees_EUR,
    IFNULL(sf.shopify_commission_fees_EUR,0) as shopify_commission_fees_EUR, 
    IFNULL(kf.klarna_commision_fee_EUR,0) as klarna_commission_fees_EUR, 
    IFNULL(pf.total_paypal_fees_EUR,0) + IFNULL(sf.shopify_commission_fees_EUR,0) + IFNULL(kf.klarna_commision_fee_EUR,0) as total_commision_EUR
from paypal_fee pf 
full outer join shop_fee sf on sf.order_id = pf.order_id
full outer join klarna_fee kf ON kf.order_id = sf.order_id
