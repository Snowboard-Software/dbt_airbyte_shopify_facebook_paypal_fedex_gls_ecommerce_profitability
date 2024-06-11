/*
We filter only orders from stg_shopify_balance_transactions by filtering for type charge. We then have displayex the amount a customer paid as well as shopify's commison
and the net amount paid by shopify after fees, expressed in euros.
*/

{{
   config(
    materialized='table'
) 
}}

with calc_shop_fees as (
    SELECT 
        id,
        source_order_id as order_id, 
        type,
        fee as shopify_commission_fees_EUR,
        amount as amount_EUR, 
        net as net_revenue_after_shop_fees_EUR
    from {{ ref('stg_shopify_balance_transactions') }}
    where type = 'charge'

)

select * 
from calc_shop_fees

