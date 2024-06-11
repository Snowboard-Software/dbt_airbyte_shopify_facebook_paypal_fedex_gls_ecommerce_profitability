/*
Transaction fees for éayments made through the shopify payments gateway (Shop Pay, Apple Pay, credit cards etc)
are not found in the order or transactions tables but in the shopify_balances as these fees are applied at the level of payouts
that aggregate different orders. The table contains data for orders, refund and the aggregated payouts.
Source_order_id is null for rows the correspond to payouts.

*/

{{
   config(
    materialized='table'
) 
}}


SELECT 
    id, 
    source_order_id, 
    fee,
    net,
    amount, 
    type
from {{ source('raw_airbyte_data', 'shopify_balance_transactions')}}

  
