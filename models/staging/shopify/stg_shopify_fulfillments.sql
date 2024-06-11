/*
This contains the tracking number for each fullfilment, as an order can have several shipments a single order can be 
affiliated with several tracking numbers
*/

{{
   config(
    materialized='table'
) 
}}


SELECT 
    id,
    order_id,
    tracking_number,
    tracking_company
from {{ source('raw_airbyte_data', 'shopify_fulfillments')}}

  