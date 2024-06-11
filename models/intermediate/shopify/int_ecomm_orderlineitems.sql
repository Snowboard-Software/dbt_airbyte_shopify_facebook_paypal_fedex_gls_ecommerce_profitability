/*
Our intermediary ecom order line items tables where we clean out line items from before transition to Shopify.
We also clean out if shipping address is null as that might represent a whoesale and we need to clean out also if the line item contains a title with a
variation of "Wholesale"
*/


{{
   config(
    materialized='table'
)
}}

select
    *,
    CASE
    WHEN name = 'Luna' THEN 'T01212'
    --Many other examples here of cleaning that needs to happen.
    ELSE sku END as sku_cleaned
from {{(ref('stg_orderlineitems'))}}
where shipping_address is not null
and created_at > TIMESTAMP("2022-02-02")
and name NOT LIKE '%Wholesale%'
