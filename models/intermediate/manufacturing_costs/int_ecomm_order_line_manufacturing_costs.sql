
/*
We join our order line items and manufacturing cost and obtain the item unit cost.
We obtain total manufacturing cost for each line item of our Shopify orders by multiplying the item unit cost by the quantity.
*/
{{
   config(
    materialized='table'
) 
}}


WITH order_line AS (
    select * from {{ref('int_ecomm_orderlineitems')}}
), sku_costs as ( 
    select * from {{ref('stg_sku_list_cost')}}
)

select 
    ol.id, --order id 
    ol.line_item_id,
    sc.sku as sku_gsheet, 
    ol.sku_cleaned as sku_shop,
    sc.year, 
    sc.cost as item_unit_cost,
    --st.created_at,
    ol.created_at, 
    ol.quantity, 
    (ol.quantity * sc.cost) as total_manufacturing_cost
from order_line ol 
--LEFT JOIN order_t ot on ol.id = ot.id
--LEFT JOIN shop_transac st on st.order_id = ol.id
LEFT JOIN sku_costs sc on (ol.sku_cleaned = sc.sku and EXTRACT(YEAR FROM ol.created_at) = CAST(sc.year as INT64))