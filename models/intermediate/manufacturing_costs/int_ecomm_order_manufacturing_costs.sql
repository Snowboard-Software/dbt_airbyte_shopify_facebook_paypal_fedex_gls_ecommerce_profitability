/*
Using the data on manufacturing cost per order line we then sum the total cost of all order lines in an order to obtain the manufacturing cost per order.

*/


{{
   config(
    materialized='table'
) 
}}


WITH order_manu_costs AS (
    select * from {{ref('int_ecomm_order_line_manufacturing_costs')}}
)

select 
    max(id) as order_id,
    sum(total_manufacturing_cost) as total_manufacturing_costs
from order_manu_costs
group by id
