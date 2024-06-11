/*
Overall Spring as a shipping service is a lot less  transparent than fedex. They  quote a certain price and then a lot of costs get added as supplements, fo fuel, for pick up etc. 
Ok so multiple things were dealt with in this table: 
1) Many orders were actually broken down into multiple subtotal costs and derivatives of tracking number, so we have to aggregate them on some clean 
tracking number 
2) More annoying, there is a pick up cost for th emerchandise that spring charges extra, taht is aggregated from multiple orders and have no tracking number that can be tied to Shopify order_id, so we have to 
acccount for these costs and distribute them accross all orders for that month where pick up costs incurred
3) Finally  some orders starting with '1Z96' should not receive any additional costs from pick up

*/

{{
   config(
    materialized='table'
) 
}}


WITH cleaned_spring as (
    select * FROM {{ ref('stg_spring_shipping_costs') }}
), agg_spring_costs as ( 
    SELECT
        cleaned_tracking_number, 
        sum(net_amount_euro) as total_net_shipping_amount_EUR, 
        max(recipient_country) as recipient_country, 
        max(service_description) as service_description, 
        max(proper_timestamp) as cleaned_timestamp, 
        max(extract(month from proper_timestamp)) as order_month, 
        max(extract(year from proper_timestamp)) as order_year
    FROM cleaned_spring
    where service_description != 'PICK-UP'
    group by cleaned_tracking_number
), count_order_month as (
    select 
        count(cleaned_tracking_number) as month_count_cl_track_number,
        EXTRACT(month from cleaned_timestamp) as order_month, 
        EXTRACT(year from cleaned_timestamp) as order_year
    from agg_spring_costs
    where service_description != 'PICK-UP' and cleaned_tracking_number NOT LIKE '1Z96%'
    group by 2, 3
), total_pickup_costs_month as (
    select 
        sum(net_amount_euro) as month_total_pickup_costs,
        EXTRACT(month from proper_timestamp) as order_month, 
        EXTRACT(year from proper_timestamp) as order_year
    from cleaned_spring 
    where service_description = 'PICK-UP'
    group by 2, 3
)

--select * from agg_spring_costs
--select * from total_pickup_costs_month_country


select 
    ac.cleaned_tracking_number, 
    ac.cleaned_timestamp,
    ac.order_month,
    ac.order_year,
    ac.recipient_country,
    ac.total_net_shipping_amount_EUR, 
    CASE 
        WHEN cleaned_tracking_number LIKE '1Z96%' then 0 
        ELSE IFNULL(co.month_count_cl_track_number,0) END as month_count_cl_track_number, 
    CASE 
        WHEN cleaned_tracking_number LIKE '1Z96%' then 0 
        ELSE IFNULL(tp.month_total_pickup_costs, 0) END as month_total_pickup_costs,
    CASE 
        WHEN cleaned_tracking_number LIKE '1Z96%' then IFNULL(ac.total_net_shipping_amount_EUR,0)
        ELSE IFNULL((IFNULL(tp.month_total_pickup_costs,0)/ co.month_count_cl_track_number),0) + IFNULL(ac.total_net_shipping_amount_EUR,0) END as total_net_ship_incl_pickup_amount_EUR
from agg_spring_costs ac
left join count_order_month co on (co.order_month = ac.order_month and co.order_year = ac.order_year)
left join total_pickup_costs_month tp on (tp.order_month = ac.order_month and tp.order_year = ac.order_year)
--where ac.recipient_country = 'IT'




