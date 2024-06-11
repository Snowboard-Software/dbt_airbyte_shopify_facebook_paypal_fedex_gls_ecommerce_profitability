/*
Our ad sales tracked by UTM or by Facebook's conversion API are only 30% of e-commerce orders however we estimate orders coming from ads amounting to at least 70% of sales.
This leads us to establish 2 models for marketing:
1. To approximate a good proxy measure for e-commerce orders we divide the total spend by country per country from this table.
We will then divide by count of orders per month per country. This means an order will be estimate to have the average marketing spend by country that month.
2. To benchmark and compare ads performance: we will only work on orders where we can link a specific ad to a sale.

To do this we extract from the int_ecomm_orders table the count of monthly orders per country.
We extract from stg_facebook_ads_insights_country the sum of spend for a country in a year.
We then divide these two to obtain the average paid marketing cost per month and combine this with the order and year
to thus get the modelled paid marketing cost for each order.

*/


{{
   config(
    materialized='table'
)
}}

WITH month_fb_costs AS (
    select
        extract(year from date_stop) as ad_year,
        extract(month from date_stop) as ad_month,
        country,
        max(date_stop) as date_stop,
        sum(spend) as fb_spend
    from {{ref('stg_facebook_ads_insights_country')}}
    group by 1, 2, 3
), month_ecomm_order AS (
    select
        extract(year from created_at) as order_year,
        extract(month from created_at) as order_month,
        country_code,
        count(distinct id) as nbr_orders
    from {{ref('int_ecomm_orders')}}
    group by 1, 2, 3
), ecomm_orders AS (
    select
        id,
        created_at,
        extract(year from created_at) as order_year,
        extract(month from created_at) as order_month,
        country_code
    from {{ref('int_ecomm_orders')}}
)

select
    eo.id,
    eo.created_at,
    eo.order_year,
    eo.order_month,
    eo.country_code,
    fc.fb_spend as monthly_country_paidmktg,
    mo.nbr_orders as monthly_nbr_country_orders,
    CASE
        WHEN mo.nbr_orders = 0 then 0
        ELSE (fc.fb_spend / mo.nbr_orders)
    END as avg_paidmktgcosts_per_order

from ecomm_orders eo
LEFT JOIN month_fb_costs fc on (eo.order_year = fc.ad_year and eo.order_month = fc.ad_month and eo.country_code = fc.country)
LEFT JOIN month_ecomm_order mo on (eo.order_year = mo.order_year and eo.order_month = mo.order_month and mo.country_code = eo.country_code)
where fc.date_stop > TIMESTAMP("2022-02-02") -- this was simply added to filter all orders pre migration to shopify
