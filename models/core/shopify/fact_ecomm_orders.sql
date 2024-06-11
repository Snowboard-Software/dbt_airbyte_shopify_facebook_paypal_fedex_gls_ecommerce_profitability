/*
This is our final table for profitability
We select all relevant fields from our intermediate tables.

Each row in this table represents an order identified by its id as our primary key.
Each record contains information about the order and costs associated with it (shipping cost, manufacturing cost, )

We calculate our profit variables as:
Gross Sales in Euro - Refund amount in euro = Gross Revenue
Gross Revenue - Vat = Net Revenue
COGS = Total Manufacturing Cost + Shipping Costs + Total commission
Net Revenue - COGS - Marketing Spend = Gross Profit

Some orders where shipped to a country where we don't market in that month so the value of marketing_cost is null
so we set it to 0

*/


{{ config(materialized='table') }}

with order_tab as (
    select * from {{ ref('int_ecomm_orders') }}
),
refund_tab as (
    select * from {{ ref('int_ecomm_order_refund') }}
),
vat_tab as (
    select * from {{ ref('int_vat_costs') }}
),
mktg_tab as (
    select * from {{ ref('int_orders_paidmktgcosts') }}
),
manuf_tab as (
    select * from {{ ref('int_ecomm_order_manufacturing_costs') }}
),
shipping_tab as (
    select * from {{ ref('int_ecomm_shipping_costs') }}
),
commission_tab as (
    select * from {{ ref('int_commissionfee_orders') }}
),
orderlineitems_tab as ( --we aggregate in order to not have duplicate rows after our join as there are several line item rows per order
    select
        id,
        sum(quantity) as total_quantity
    from {{ ref('int_ecomm_orderlineitems') }}
    group by id
)


select
    ot.*,
    rt.total_refund_amount_EUR,
    ROUND((ot.gross_sales_EUR - IFNULL(rt.total_refund_amount_EUR, 0)), 2) as gross_revenue_EUR,
    vt.vat_fee,
    ROUND((ot.gross_sales_EUR - IFNULL(rt.total_refund_amount_EUR, 0) - vt.vat_fee), 2) as net_revenue_EUR,
    ROUND(IFNULL(mkt.avg_paidmktgcosts_per_order, 0), 2) as marketing_cost,
    ROUND(mt.total_manufacturing_costs, 2) as manufacturing_cost,
    st.shipping_costs,
    st.tracking_number,
    ct.total_commision_EUR,
    ROUND((mt.total_manufacturing_costs + st.shipping_costs + ct.total_commision_EUR), 2) as cogs_EUR,
    ct.total_paypal_fees_EUR,
    ct.shopify_commission_fees_EUR,
    ct.klarna_commission_fees_EUR,
    CASE  --case statement to eliminate rows with cancelled orders where we incur marketing cost but manufcturing, shipping or tax
        WHEN st.shipping_costs IS NULL AND ot.gross_sales_EUR - IFNULL(rt.total_refund_amount_EUR, 0) < 20 THEN -ROUND(IFNULL(mkt.avg_paidmktgcosts_per_order, 0), 2)
        ELSE ROUND((ot.gross_sales_EUR - IFNULL(rt.total_refund_amount_EUR, 0) - vt.vat_fee - (IFNULL(mkt.avg_paidmktgcosts_per_order, 0) + mt.total_manufacturing_costs + st.shipping_costs + ct.total_commision_EUR)), 2)
    END as gross_profit_EUR
    --add later projected profit as we don't get live data for shipping until 2 months later using projected cost and projected profit

from order_tab ot
left join refund_tab rt on ot.id = rt.id
left join vat_tab vt on ot.id = vt.id
left join mktg_tab mkt on ot.id = mkt.id
left join manuf_tab mt on ot.id = mt.order_id
left join shipping_tab st on ot.id = st.id
left join commission_tab ct on ot.id = ct.o_id
inner join orderlineitems_tab oli on ot.id = oli.id
