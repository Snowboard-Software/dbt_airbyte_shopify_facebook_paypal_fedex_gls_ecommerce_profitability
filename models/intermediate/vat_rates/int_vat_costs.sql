/*
To calculate VAT per order we join the order table and refunds table to the vat rates tables.
We have for each order a country code and a year which corresponds to a vat rate and a fiscal year for a country with the same country code in the vat table.
We have three scenarios:
1. If there is no VAT in the country the order's VAT is 0.
2. If the country is not the UK we simply multiply whatever amount is left after refunds by the country's VAT rate.
3. But in the UK VAT is paid by the client when the order's residual amount shipping excluded is above the equivalent of 135 pounds.
If the order post refund and shipping excluded amount in pounds is under 135 pounds then we have the previous calculation for the VAT amount.
We use the subtotal price field to have price excluding shipping we then minus any refunds.
*/


{{
   config(
    materialized='table'
)
}}

with order_tab as (
    select * from {{(ref('int_ecomm_orders'))}}
), vat_tab as (
    select * from {{(ref('stg_vat_rates'))}}
), refund_orders as (
    select * from {{(ref('int_ecomm_order_refund'))}}
)

select
    ot.id,
    ot.subtotal_price,
    ot.country_code,
    vt.vat_rate,
    ot.gross_sales_EUR,
    ot.gross_sales_LC,
    ro.total_refund_amount_EUR,
    CASE
        when vt.vat_rate is null then 0
        when vt.vat_rate is not null and ot.country_code != 'GB' then ROUND(((vt.vat_rate/ (100+vt.vat_rate))  * (ot.gross_sales_EUR - ro.total_refund_amount_EUR)),2)
        when vt.vat_rate is not null and ot.country_code = 'GB' and (ot.subtotal_price - ro.total_refund_amount_EUR) > (135 * (ot.gross_sales_EUR/ gross_sales_LC)) then 0
        when vt.vat_rate is not null and ot.country_code = 'GB' and (ot.subtotal_price - ro.total_refund_amount_EUR) < (135 * (ot.gross_sales_EUR/ gross_sales_LC)) then ROUND(((vt.vat_rate/ (100+vt.vat_rate))  * (ot.subtotal_price - ro.total_refund_amount_EUR)),2)
    END as vat_fee
from order_tab ot
LEFT JOIN vat_tab vt on vt.country_code = ot.country_code and vt.year = CAST(extract(year from ot.created_at) as string)
LEFT JOIN refund_orders ro on ro.id = ot.id
