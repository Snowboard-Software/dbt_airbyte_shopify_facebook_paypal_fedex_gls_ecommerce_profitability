/*
Our intermediary refunds table from int_ecom_orders and stg_shopify_transactions. When a refund does not exist we set a default refund value of zero otherwise we use 
the amount column which contains the euro amount of the refund.
We find refunds in the transactions table by selecting records where the kind of transaction is a refund and the status is success. We also added pending as some refunds actually were debited to us but are marked as pending in the table
*/


{{
   config(
    materialized='table'
) 
}}

with order_tab as ( 
    select * from {{(ref('int_ecomm_orders'))}}
), refund_tab as ( 
    select 
        id as refund_id,
        order_id,
        kind, 
        status,
        CASE 
            when amount is null then 0
            ELSE amount
        END as refund_amount_EUR,
        created_at
    from {{(ref('stg_shopify_transactions'))}}
    where kind ='REFUND' and (status = 'SUCCESS' or status = 'PENDING')
), refund_orders as (
    select 
        ot.id, 
        CASE 
            when refund_amount_EUR is null then 0 
            ELSE refund_amount_EUR
        END as refund_amount_EUR
    from order_tab ot
    LEFT JOIN refund_tab rt on ot.id = rt.order_id 
)

select 
    ro.id, 
    sum(refund_amount_EUR) as total_refund_amount_EUR
from refund_orders ro
group by 1



