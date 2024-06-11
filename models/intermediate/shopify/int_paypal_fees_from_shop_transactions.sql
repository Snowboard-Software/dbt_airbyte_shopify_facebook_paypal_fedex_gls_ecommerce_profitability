{{
   config(
    materialized='table'
) 
}}

/* 
INFO
the purpose of this model is to find in EUR paypal fees
one of the porblem was that shopify only returned exchange rate from local currency and commission in local currency 
the conversion fee was hidden and transaction fees are returned in local currency.


We had three cases:
1. Local payment in euro. The fee is in euro and there's no converson fee.
2. Foreign card paying in euro, there we have all the fee data in euro, no conversion fee as the customer pays it on their end. We insert an implicit exchange rate of 1.
Paypal commision in euro is equal to fee amount currency local and there is no conversion fee as it's paid in euros.
3. Foreign card payment in foreign currency and automatic paypal conversion. Here we have local currency fee in (?) and a paypal conversion exchange rate given by paypla.
We can then compute the fee in euros by applying that conversion rate. We can also compute the exchange fee implicit in the exchange rate as paypal gives settle amunt
as the net amount in euro left after conversion, we use the formula conversion fee = amount of the order - (fee_amount_local_currency * po.exchange_rate_cleaned) - settle_amount 
4. Foreign card payment in foreign currency without automatic paypal conversion.
Here we used the average conversion fee of circa 3% on our orders with automatic conversions on the amount of the order. Local fee is converted to euro by computing
an exchange rate from shopify data where we have the local currency amount calculated by shopify divided by the euro amount given by shopify and use this a proxy
for the paypal exchange rate which is similar but not equal.

QUESTIONS: 
- is the filter correct: where kind = 'SALE' and gateway = 'paypal' and status = 'SUCCESS'
It's kind = sale and also capture for amex cards
- What happens to fees after refund? 
*/ 


with paypal_fees as ( 
    select * from {{ ref('stg_shopify_transactions') }}
), orders as (
    select 
        *, 
        CASE 
            when gross_sales_LC = 0 then 1
            ELSE gross_sales_EUR/gross_sales_LC 
        END  as exchange_rate_from_orders
    from {{ ref('int_ecomm_orders') }}
), paypal_fees_from_orders as (
    select 
        pf.order_id,
        oo.exchange_rate_from_orders,
        CASE 
            when pf.fee_amount_currency_id = 'EUR' then 1 --removes cases where foreigner pays in euros, exchnage rate fees is paid by customer, written in shopify
            when pf.fee_amount_currency_id <> 'EUR' and pf.exchange_rate is not null then pf.exchange_rate
            when pf.fee_amount_currency_id <> 'EUR' and pf.exchange_rate is null then oo.exchange_rate_from_orders
        END as exchange_rate_cleaned
    from paypal_fees pf
    left join orders oo on oo.id = pf.order_id 
    where (pf.kind = 'SALE' or pf.kind = 'CAPTURE') and pf.gateway = 'paypal' and pf.status = 'SUCCESS' and pf.fee_amount_currency_id <> 'EUR'
), calc_paypal_fees as (
    SELECT 
        id,
        kind, 
        gateway,
        status,
        pf.order_id as order_id,
        amount, --gross revenue from order as given to the customer by shopify in EUR
        settle_amount, --amount in EUR given by shopify transac of how much money you actually get after commission fee and currency conversion fee 
        fee_amount_currency_id,
        --fee_amount_local_currency,
        --exchange_rate,
        po.exchange_rate_cleaned,
        created_at,
        CASE 
            when exchange_rate_cleaned is null and fee_amount_currency_id = 'EUR' then fee_amount_local_currency
            when exchange_rate_cleaned is not null and  fee_amount_currency_id = 'EUR' then fee_amount_local_currency --exchange rate is given by paypal and include some conversion fee
            when exchange_rate_cleaned is not null and  fee_amount_currency_id <> 'EUR' then fee_amount_local_currency * po.exchange_rate_cleaned --exchange rate is given by paypal and include some conversion fee
        END as paypal_commision_fee_EUR,
        CASE 
            when fee_amount_currency_id = 'EUR' then 0
            when fee_amount_currency_id <> 'EUR' and exchange_rate is null then (0.0311 * amount) 
            when fee_amount_currency_id <> 'EUR' and exchange_rate is not null then (amount - (fee_amount_local_currency * po.exchange_rate_cleaned) - settle_amount)
        END as paypal_conversion_fee_EUR
    from paypal_fees pf
    left join paypal_fees_from_orders po on pf.order_id = po.order_id
    where (kind = 'SALE' or pf.kind = 'CAPTURE') and gateway = 'paypal' and status = 'SUCCESS'
)

select 
    *,
    paypal_commision_fee_EUR + paypal_conversion_fee_EUR  as total_paypal_fees_EUR
from calc_paypal_fees

