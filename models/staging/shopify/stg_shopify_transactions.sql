/*
The shopify transactions table contains information relevant to orders made paying with Paypal.
We first filter to only select succesfull payments for an order made with the Paypal gateway.
In Paypal we have three types of orders:
1. Payments in our shop's currency (euros) which has a paypal transaction fee
2. Payments in a foreign currency with automatic conversion. These have a fee expressed in the foreign currency, an exchange rate and a net residual amount given by Paypal.
This indicates an implicit conversion fee through the exchange rate and is found in the difference between the amount which should come after transaction fee and the actual settle_amount.
3. Payments in a foreign currency without automatic conversion. This means the funds are added after the transaction fee into a paypal balance in the foreign currency which can then be converted to euro later on along with the funds from other orders.
In this table those rows will have local currency fee but no exchange rate or settle amount.
*/


{{
   config(
    materialized='table'
) 
}}

WITH cleaned_receipt_table AS (
  SELECT 
    *, 
    SUBSTR(replace(to_json_string(receipt),'\\',''), 2, LENGTH(replace(to_json_string(receipt),'\\','')) - 2) as cleaned_receipt
  from {{ source('raw_airbyte_data', 'shopify_transactions')}}
  --where id = 7371284971852
  --where kind = 'SALE' and gateway = 'paypal' and status = 'SUCCESS'
)

--/*
SELECT
  _airbyte_raw_id,
  _airbyte_extracted_at,
  id,
  order_id, 
  kind, 
  status, 
  gateway,
  amount,
  created_at, 
  CAST(JSON_EXTRACT_SCALAR(cleaned_receipt, '$.fee_amount') as NUMERIC) AS fee_amount_local_currency,
  JSON_EXTRACT_SCALAR(cleaned_receipt, '$.fee_amount_currency_id') AS fee_amount_currency_id,
  CAST(JSON_EXTRACT_SCALAR(cleaned_receipt, '$.settle_amount') as NUMERIC) AS settle_amount,
  CAST(JSON_EXTRACT_SCALAR(cleaned_receipt, '$.exchange_rate') as NUMERIC) AS exchange_rate
  -- Nested `PaymentInfo` fields (repeating some fields to showcase the structure, adjust as needed)
  --JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(cleaned_receipt, '$.PaymentInfo'), '$.TransactionID') AS PaymentInfo_TransactionID,
  -- Nested `PaymentInfo.SellerDetails` fields
  --JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(cleaned_receipt, '$.PaymentInfo'), '$.SellerDetails'), '$.PayPalAccountID') AS SellerDetails_PayPalAccountID
FROM cleaned_receipt_table
--*/

  
