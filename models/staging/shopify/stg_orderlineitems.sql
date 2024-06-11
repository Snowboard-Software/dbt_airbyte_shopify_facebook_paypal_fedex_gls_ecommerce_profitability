/*
Each order is composed of several line items that are an article/variant combination , price, quantity, grammage etc.
Here if we apply discount on an item basis (for example listing some products as 20% off sale) there would be the normal price and discounted price.
In our case we are only offering discount codes so general order discounts are included in the order table.
*/

{{
   config(
    materialized='table'
) 
}}


WITH your_table AS (
  SELECT 
    id,
    shipping_address,
    created_at,
    line_items
  from {{ source('raw_airbyte_data', 'shopify_orders')}}
)
SELECT 
  id,
  shipping_address,
  created_at,
  JSON_EXTRACT_SCALAR(line_item, '$.admin_graphql_api_id') AS admin_graphql_api_id,
  REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(line_item, '$.admin_graphql_api_id'), r'gid://shopify/LineItem/(\d+)') AS line_item_id,
  CONCAT(id, '-', REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(line_item, '$.admin_graphql_api_id'), r'gid://shopify/LineItem/(\d+)')) AS unique_identifier,
  JSON_EXTRACT_SCALAR(line_item, '$.fulfillable_quantity') AS fulfillable_quantity,
  JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_service') AS fulfillment_service,
  JSON_EXTRACT_SCALAR(line_item, '$.fulfillment_status') AS fulfillment_status,
  JSON_EXTRACT_SCALAR(line_item, '$.gift_card') AS gift_card,
  JSON_EXTRACT_SCALAR(line_item, '$.grams') AS grams,
  --JSON_EXTRACT_SCALAR(line_item, '$.id') AS id,
  JSON_EXTRACT_SCALAR(line_item, '$.name') AS name,
  JSON_EXTRACT_SCALAR(line_item, '$.price') AS price,
  JSON_EXTRACT_SCALAR(line_item, '$.product_exists') AS product_exists,
  JSON_EXTRACT_SCALAR(line_item, '$.product_id') AS product_id,
  CAST(JSON_EXTRACT_SCALAR(line_item, '$.quantity') AS NUMERIC) AS quantity,
  JSON_EXTRACT_SCALAR(line_item, '$.requires_shipping') AS requires_shipping,
  JSON_EXTRACT_SCALAR(line_item, '$.sku') AS sku,
  JSON_EXTRACT_SCALAR(line_item, '$.taxable') AS taxable,
  JSON_EXTRACT_SCALAR(line_item, '$.title') AS title,
  JSON_EXTRACT_SCALAR(line_item, '$.total_discount') AS total_discount,
  JSON_EXTRACT_SCALAR(line_item, '$.variant_id') AS variant_id,
  JSON_EXTRACT_SCALAR(line_item, '$.variant_inventory_management') AS variant_inventory_management,
  JSON_EXTRACT_SCALAR(line_item, '$.variant_title') AS variant_title,
  JSON_EXTRACT_SCALAR(line_item, '$.vendor') AS vendor,
  -- Extracting nested fields
  JSON_EXTRACT_SCALAR(line_item, '$.price_set.presentment_money.amount') AS presentment_money_amount,
  JSON_EXTRACT_SCALAR(line_item, '$.price_set.presentment_money.currency_code') AS presentment_money_currency,
  JSON_EXTRACT_SCALAR(line_item, '$.price_set.shop_money.amount') AS shop_money_amount,
  JSON_EXTRACT_SCALAR(line_item, '$.price_set.shop_money.currency_code') AS shop_money_currency,
  JSON_EXTRACT_SCALAR(line_item, '$.total_discount_set.presentment_money.amount') AS total_discount_presentment_money_amount,
  JSON_EXTRACT_SCALAR(line_item, '$.total_discount_set.presentment_money.currency_code') AS total_discount_presentment_money_currency,
  JSON_EXTRACT_SCALAR(line_item, '$.total_discount_set.shop_money.amount') AS total_discount_shop_money_amount,
  JSON_EXTRACT_SCALAR(line_item, '$.total_discount_set.shop_money.currency_code') AS total_discount_shop_money_currency_code
FROM your_table,
UNNEST(JSON_EXTRACT_ARRAY(line_items)) AS line_item
WHERE 
  (JSON_EXTRACT_SCALAR(line_item, '$.name') IS NOT NULL AND
   LOWER(JSON_EXTRACT_SCALAR(line_item, '$.name')) NOT LIKE '%wholesale%' AND
   LOWER(JSON_EXTRACT_SCALAR(line_item, '$.name')) NOT LIKE '%wholsale%' AND
   LOWER(JSON_EXTRACT_SCALAR(line_item, '$.name')) NOT LIKE '%wholsael%')
--we filter out wholesale orders paid on shopify


--total_price = total_line_items_price - total_discounts + total_shipping_price_set(shop_money/amounts) - refunds (shop_money/amounts)