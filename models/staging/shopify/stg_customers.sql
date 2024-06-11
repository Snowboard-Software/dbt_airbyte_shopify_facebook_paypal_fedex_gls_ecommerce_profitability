/*
List of historical e-commerce customers and relevant data
*/

{{
   config(
    materialized='table'
) 
}}



select 
    id,
    first_name,
    last_name,
    email, 
    created_at
from {{ source('raw_airbyte_data', 'shopify_customers')}}