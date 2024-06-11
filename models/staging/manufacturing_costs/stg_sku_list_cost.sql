/*
Our manufacturing cost is calculated for each SKU by taking the yearly order to our producers and dividing the dollar amount by the average usd/euro exchange rate for that year to obtain the euro purchase cost.
Using the weight of each piece and the kilo rate per shipment from our production facilities to distriubtion warehouse we also get the shipping cost per sku.
We thus have a total manufacturing cost per sku which we add to a gsheet along with the name, size, year and category of reference.
We then pull this data into our data warehouse via the Gsheet airbyte connector. 
*/


{{
   config(
    materialized='table'
) 
}}

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    sku,
    CAST (cost as NUMERIC) as cost,
    name,
    size,
    year,
    category

from {{ source('raw_airbyte_data', 'gsheet_sku_list_cost') }}