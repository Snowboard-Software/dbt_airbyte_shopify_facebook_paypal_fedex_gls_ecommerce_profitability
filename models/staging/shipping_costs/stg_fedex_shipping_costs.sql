/*
Fedex offers no API to retrieve billed amounts for shipments neither do they allow scraping so we have to proceed with a monthly
report download of the entire billing history which is then copy pasted into a google sheet and pulled into our data warehouse via the Airbyte Google Sheets connector.
We have for each tracking number the amount net of VAT charged to us, recipient country, and the Fedex service used (allowing us to compare rates for standard and express deliveries)
*/

{{
   config(
    materialized='table'
) 
}}


with source as (
    select * from {{ source('raw_airbyte_data', 'shipping_fedex_shipping_cost') }}
)

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    --_airbyte_meta,
    CAST(REPLACE(NetChargeAmountBilledCurrency, ',', '.') as DECIMAL) as net_amount_euro,
    --recipient_name,
    ShipmentTrackingNumber as tracking_number,
    RecipientCountry as recipient_country,
    ServiceDesc as service_description,
    PARSE_TIMESTAMP('%m/%d/%Y', ShipmentDate) AS proper_timestamp
from source

