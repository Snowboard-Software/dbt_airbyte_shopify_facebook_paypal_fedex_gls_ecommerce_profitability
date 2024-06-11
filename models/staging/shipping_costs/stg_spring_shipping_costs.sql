/*
Loaded data from our historical provider Spring from their invoice excels to our data warehouse via the Airbyte Google Sheets connector.
Has for each tracking number the net amount in euros as well as recipient country and service description. For one delivery there are several tracking number:
the vanilla base one for the shipment itself and then extensions of the base after the / character for surcharges, fees etc.
We will have to find a way to spread out pickup fees over the entire shipping later on (for example to have average for shipments that month and then add back to the cost this fee).
*/


{{ config(materialized="table") }}

with source as (
    select * from {{ source("raw_airbyte_data", "shipping_spring_shipping") }}
)

select
    _airbyte_raw_id,
    _airbyte_extracted_at,
    _airbyte_meta,
    cast(replace(net_amount_euro, ',', '.') as decimal) as net_amount_euro,
    tracking_number,
    CASE
        WHEN tracking_number LIKE '%/%' THEN REGEXP_EXTRACT(tracking_number, r'^(.*?)/')
        ELSE tracking_number
    END AS cleaned_tracking_number,
    recipient_country,
    service_description,
    parse_timestamp(
        '%d/%m/%Y %H.%M.%S', shipment_date_dd_mm_yyyy
    ) as proper_timestamp
from source
