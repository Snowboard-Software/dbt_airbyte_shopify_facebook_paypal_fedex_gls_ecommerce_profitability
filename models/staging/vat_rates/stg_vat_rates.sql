{{
   config(
    materialized='table'
) 
}}

select
    _airbyte_raw_id, 
    _airbyte_extracted_at, 
    code as country_code, 
    year, 
    CAST(vat_rate as NUMERIC) as vat_rate, 
    member_states as member_state

from {{ source('raw_airbyte_data', 'gsheet_eu_vat_rates') }}