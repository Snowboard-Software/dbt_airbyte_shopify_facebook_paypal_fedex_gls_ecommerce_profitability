{{ config(materialized='table') }}

SELECT
    _airbyte_raw_id,
    _airbyte_extracted_at,
    cpc,
    cpm,
    cpp,
    ctr,
    ad_id,
    reach,
    spend,
    clicks,
    ad_name,
    country,
    TIMESTAMP(date_stop) AS date_stop,
    created_time,
    cost_per_unique_click
FROM {{ source('raw_airbyte_data', 'facebook_ads_insights_country') }}
WHERE LOWER(ad_name) NOT LIKE '%instagram%' --we filter out promoted posts as those are for wholesale
