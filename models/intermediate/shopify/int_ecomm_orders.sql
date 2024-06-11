{{
   config(
    materialized='table'
) 
}}

SELECT 
    *
FROM {{ ref('stg_orders') }}
WHERE shipping_address IS NOT NULL --if no shipping address then we assume it is a wholesale order
AND created_at > TIMESTAMP("2022-02-02") --before this date we were selling on squarespace
AND NOT EXISTS (
    SELECT 1
    FROM UNNEST(JSON_EXTRACT_ARRAY(note_attributes)) AS na_string
    WHERE 
        JSON_EXTRACT_SCALAR(na_string, '$.name') LIKE '%Choose%'
        OR JSON_EXTRACT_SCALAR(na_string, '$.value') LIKE '%YOOX%'
) --we filter out third party marketplace orders
AND id != CAST('5900016419148' AS INT64) -- excluding specific order
