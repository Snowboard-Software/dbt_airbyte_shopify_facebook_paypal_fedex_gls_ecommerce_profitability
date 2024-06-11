/*
Here we union our spring and fedex shipping tables which have the same structure and have a table with all our shipping costs by tracking number
Here we modify staging fullfilment to select the tracking company and we make here a case
where if its value is GLS Italy then shipping_cost = 5.5 else shipping_cost = shipping_cost

We have an issue where we don't have fedex shipping cost because:
1. A bug where fedex reports doesn't include all shipments made
2. Shipment is made but not yet included in invoicing/reporing
3. We didn't ship yet
4. We also have cases where order fullfilment had a bug and filled a draft tracking number so our tabel can't joint

So we

*/

{{
   config(
    materialized='table'
)
}}

WITH avg_fedex_by_country AS (
    SELECT
        ROUND(AVG(net_amount_euro), 2) as average_fedex_cost_per_order_per_country,
        recipient_country
    FROM {{ ref('int_ecomm_orders') }} eo
    LEFT JOIN {{ ref('stg_shopify_fulfillments') }} sf ON sf.order_id = eo.id
    LEFT JOIN {{ ref('stg_fedex_shipping_costs') }} fs ON fs.tracking_number = sf.tracking_number
    WHERE net_amount_euro IS NOT NULL AND eo.fulfillment_status = 'fulfilled'
    GROUP BY recipient_country
),
union_table AS (
    SELECT
        ROUND(net_amount_euro, 2) as shipping_costs,
        tracking_number,
        'FEDEX' AS shipping_service,
        recipient_country,
        --service_description,
        proper_timestamp AS cleaned_timestamp
    FROM {{ ref('stg_fedex_shipping_costs') }}

    UNION ALL

    SELECT
        total_net_ship_incl_pickup_amount_EUR as shipping_costs,
        cleaned_tracking_number as tracking_number,
        'SPRING' AS shipping_service,
        recipient_country,
        --service_description,
        cleaned_timestamp AS cleaned_timestamp
    FROM {{ ref('int_spring_costs') }}
)

SELECT
    eo.id as id,
    CASE
        WHEN sf.tracking_company = 'GLS Italy' THEN 5.5 -- filling missing data
        WHEN sf.tracking_company = 'FedEx' AND ut.shipping_costs IS NULL THEN af.average_fedex_cost_per_order_per_country --filling missing data
        ELSE ROUND(ut.shipping_costs, 2)
    END AS shipping_costs,
    sf.tracking_number,
    eo.country_code,
    ut.recipient_country,
    sf.tracking_company
FROM {{ ref('int_ecomm_orders') }} eo
LEFT JOIN {{ ref('stg_shopify_fulfillments') }} sf ON sf.order_id = eo.id
LEFT JOIN union_table ut ON ut.tracking_number = sf.tracking_number
LEFT JOIN avg_fedex_by_country af ON af.recipient_country = eo.country_code
