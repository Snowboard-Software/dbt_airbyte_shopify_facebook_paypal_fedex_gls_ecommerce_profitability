/* Model to calculate Klarna comissions, as all the transasction on Klarna are so far in euros and airbyte
connector is deprectated we checked Klarna's settlement list and the fee is 35 cents plus 5% of the sales amount
*/

WITH klarna_fees AS (
    SELECT * FROM {{ ref('stg_shopify_transactions') }}
), calc_klarna_fees AS (
    SELECT 
        kf.id,
        kf.kind, 
        kf.gateway,
        kf.status,
        kf.order_id as order_id,
        kf.amount, -- Gross sales from order as given to the customer by Shopify in EUR
        kf.created_at,
        0.35 + 0.05 * kf.amount AS klarna_commision_fee_EUR -- Calculating Klarna commission as 0.35 EUR + 5% of the transaction amount
    FROM klarna_fees kf
    WHERE kf.kind = 'SALE' AND kf.gateway = 'Klarna' AND kf.status = 'SUCCESS'
)

SELECT 
    *
    FROM calc_klarna_fees
