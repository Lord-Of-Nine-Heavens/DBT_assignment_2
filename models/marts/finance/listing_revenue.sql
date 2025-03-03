WITH listing_data AS (
    SELECT
        listing_id,
        price_final,
        minimum_nights,
        price_final * minimum_nights AS estimated_revenue
    FROM {{ ref('stg_raw_airbnb_data__listings') }}
),
listing_revenue AS (
SELECT
    listing_id,
    SUM(estimated_revenue) AS total_revenue
FROM listing_data
GROUP BY listing_id
)

select * from listing_revenue