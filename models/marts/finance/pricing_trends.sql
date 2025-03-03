WITH revenue_trends AS (
    SELECT
        l.listing_id,
        r.review_date,
        l.price_final * l.minimum_nights AS estimated_revenue,
        SUM(l.price_final * l.minimum_nights) OVER (PARTITION BY l.listing_id ORDER BY r.review_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS estimated_rolling_revenue
    FROM {{ ref('stg_raw_airbnb_data__listings') }} l
    JOIN {{ ref('stg_raw_airbnb_data__reviews') }} r ON l.listing_id = r.listing_id
),
pricing_trends AS (
SELECT
    listing_id,
    review_date,
    estimated_revenue,
    estimated_rolling_revenue
FROM revenue_trends
)

select * from pricing_trends