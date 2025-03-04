WITH listing_revenue AS (
    SELECT
        room_type,
        listing_id,
        price_final,
        minimum_nights,
        price_final * minimum_nights AS estimated_revenue
    FROM {{ ref('stg_raw_airbnb_data__listings') }}
),
room_type_revenue AS (
    SELECT
        room_type,
        COUNT(DISTINCT listing_id) AS total_listings,
        round(SUM(estimated_revenue), 2) AS total_revenue,
        round(AVG(price_final), 2) AS avg_price_per_night
    FROM listing_revenue
    GROUP BY room_type
)

SELECT * FROM room_type_revenue