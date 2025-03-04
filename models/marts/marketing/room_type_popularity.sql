WITH review_counts AS (
    SELECT
        l.room_type,
        COUNT(r.review_id) AS total_reviews,
        round(AVG(l.price_final), 2) AS avg_price_per_night
    FROM {{ ref('stg_raw_airbnb_data__listings') }} l
    LEFT JOIN {{ ref('stg_raw_airbnb_data__reviews') }} r 
    ON l.listing_id = r.listing_id
    GROUP BY l.room_type
)

SELECT * FROM review_counts
