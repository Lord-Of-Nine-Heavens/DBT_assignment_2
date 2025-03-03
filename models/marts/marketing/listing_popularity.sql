WITH review_counts AS (
    SELECT
        listing_id,
        COUNT(review_id) AS total_reviews
    FROM {{ ref('stg_raw_airbnb_data__reviews') }}
    GROUP BY listing_id
),
listing_popularity AS (
SELECT
    l.listing_id,
    l.listing_name,
    r.total_reviews
FROM {{ ref('stg_raw_airbnb_data__listings') }} l
JOIN review_counts r ON l.listing_id = r.listing_id
)

select * from listing_popularity