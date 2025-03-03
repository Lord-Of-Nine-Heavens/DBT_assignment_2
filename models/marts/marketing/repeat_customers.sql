WITH guest_bookings AS (
    SELECT
        reviewer_id,
        reviewer_name,
        listing_id,
        COUNT(review_id) AS review_count
    FROM {{ ref('stg_raw_airbnb_data__reviews') }}
    GROUP BY reviewer_id, reviewer_name, listing_id
),
repeat_visitors AS (
    SELECT
        reviewer_id,
        reviewer_name,
        listing_id,
        review_count,
        CASE
            WHEN review_count > 1 THEN 'Repeat Visitor'
            ELSE 'One-Time Visitor'
        END AS visit_type
    FROM guest_bookings
)

SELECT * FROM repeat_visitors