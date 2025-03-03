WITH host_data AS (
    SELECT 
        host_id,
        host_name,
        is_superhost,
        created_at,
        updated_at
    FROM {{ ref('stg_raw_airbnb_data__hosts') }}
),

listing_counts AS (
    SELECT 
        host_id, 
        COUNT(listing_id) AS total_listings
    FROM {{ ref('stg_raw_airbnb_data__listings') }}
    GROUP BY host_id
),

review_counts AS (
    SELECT 
        l.host_id, 
        COUNT(r.review_id) AS total_reviews
    FROM {{ ref('stg_raw_airbnb_data__listings') }} l
    LEFT JOIN {{ ref('stg_raw_airbnb_data__reviews') }} r 
    ON l.listing_id = r.listing_id
    GROUP BY l.host_id
),

host_activity AS (
    SELECT 
        h.host_id,
        h.host_name,
        h.is_superhost,
        COALESCE(l.total_listings, 0) AS total_listings,  -- Ensure hosts with no listings appear
        COALESCE(r.total_reviews, 0) AS total_reviews,  -- Ensure hosts with no reviews appear
        DATEDIFF(DAY, h.created_at, CURRENT_DATE) AS days_active,
        CASE 
            WHEN r.total_reviews > 50 THEN 'Highly Engaged Host'
            WHEN l.total_listings > 5 THEN 'Active Host'
            ELSE 'Regular Host'
        END AS host_engagement_category
    FROM host_data h
    LEFT JOIN listing_counts l ON h.host_id = l.host_id  -- LEFT JOIN ensures all hosts are included
    LEFT JOIN review_counts r ON h.host_id = r.host_id
)

SELECT * FROM host_activity
