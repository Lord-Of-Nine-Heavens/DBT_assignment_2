WITH host_listings AS (
    SELECT
        l.host_id,
        l.listing_id,
        l.price_final,
        l.minimum_nights,
        (l.price_final * l.minimum_nights) AS estimated_revenue
    FROM {{ ref('stg_raw_airbnb_data__listings') }} l
),
host_earnings AS (
SELECT
    h.host_id,
    SUM(h.estimated_revenue) AS total_host_earnings,
    COUNT(DISTINCT h.listing_id) AS total_listings
FROM host_listings h
GROUP BY h.host_id
)

select * from host_earnings