WITH nightly_rates AS (
    SELECT 
        listing_id, 
        price_final AS nightly_rate
    FROM {{ ref('stg_raw_airbnb_data__listings') }}
),
average_nightly_rate AS (
SELECT 
    listing_id,
    AVG(nightly_rate) AS avg_nightly_rate
FROM nightly_rates
GROUP BY listing_id
)

select * from average_nightly_rate
