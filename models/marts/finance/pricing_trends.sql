{{ config(
    materialized='incremental',
    unique_key='listing_id',
    incremental_strategy='merge'
) }}

WITH all_reviews AS (
    -- Ensure we get all past and new reviews
    SELECT listing_id, review_date
    FROM {{ ref('stg_raw_airbnb_data__reviews') }}
),

revenue_trends AS (
    SELECT
        l.listing_id,
        r.review_date,
        l.price_final * l.minimum_nights AS estimated_revenue,
        SUM(l.price_final * l.minimum_nights) 
            OVER (
                PARTITION BY l.listing_id 
                ORDER BY r.review_date 
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS estimated_rolling_revenue
    FROM {{ ref('stg_raw_airbnb_data__listings') }} l
    JOIN all_reviews r ON l.listing_id = r.listing_id
    {% if is_incremental() %}
    WHERE r.review_date > (SELECT MAX(review_date) FROM {{ this }})
    {% endif %}
),

pricing_trends AS (
    SELECT
        listing_id,
        review_date,
        estimated_revenue,
        estimated_rolling_revenue
    FROM revenue_trends
)

SELECT * FROM pricing_trends
