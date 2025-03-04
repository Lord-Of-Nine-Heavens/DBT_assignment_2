{{ config(
    materialized='incremental',
    unique_key='listing_id',
    cluster_by=['review_date'],
    incremental_strategy='merge'
) }}

with all_reviews as (
    -- ensure we get all past and new reviews
    select listing_id, review_date
    from {{ ref('stg_raw_airbnb_data__reviews') }}
),

revenue_trends as (
    select
        l.listing_id,
        r.review_date,
        l.price_final * l.minimum_nights as estimated_revenue,
        sum(l.price_final * l.minimum_nights) 
            over (
                partition by l.listing_id 
                order by r.review_date 
                rows between unbounded preceding and current row
            ) as estimated_rolling_revenue
    from {{ ref('stg_raw_airbnb_data__listings') }} l
    join all_reviews r on l.listing_id = r.listing_id
    {% if is_incremental() %}
    where r.review_date > (select max(review_date) from {{ this }})
    {% endif %}
),

pricing_trends as (
    select
        listing_id,
        review_date,
        estimated_revenue,
        estimated_rolling_revenue
    from revenue_trends
)

select * from pricing_trends
