with listing_data as (
    select
        listing_id,
        price_final,
        minimum_nights,
        price_final * minimum_nights as estimated_revenue
    from {{ ref('stg_raw_airbnb_data__listings') }}
),
listing_revenue as (
select
    listing_id,
    SUM(estimated_revenue) as total_revenue
from listing_data
group by listing_id
)

select * from listing_revenue