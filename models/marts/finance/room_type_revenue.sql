with listing_revenue as (
    select
        room_type,
        listing_id,
        price_final,
        minimum_nights,
        price_final * minimum_nights AS estimated_revenue
    from {{ ref('stg_raw_airbnb_data__listings') }}
),
room_type_revenue as (
    select
        room_type,
        count(distinct listing_id) as total_listings,
        round(sum(estimated_revenue), 2) as total_revenue,
        round(avg(price_final), 2) as avg_price_per_night
    from listing_revenue
    group by room_type
)

select * from room_type_revenue