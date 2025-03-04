with host_listings as (
    select
        l.host_id,
        l.listing_id,
        l.price_final,
        l.minimum_nights,
        (l.price_final * l.minimum_nights) as estimated_revenue
    from {{ ref('stg_raw_airbnb_data__listings') }} l
),

host_earnings as (
    select
        h.host_id,
        sum(h.estimated_revenue) as total_host_earnings,
        count(distinct h.listing_id) as total_listings
    from host_listings h
    group by h.host_id
)

select * from host_earnings
