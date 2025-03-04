with review_counts as (
    select
        l.room_type,
        count(r.review_id) as total_reviews,
        round(avg(l.price_final), 2) as avg_price_per_night
    from {{ ref('stg_raw_airbnb_data__listings') }} l
    left join {{ ref('stg_raw_airbnb_data__reviews') }} r 
    on l.listing_id = r.listing_id
    group by l.room_type
)

select * from review_counts
