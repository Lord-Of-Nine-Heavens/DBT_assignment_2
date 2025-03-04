with review_counts AS (
    select
        listing_id,
        count(review_id) AS total_reviews
    from {{ ref('stg_raw_airbnb_data__reviews') }}
    group by listing_id
),
listing_popularity as (
select
    l.listing_id,
    l.listing_name,
    r.total_reviews
from {{ ref('stg_raw_airbnb_data__listings') }} l
join review_counts r on l.listing_id = r.listing_id
)

select * from listing_popularity