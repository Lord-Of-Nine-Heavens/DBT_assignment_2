with host_data as (
    select 
        host_id,
        host_name,
        is_superhost,
        created_at,
        updated_at
    from {{ ref('stg_raw_airbnb_data__hosts') }}
),

listing_counts as (
    select 
        host_id, 
        count(listing_id) as total_listings
    from {{ ref('stg_raw_airbnb_data__listings') }}
    group by host_id
),

review_counts as (
    select 
        l.host_id, 
        count(r.review_id) as total_reviews
    from {{ ref('stg_raw_airbnb_data__listings') }} l
    left join {{ ref('stg_raw_airbnb_data__reviews') }} r 
    on l.listing_id = r.listing_id
    group by l.host_id
),

host_activity as (
    select 
        h.host_id,
        h.host_name,
        h.is_superhost,
        coalesce(l.total_listings, 0) as total_listings,  -- ensure hosts with no listings appear
        coalesce(r.total_reviews, 0) as total_reviews,  -- ensure hosts with no reviews appear
        datediff(day, h.created_at, current_date) as days_active,
        case 
            when r.total_reviews > 50 then 'highly engaged host'
            when l.total_listings > 5 then 'active host'
            else 'regular host'
        end as host_engagement_category
    from host_data h
    left join listing_counts l on h.host_id = l.host_id  -- left join ensures all hosts are included
    left join review_counts r on h.host_id = r.host_id
)

select * from host_activity
